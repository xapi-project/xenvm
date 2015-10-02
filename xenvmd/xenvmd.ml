open Sexplib.Std
open Lwt
open Log
open Errors

module Impl = struct
  type 'a t = 'a Lwt.t
  let bind = Lwt.bind
  let return = Lwt.return
  let fail = Lwt.fail
  let handle_failure = Lwt.catch

  type context = {
    stoppers : (unit Lwt.u) list
  }

  let get context () =
    fatal_error "get" (Vg_io.read (fun x -> return (`Ok x)))

  let create context ~name ~size ~creation_host ~creation_time ~tags =
    Vg_io.write (fun vg ->
      Lvm.Vg.create vg name ~creation_host ~creation_time ~tags size
    )

  let rename context ~oldname ~newname =
    Vg_io.write (fun vg ->
      Lvm.Vg.rename vg oldname newname
    )

  let remove context ~name =
    Vg_io.write (fun vg ->
      Lvm.Vg.remove vg name
    )

  let resize context ~name ~size =
    Vg_io.write (fun vg ->
      Lvm.Vg.resize vg name size
    )

  let set_status context ~name ~readonly =
    Vg_io.write (fun vg ->
      Lvm.Vg.set_status vg name Lvm.Lv.Status.(if readonly then [Read] else [Read; Write])
    )

  let add_tag context ~name ~tag =
    Vg_io.write (fun vg ->
      Lvm.Vg.add_tag vg name tag
    )

  let remove_tag context ~name ~tag =
    Vg_io.write (fun vg ->
      Lvm.Vg.remove_tag vg name tag
    )

  let get_lv context ~name =
    let open Lvm in
    fatal_error "get_lv"
      (Vg_io.read (fun vg ->
        let lv = Lvm.Vg.LVs.find_by_name name vg.Vg.lvs in
        return (`Ok ({ vg with Vg.lvs = Vg.LVs.empty }, lv))
      ))

  let flush context ~name =
    (* We don't know where [name] is attached so we have to flush everything *)
    Host.flush_all () >>=
    Vg_io.sync

  let shutdown context () =
    List.iter (fun u -> Lwt.wakeup u ()) context.stoppers;
    Xenvmd_stats.stop ()
    >>= fun () ->
    Host.shutdown ()
    >>= fun () ->
    Freepool.shutdown ()
    >>= fun () ->
    let (_: unit Lwt.t) =
      Lwt_unix.sleep 1.
      >>= fun () ->
      exit 0 in
    return (Unix.getpid ())

  module Host = struct
    let create context ~name = Host.create name
    let connect context ~name = Host.connect name
    let disconnect context ~cooperative ~name = Host.disconnect ~cooperative name
    let destroy context ~name = Host.destroy name
    let all context () = Host.all ()
  end
end

module XenvmServer = Xenvm_interface.ServerM(Impl)

open Cohttp_lwt_unix

let handler ~info stoppers (ch,conn) req body =
  Cohttp_lwt_body.to_string body >>= fun bodystr ->
  XenvmServer.process {Impl.stoppers} (Jsonrpc.call_of_string bodystr) >>= fun result ->
  Server.respond_string ~status:`OK ~body:(Jsonrpc.string_of_response result) ()

let maybe_write_pid config =
  match config.Config.Xenvmd.listenPath with
  | None ->
      (* don't need a lock file because we'll fail to bind to the port *)
    ()
  | Some path ->
    info "Writing pidfile to %s" path;
    Pidfile.write_pid (path ^ ".lock")

let run port sock_path config =
  maybe_write_pid config;

  let t =
    info "Started with configuration: %s" (Sexplib.Sexp.to_string_hum (Config.Xenvmd.sexp_of_t config));
    Vg_io.vgopen ~devices:config.Config.Xenvmd.devices
    >>= fun () ->
    Freepool.start Xenvm_interface._journal_name
    >>= fun () ->
    Host.reconnect_all ()
    >>= fun () ->
    (* Create a snapshot cache of the metadata for the stats thread *)
    Vg_io.read return >>= fun vg ->
    let stats_vg_cache = ref vg in

    let rec service_queues () =
      (* 0. Have any local allocators restarted? *)
      Freepool.resend_free_volumes ()
      >>= fun () ->
      (* 1. Do any of the host free LVs need topping up? *)
      Freepool.top_up_free_volumes config
      >>= fun () ->
      (* 2. Are there any pending LVM updates from hosts? *)
      Host.flush_all ()
      >>= fun () ->
      (* 3. Update the metadata snapshot for the stats collection *)
      Vg_io.read return >>= fun vg -> stats_vg_cache := vg;

      Lwt_unix.sleep 5.
      >>= fun () ->
      service_queues () in

    (* See below for a description of 'stoppers' and 'stop' *)
    let service_http stoppers mode stop =
      let ty = match mode with
        | `TCP (`Port x) -> Printf.sprintf "TCP port %d" x
        | `Unix_domain_socket (`File p) -> Printf.sprintf "Unix domain socket '%s'" p
        | _ -> "<unknown>"
      in
      Printf.printf "Listening for HTTP request on: %s\n" ty;
      let info = Printf.sprintf "Served by Cohttp/Lwt listening on %s" ty in
      let conn_closed (ch,conn) = () in
      let callback = handler ~info stoppers in
      let c = Server.make ~callback ~conn_closed () in
      (* Listen for regular API calls *)
      Server.create ~mode ~stop c in

    
    let tcp_mode =
      match config.Config.Xenvmd.listenPort with
      | Some port -> [`TCP (`Port port)]
      | None -> []
    in
    
    begin
      match config.Config.Xenvmd.listenPath with
      | Some p ->
        (* Remove the socket first, if it already exists *)
        Lwt.catch (fun () -> Lwt_unix.unlink p) (fun _ -> Lwt.return ()) >>= fun () -> 
        Lwt.return [ `Unix_domain_socket (`File p) ]            
      | None ->
        Lwt.return []
    end >>= fun unix_mode ->

    let services = tcp_mode @ unix_mode in

    (* stoppers here is a list of type (unit Lwt.u) list, and 'stops'
       is a list of type (unit Lwt.t). Each of the listening Cohttp
       servers is given one of the 'stop' threads, and the whole
       'stoppers' list is passed to every handler. When a 'shutdown'
       is issued, whichever server received the call to shutdown can
       use the 'stoppers' list to shutdown each of the listeners so
       they no longer react to API calls. *)
    let stops,stoppers = List.map (fun _ -> Lwt.wait ()) services |> List.split in
    let threads = List.map2 (service_http stoppers) (tcp_mode @ unix_mode) stops in

    (* start reporting stats to rrdd if we have the config option *)
    begin match config.Config.Xenvmd.rrd_ds_owner with
    | Some owner -> Xenvmd_stats.start owner stats_vg_cache
    | None -> ()
    end;

    Lwt.join ((service_queues ())::threads) in

  Lwt_main.run t

let daemonize config =
  (* Ideally we would bind our sockets before daemonizing to avoid racing
     with the next command, but the conduit API doesn't let us pass a socket
     in. Instead we daemonize in a fork()ed child, and in the parent we wait
     for a connect() to succeed. *)
  if Unix.fork () <> 0 then begin
    let started = ref false in
    let rec wait remaining =
      if remaining = 0 then begin
        Printf.fprintf stderr "Failed to communicate with xenvmd: check the configuration and try again.\n%!";
        exit 1;
      end;
      begin match config.Config.Xenvmd.listenPort with
      | Some port ->
        let s = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        (try
          Unix.connect s (Unix.ADDR_INET(Unix.inet_addr_of_string "127.0.0.1", port));
          Unix.close s;
          started := true
        with _ ->
          Unix.close s)
      | None -> ()
      end;
      begin match config.Config.Xenvmd.listenPath with
      | Some path ->
        let s = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
        (try
          Unix.connect s (Unix.ADDR_UNIX path);
          Unix.close s;
          started := true
        with e ->
          Unix.close s)
      | None -> ()
      end;
      if not !started then begin
        Unix.sleep 1;
        wait (remaining - 1)
      end in
    wait 30;
    exit 0
  end;
  Lwt_daemon.daemonize ()
  
let main port sock_path config daemon =
  let open Config.Xenvmd in
  let config = t_of_sexp (Sexplib.Sexp.load_sexp config) in
  let config = { config with listenPort = match port with None -> config.listenPort | Some x -> Some x } in
  let config = { config with listenPath = match sock_path with None -> config.listenPath | Some x -> Some x } in

  if daemon then daemonize config;

  run port sock_path config
    
open Cmdliner

let info =
  let doc =
    "XenVM LVM daemon" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "xenvm" ~version:"0.1-alpha" ~doc ~man

let port =
  let doc = "TCP port of xenvmd server" in
  Arg.(value & opt (some int) None & info [ "port" ] ~docv:"PORT" ~doc)

let sock_path =
  let doc = "Path to create unix-domain socket for server" in
  Arg.(value & opt (some string) None & info [ "path" ] ~docv:"PATH" ~doc)

let config =
  let doc = "Path to the config file" in
  Arg.(value & opt file "remoteConfig" & info [ "config" ] ~docv:"CONFIG" ~doc)

let daemon =
  let doc = "Detach from the terminal and run as a daemon" in
  Arg.(value & flag & info ["daemon"] ~docv:"DAEMON" ~doc)

let cmd = 
  let doc = "Start a XenVM daemon" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.(pure main $ port $ sock_path $ config $ daemon),
  Term.info "xenvmd" ~version:"0.1" ~doc ~man

let _ =
   Random.self_init ();
   match Term.eval cmd with | `Error _ -> exit 1 | _ -> exit 0


