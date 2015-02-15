open Lwt

let (>>|=) m f = m >>= function
  | `Error e -> fail (Failure e)
  | `Ok x -> f x
let (>>*=) m f = match m with
  | `Error e -> fail (Failure e)
  | `Ok x -> f x

module Disk_mirage_unix = Disk_mirage.Make(Block)(Io_page)
module Vg_IO = Lvm.Vg.Make(Disk_mirage_unix)
module J = Shared_block.Journal.Make(Block)(Lvm.Redo.Op)

module Impl = struct
  type 'a t = 'a Lwt.t
  let bind = Lwt.bind
  let return = Lwt.return
  let fail = Lwt.fail
  let handle_failure = Lwt.catch

  type context = unit
  let myvg = ref None
  let lock = Lwt_mutex.create ()
  let journal = ref None

  let format context ~name ~pvs =
    Vg_IO.format name ~magic:`Journalled pvs >>|= fun () ->
    return ()

  let vgopen context ~devices =
    match !myvg with 
    | Some _ -> 
      raise Xenvm_interface.AlreadyOpen
    | None ->
      Vg_IO.read devices >>|= fun vg ->
      myvg := Some vg;
      return ()

  let close context =
    myvg := None

  let operate fn =
    Lwt_mutex.with_lock lock (fun () -> 
        match !myvg with
        | None -> raise Xenvm_interface.Uninitialised
        | Some vg -> fn vg)
                       
  let get context () =
    operate return
    
  let create context ~name ~size = 
    operate (fun vg ->
        match Lvm.Vg.create vg name size with
        | `Ok (vg,op) ->
          myvg := Some vg;
          (match !journal with
          | Some j ->
            J.push j op;
            Lwt.return (`Ok vg)
          | None ->
            Vg_IO.write vg) >>|= fun _ ->
          return ()
        | `Error x -> failwith x)

  let rename context ~oldname ~newname = 
    operate (fun vg ->
        match Lvm.Vg.rename vg oldname newname with
        | `Ok (vg,op) ->
          myvg := Some vg;
          Vg_IO.write vg >>|= fun _ ->
          return ()
        | `Error x -> failwith x)

  let get_lv context ~name =
    let open Lvm in
    operate (fun vg ->
        let lv = List.find (fun lv -> lv.Lv.name = name) vg.Vg.lvs in
        return ({ vg with Vg.lvs = [] }, lv)
    )

  let perform vg =
    let state = ref vg in
    let perform ops =
      Lwt_list.fold_left_s (fun vg op ->
        Lvm.Vg.do_op vg op >>*= fun (vg, _) ->
        return vg
      ) !state ops
      >>= fun vg ->
      Vg_IO.write vg >>|= fun vg ->
      Printf.printf "Performed %d ops\n%!" (List.length ops);
      state := vg;
      Lwt.return ()
    in perform

  let start_journal context ~path =
    let mypath = Printf.sprintf "%s" path in
    match !myvg with
    | Some vg ->
      begin Block.connect mypath
      >>= function
      | `Ok device ->
        J.start device (perform vg) >>= fun j -> journal := Some j; Lwt.return ()
      | `Error _ ->
        failwith (Printf.sprintf "failed to start journal on %s" path)
      end
    | None -> 
      raise Xenvm_interface.Uninitialised

  let shutdown context () =
    ( match !journal with
      | Some j ->
        J.shutdown j
      | None ->
        return ()
    ) >>= fun () ->
    let (_: unit Lwt.t) =
      Lwt_unix.sleep 1.
      >>= fun () ->
      exit 0 in
    return ()
end

module XenvmServer = Xenvm_interface.ServerM(Impl)

open Cohttp_lwt_unix

let handler ~info (ch,conn) req body =
  Cohttp_lwt_body.to_string body >>= fun bodystr ->
  XenvmServer.process () (Jsonrpc.call_of_string bodystr) >>= fun result ->
  Server.respond_string ~status:`OK ~body:(Jsonrpc.string_of_response result) ()

let start_server port () =
  Printf.printf "Listening for HTTP request on: %d\n" port;
  let info = Printf.sprintf "Served by Cohttp/Lwt listening on port %d" port in
  let conn_closed (ch,conn) = () in
  let callback = handler ~info in
  let config = Server.make ~callback ~conn_closed () in
  let mode = `TCP (`Port port) in
  Server.create ~mode config

let run port daemon =
  if daemon then Lwt_daemon.daemonize ();
  (* Listen for regular API calls *)
  Lwt_main.run (start_server port ())

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
  Arg.(value & opt int 4000 & info [ "port" ] ~docv:"PORT" ~doc)

let daemon =
  let doc = "Detach from the terminal and run as a daemon" in
  Arg.(value & flag & info ["daemon"] ~docv:"DAEMON" ~doc)

let cmd = 
  let doc = "Start a XenVM daemon" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.(pure run $ port $ daemon),
  Term.info "xenvmd" ~version:"0.1" ~doc ~man

let _ =
   match Term.eval cmd with | `Error _ -> exit 1 | _ -> exit 0


