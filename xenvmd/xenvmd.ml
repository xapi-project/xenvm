open Sexplib.Std
open Lwt
open Log

let (>>|=) m f = m >>= function
  | `Error e -> fail (Failure e)
  | `Ok x -> f x
let (>>*=) m f = match m with
  | `Error e -> fail (Failure e)
  | `Ok x -> f x

module Config = struct
  type t = {
    listenPort: int; (* TCP port number to listen on *)
    host_allocation_quantum: int64; (* amount of allocate each host at a time (MiB) *)
    host_low_water_mark: int64; (* when the free memory drops below, we allocate (MiB) *)
    vg: string; (* name of the volume group *)
    devices: string list; (* physical device containing the volume group *)
  } with sexp
end

module ErrorLogOnly = struct
  let debug fmt = Printf.ksprintf (fun _ -> ()) fmt
  let info  fmt = Printf.ksprintf (fun _ -> ()) fmt
  let error fmt = Printf.ksprintf (fun s -> print_endline s) fmt
end

(* This error must cause the system to stop for manual maintenance.
   Perhaps we could scope this later and take down only a single connection? *)
let fatal_error_t msg =
  error "%s" msg;
  fail (Failure msg)

let fatal_error msg m = m >>= function
  | `Error _ -> fatal_error_t msg
  | `Ok x -> return x

module Vg_IO = Lvm.Vg.Make(Block)

module ToLVM = struct
  module R = Shared_block.Ring.Make(Vg_IO.Volume)(ExpandVolume)
  let create ~disk () = R.Producer.create ~disk () >>= function
  | `Error x -> fatal_error_t (Printf.sprintf "Error creating ToLVM queue: %s" x)
  | `Ok x -> return x
  let rec attach ~disk () = R.Consumer.attach ~disk () >>= function
  | `Error _ ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    attach ~disk ()
  | `Ok x -> return x
  let rec pop t =
    R.Consumer.fold ~f:(fun item acc -> item :: acc) ~t ~init:[] ()
    >>= function
    | `Error msg -> fatal_error_t msg
    | `Ok (position, rev_items) ->
      let items = List.rev rev_items in
      return (position, items)
  let advance t position = R.Consumer.advance ~t ~position () >>= function
  | `Error x -> fatal_error_t (Printf.sprintf "Error advancing the ToLVM consumer pointer: %s" x)
  | `Ok x -> return x
end
module FromLVM = struct
  module R = Shared_block.Ring.Make(Vg_IO.Volume)(FreeAllocation)
  let create ~disk () = R.Producer.create ~disk () >>= function
  | `Error x -> fatal_error_t (Printf.sprintf "Error creating FromLVM queue: %s" x)
  | `Ok x -> return x
  let attach ~disk () = R.Producer.attach ~disk () >>= function
  | `Error x -> fatal_error_t (Printf.sprintf "Error attaching to the FromLVM producer queue: %s" x)
  | `Ok x -> return x
  let state t = R.Producer.state t >>= function
  | `Error x -> fatal_error_t (Printf.sprintf "Error querying FromLVM state: %s" x)
  | `Ok x -> return x
  let rec push t item = R.Producer.push ~t ~item () >>= function
  | `TooBig -> fatal_error_t "Item is too large to be pushed to the FromLVM queue"
  | `Error x -> fatal_error_t (Printf.sprintf "Error pushing to the FromLVM queue: %s" x)
  | `Retry ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    push t item
  | `Suspend ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    push t item
  | `Ok x -> return x
  let advance t position = R.Producer.advance ~t ~position () >>= function
  | `Error x -> fatal_error_t (Printf.sprintf "Error advancing the FromLVM producer pointer: %s" x)
  | `Ok x -> return x
end

module VolumeManager = struct
  module J = Shared_block.Journal.Make(ErrorLogOnly)(Vg_IO.Volume)(Lvm.Redo.Op)

  let devices = ref []
  let metadata = ref None
  let myvg = ref None
  let wait_for_flush_t = ref (fun () -> return ())
  let lock = Lwt_mutex.create ()
  let journal = ref None

  let vgopen ~devices:devices' =
    Lwt_list.map_s
      (fun filename ->
      Block.connect filename
        >>= function
        | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" filename))
        | `Ok x -> return x
      ) devices'
    >>= fun devices' ->
    Vg_IO.read devices' >>|= fun vg ->
    myvg := Some vg;
    metadata := Some (Vg_IO.metadata_of vg);
    devices := devices';
    return ()

  let read fn =
    Lwt_mutex.with_lock lock (fun () -> 
      match !metadata with
      | None -> assert false
      | Some metadata -> fn metadata)

  let write fn =
    Lwt_mutex.with_lock lock (fun () -> 
      match !metadata, !journal with
      | Some md, Some j ->
        ( match fn md with
          | `Error e -> fail (Failure e)
          | `Ok x -> Lwt.return x )
        >>= fun (md, op) ->
        metadata := Some md;
        J.push j op
        >>= fun waiter ->
        wait_for_flush_t := waiter;
        Lwt.return ()
      | _, _ -> assert false
    )

  let last_flush = ref 0.
  let perform ops =
    if ops = []
    then return ()
    else match !myvg with
    | None -> assert false
    | Some vg ->
      Vg_IO.update vg ops >>|= fun vg' ->
      debug "Performed %d ops" (List.length ops);
      (* Encourage batching of metadata writes by sleeping *)
      let now = Unix.gettimeofday () in
      Lwt_unix.sleep (max 0. (5. -. (now -. !last_flush)))
      >>= fun () ->
      last_flush := now;
      myvg := Some vg';
      Lwt.return ()

  let start name =
    match !myvg with
    | Some vg ->
      begin Vg_IO.Volume.(connect { vg; name })
      >>= function
      | `Ok device ->
        J.start device perform >>= fun j -> journal := Some j; Lwt.return ()
      | `Error _ ->
        failwith (Printf.sprintf "failed to start journal on %s" name)
      end
    | None ->
      assert false

  let shutdown () =
    match !journal with
    | Some j ->
      J.shutdown j
    | None ->
      return ()

  let to_LVMs = ref []
  let from_LVMs = ref []
  let free_LVs = ref []

  (* Conventional names of the metadata volumes *)
  let toLVM host = host ^ "-toLVM"
  let fromLVM host = host ^ "-fromLVM"
  let freeLVM host = host ^ "-free"

  module Host = struct
    let create name =
      let size = Int64.(mul 4L (mul 1024L 1024L)) in
      let toLVM = toLVM name in
      let fromLVM = fromLVM name in
      let freeLVM = freeLVM name in
      write (fun vg ->
        Lvm.Vg.create vg toLVM size
      ) >>= fun () ->
      write (fun vg ->
        Lvm.Vg.create vg fromLVM size
      ) >>= fun () ->
      write (fun vg ->
        Lvm.Vg.create vg freeLVM size
      ) >>= fun () ->
      (!wait_for_flush_t) () >>= fun () ->
      ( match !myvg with
        | None -> assert false
        | Some vg -> return vg )
      >>= fun vg ->
      Vg_IO.Volume.connect { Vg_IO.Volume.vg; name = toLVM }
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
      | `Ok disk ->
      ToLVM.create ~disk ()
      >>= fun () ->
      Vg_IO.Volume.disconnect disk
      >>= fun () ->
      Vg_IO.Volume.connect { Vg_IO.Volume.vg; name = fromLVM }
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
      | `Ok disk ->
      FromLVM.create ~disk ()
      >>= fun () ->
      Vg_IO.Volume.disconnect disk
  
    let register name =
      (* XXX: we have an out-of-sync pair 'myvg' and 'metadata' which means we
         have to wait for 'myvg' to be flushed. This can be removed if the redo log
         is pushed into the library. *)
      (!wait_for_flush_t) () >>= fun () ->
      ( match !myvg with
        | None -> assert false
        | Some vg -> return vg )
      >>= fun vg ->
      info "Registering host %s" name;
      let toLVM = toLVM name in
      let fromLVM = fromLVM name in
      let freeLVM = freeLVM name in
      Vg_IO.Volume.connect { Vg_IO.Volume.vg; name = toLVM }
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
      | `Ok disk ->
      ToLVM.attach ~disk ()
      >>= fun to_LVM ->
    
      Vg_IO.Volume.connect { Vg_IO.Volume.vg; name = fromLVM }
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
      | `Ok disk ->
      FromLVM.attach ~disk ()
      >>= fun from_LVM ->
      to_LVMs := (name, to_LVM) :: !to_LVMs;
      from_LVMs := (name, from_LVM) :: !from_LVMs;
      free_LVs := (name, freeLVM) :: !free_LVs;
      return ()
  end
end

module FreePool = struct
  (* Manage the Free LVs *)

  module Op = struct
    module T = struct
      type host = string with sexp
      type t =
        | FreeAllocation of (host * FreeAllocation.t)
      (* Assign a block allocation to a host *)
      with sexp
    end

    include SexpToCstruct.Make(T)
    include T
  end

  let perform t =
    let open Op in
    debug "%s" (sexp_of_t t |> Sexplib.Sexp.to_string_hum);
    match t with
    | FreeAllocation (host, allocation) ->
      let q = try Some(List.assoc host !VolumeManager.from_LVMs) with Not_found -> None in
      let host' = try Some(List.assoc host !VolumeManager.free_LVs) with Not_found -> None in
      begin match q, host' with
      | Some from_lvm, Some free  ->
        VolumeManager.write
          (fun vg ->
             match List.partition (fun lv -> lv.Lvm.Lv.name=free) vg.Lvm.Vg.lvs with
             | [ lv ], others ->
               let size = Lvm.Lv.size_in_extents lv in
               let segments = Lvm.Lv.Segment.linear size allocation in
               Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvExpand(free, { lvex_segments = segments })))
             | _, _ ->
               `Error (Printf.sprintf "Failed to find volume %s" free)
          )
        >>= fun () ->
        FromLVM.push from_lvm allocation
        >>= fun pos ->
        FromLVM.advance from_lvm pos
      | _, _ ->
        info "unable to push block update to host %s because it has disappeared" host;
        return ()
      end

  let perform = Lwt_list.iter_s perform

  module J = Shared_block.Journal.Make(Log)(Vg_IO.Volume)(Op)

  let journal = ref None

  let start name = match !VolumeManager.myvg with
    | Some vg ->
      debug "Opening LV '%s' to use as a freePool journal" name;
      ( Vg_IO.Volume.connect { Vg_IO.Volume.vg; name }
        >>= function
        | `Ok x -> return x
        | `Error _ -> fail (Failure (Printf.sprintf "Failed to open '%s' as a freePool journal" name))
      ) >>= fun device ->
      J.start device perform
      >>= fun j' ->
      journal := Some j';
      return ()
    | None ->
      assert false

  let shutdown () =
    match !journal with
    | Some j ->
      J.shutdown j
    | None ->
      return ()

  let resend_free_volumes config =
    Device.read_sector_size config.Config.devices
    >>= fun sector_size ->

    fatal_error "resend_free_volumes unable to read LVM metadata"
      ( VolumeManager.read (fun x -> return (`Ok x)) )
    >>= fun lvm ->

    Lwt_list.iter_s
      (fun (host, free) ->
        (* XXX: need to lock the host somehow. Ideally we would still service
           other queues while one host is locked. *)
        let from_lvm = List.assoc host !VolumeManager.from_LVMs in
        FromLVM.state from_lvm
        >>= function
        | `Running -> return ()
        | `Suspended ->
          let rec wait () =
            FromLVM.state from_lvm
            >>= function
            | `Suspended ->
              Lwt_unix.sleep 5.
              >>= fun () ->
              wait ()
            | `Running -> return () in
          wait ()
          >>= fun () ->
          fatal_error "resend_free_volumes"
            ( match try Some(List.find (fun lv -> lv.Lvm.Lv.name = free) lvm.Lvm.Vg.lvs)
                    with _ -> None with
              | Some lv -> return (`Ok (Lvm.Lv.to_allocation lv))
              | None -> return (`Error (Printf.sprintf "Failed to find LV %s" free)) )
          >>= fun allocation ->
          FromLVM.push from_lvm allocation
          >>= fun pos ->
          FromLVM.advance from_lvm pos
      ) !VolumeManager.free_LVs

  let top_up_free_volumes config =
    Device.read_sector_size config.Config.devices
    >>= fun sector_size ->

    VolumeManager.read (fun x -> return (`Ok x))
    >>= function
    | `Error _ -> return () (* skip if there's no LVM to read *)
    | `Ok x ->
      let extent_size = x.Lvm.Vg.extent_size in (* in sectors *)
      let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in
      (* XXX: avoid double-allocating the same free blocks *)
      Lwt_list.iter_s
       (fun (host, free) ->
         match try Some(List.find (fun lv -> lv.Lvm.Lv.name = free) x.Lvm.Vg.lvs) with _ -> None with
         | Some lv ->
           let size_mib = Int64.mul (Lvm.Lv.size_in_extents lv) extent_size_mib in
           if size_mib < config.Config.host_low_water_mark then begin
             info "LV %s is %Ld MiB < low_water_mark %Ld MiB; allocating %Ld MiB"
               free size_mib config.Config.host_low_water_mark config.Config.host_allocation_quantum;
             (* find free space in the VG *)
             begin match !journal, Lvm.Pv.Allocator.find x.Lvm.Vg.free_space Int64.(div config.Config.host_allocation_quantum extent_size_mib) with
             | _, `Error free_extents ->
               info "LV %s is %Ld MiB but total space free (%Ld MiB) is less than allocation quantum (%Ld MiB)"
                 free size_mib Int64.(mul free_extents extent_size_mib) config.Config.host_allocation_quantum;
               (* try again later *)
               return ()
             | Some j, `Ok allocated_extents ->
               J.push j (Op.FreeAllocation (host, allocated_extents))
               >>= fun wait ->
               (* The operation is now in the journal *)
               wait ()
               (* The operation has been performed *)
             | None, `Ok _ ->
               error "Unable to extend LV %s because the journal is not configured" free;
               return ()
             end
           end else return ()
         | None ->
           error "Failed to find host %s free LV %s" host free;
           return ()
       ) !VolumeManager.free_LVs
end

module Impl = struct
  type 'a t = 'a Lwt.t
  let bind = Lwt.bind
  let return = Lwt.return
  let fail = Lwt.fail
  let handle_failure = Lwt.catch

  type context = unit

  let get context () =
    VolumeManager.read (fun x -> return (`Ok x))
    >>= function
    | `Error e -> fail e
    | `Ok x -> return x

  let create context ~name ~size = 
    VolumeManager.write (fun vg ->
      Lvm.Vg.create vg name size
    )

  let rename context ~oldname ~newname = 
    VolumeManager.write (fun vg ->
      Lvm.Vg.rename vg oldname newname
    )

  let get_lv context ~name =
    let open Lvm in
    VolumeManager.read (fun vg ->
        let lv = List.find (fun lv -> lv.Lv.name = name) vg.Vg.lvs in
        return (`Ok ({ vg with Vg.lvs = [] }, lv))
    ) >>= function
    | `Error e -> fail e
    | `Ok x -> return x

  let shutdown context () =
    VolumeManager.shutdown ()
    >>= fun () ->
    FreePool.shutdown ()
    >>= fun () ->
    let (_: unit Lwt.t) =
      Lwt_unix.sleep 1.
      >>= fun () ->
      exit 0 in
    return ()

  module Host = struct
    let create context ~name = VolumeManager.Host.create name
    let register context ~name = VolumeManager.Host.register name
  end

end

module XenvmServer = Xenvm_interface.ServerM(Impl)

open Cohttp_lwt_unix

let handler ~info (ch,conn) req body =
  Cohttp_lwt_body.to_string body >>= fun bodystr ->
  XenvmServer.process () (Jsonrpc.call_of_string bodystr) >>= fun result ->
  Server.respond_string ~status:`OK ~body:(Jsonrpc.string_of_response result) ()

let run port config daemon =
  let config = Config.t_of_sexp (Sexplib.Sexp.load_sexp config) in
  let config = { config with Config.listenPort = match port with None -> config.Config.listenPort | Some x -> x } in
  if daemon then Lwt_daemon.daemonize ();
  let t =
    info "Started with configuration: %s" (Sexplib.Sexp.to_string_hum (Config.sexp_of_t config));
    VolumeManager.vgopen ~devices:config.Config.devices
    >>= fun () ->
    VolumeManager.start Xenvm_interface._redo_log_name
    >>= fun () ->
    FreePool.start Xenvm_interface._journal_name
    >>= fun () ->

    let rec service_queues () =
      (* 0. Have any local allocators restarted? *)
      FreePool.resend_free_volumes config
      >>= fun () ->
      (* 1. Do any of the host free LVs need topping up? *)
      FreePool.top_up_free_volumes config
      >>= fun () ->

      (* 2. Are there any pending LVM updates from hosts? *)
      Lwt_list.map_p
        (fun (host, to_lvm) ->
          ToLVM.pop to_lvm
          >>= fun (pos, item) ->
          return (host, to_lvm, pos, item)
        ) !VolumeManager.to_LVMs
      >>= fun work ->
      let items = List.concat (List.map (fun (_, _, _, bu) -> bu) work) in
      if items = [] then begin
        debug "sleeping for 5s";
        Lwt_unix.sleep 5.
        >>= fun () ->
        service_queues ()
      end else begin
        let allocations = List.concat (List.map (fun (host, _, _, bu) -> List.map (fun x -> host, x) bu) work) in
        Lwt_list.iter_s (function (host, { ExpandVolume.volume; segments }) ->
          VolumeManager.write (fun vg ->
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvExpand(volume, { lvex_segments = segments })))
          ) >>= fun () ->
          VolumeManager.write (fun vg ->
            let free = (List.assoc host !VolumeManager.free_LVs) in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvCrop(free, { lvc_segments = segments })))
          )
        ) allocations
        >>= fun () ->
        (* The operation is in the journal *)
        Lwt_list.iter_p
          (fun (_, t, pos, _) ->
            ToLVM.advance t pos
          ) work
        >>= fun () ->
        (* The operation is now complete *)
        service_queues ()
      end in

    let service_http () =
      Printf.printf "Listening for HTTP request on: %d\n" config.Config.listenPort;
      let info = Printf.sprintf "Served by Cohttp/Lwt listening on port %d" config.Config.listenPort in
      let conn_closed (ch,conn) = () in
      let callback = handler ~info in
      let c = Server.make ~callback ~conn_closed () in
      let mode = `TCP (`Port config.Config.listenPort) in
      (* Listen for regular API calls *)
      Server.create ~mode c in

    Lwt.join [ service_queues (); service_http () ] in

  Lwt_main.run t

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
  Term.(pure run $ port $ config $ daemon),
  Term.info "xenvmd" ~version:"0.1" ~doc ~man

let _ =
   match Term.eval cmd with | `Error _ -> exit 1 | _ -> exit 0


