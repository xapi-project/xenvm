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

module ToLVM = Block_queue.Popper(ExpandVolume)
module FromLVM = Block_queue.Pusher(FreeAllocation)

module Disk_mirage_unix = Disk_mirage.Make(Block)(Io_page)
module Vg_IO = Lvm.Vg.Make(Disk_mirage_unix)

module VolumeManager = struct
  module J = Shared_block.Journal.Make(Block)(Lvm.Redo.Op)

  let myvg = ref None
  let lock = Lwt_mutex.create ()
  let journal = ref None

  let vgopen ~devices =
    match !myvg with 
    | Some _ -> 
      raise Xenvm_interface.AlreadyOpen
    | None ->
      Vg_IO.read devices >>|= fun vg ->
      myvg := Some vg;
      return ()

  let close () =
    myvg := None

  let read fn =
    Lwt_mutex.with_lock lock (fun () -> 
      match !myvg with
      | None -> raise Xenvm_interface.Uninitialised
      | Some vg -> fn vg)

  let write fn =
    Lwt_mutex.with_lock lock (fun () -> 
      match !myvg with
      | None -> raise Xenvm_interface.Uninitialised
      | Some vg ->
        ( match fn vg with
          | `Error e -> fail (Failure e)
          | `Ok x -> Lwt.return x )
        >>= fun (vg, op) ->
        myvg := Some vg;
        (match !journal with
        | Some j ->
          J.push j op;
          Lwt.return (`Ok vg)
        | None ->
          Vg_IO.write vg) >>|= fun _ ->
        return ()
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

  let start path =
    match !myvg with
    | Some vg ->
      begin Block.connect path
      >>= function
      | `Ok device ->
        J.start device (perform vg) >>= fun j -> journal := Some j; Lwt.return ()
      | `Error _ ->
        failwith (Printf.sprintf "failed to start journal on %s" path)
      end
    | None -> 
      raise Xenvm_interface.Uninitialised

  let shutdown () =
    match !journal with
    | Some j ->
      J.shutdown j
    | None ->
      return ()

  let to_LVMs = ref []
  let from_LVMs = ref []
  let free_LVs = ref []

  let register host =
    let open Xenvm_interface in
    info "Registering host %s" host.name;
    ToLVM.start host.toLVM
    >>= fun to_LVM ->
    FromLVM.start host.fromLVM
    >>= fun from_LVM ->
    to_LVMs := (host.name, to_LVM) :: !to_LVMs;
    from_LVMs := (host.name, from_LVM) :: !from_LVMs;
    free_LVs := (host.name, host.freeLV) :: !free_LVs;
    return ()
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
             match List.partition (fun lv -> lv.Lvm.Lv.name=free) vg.lvs with
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

  module J = Shared_block.Journal.Make(Block)(Op)

  let journal = ref None

  let start journal_path =
    debug "Opening '%s' to use as a freePool journal" journal_path;
    ( Block.connect journal_path
      >>= function
      | `Ok x -> return x
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open '%s' as a freePool journal" journal_path))
    ) >>= fun device ->
    J.start device perform
    >>= fun j' ->
    journal := Some j';
    return ()

  let shutdown () =
    match !journal with
    | Some j ->
      J.shutdown j
    | None ->
      return ()

  let top_up_free_volumes config =
    Device.read_sector_size config.Config.devices
    >>= fun sector_size ->

    Vg_IO.read config.Config.devices
    >>= function
    | `Error e ->
      error "Ignoring error reading LVM metadata: %s" e;
      return ()
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

  let format context ~name ~pvs =
    Vg_IO.format name ~magic:`Journalled pvs >>|= fun () ->
    return ()
    
  let vgopen context ~devices = VolumeManager.vgopen ~devices

  let close context () = VolumeManager.close ()

  let get context () = VolumeManager.read return

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
        return ({ vg with Vg.lvs = [] }, lv)
    )

  let set_redo_log context ~path = VolumeManager.start path

  let set_journal context ~path = FreePool.start path

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

  let register context host = VolumeManager.register host
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
  debug "Loaded configuration: %s" (Sexplib.Sexp.to_string_hum (Config.sexp_of_t config));
  if daemon then Lwt_daemon.daemonize ();
  let t =
    let rec service_queues () =
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


