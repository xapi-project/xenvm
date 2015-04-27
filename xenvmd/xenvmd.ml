open Sexplib.Std
open Lwt
open Log
open Errors

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

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end

(* This error must cause the system to stop for manual maintenance.
   Perhaps we could scope this later and take down only a single connection? *)
let fatal_error_t msg =
  error "%s" msg;
  fail (Failure msg)

let fatal_error msg m = m >>= function
  | `Error (`Msg x) -> fatal_error_t (msg ^ ": " ^ x)
  | `Error `Suspended -> fatal_error_t (msg ^ ": queue is suspended")
  | `Error `Retry -> fatal_error_t (msg ^ ": queue temporarily unavailable")
  | `Ok x -> return x

module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock)

module ToLVM = struct
  module R = Shared_block.Ring.Make(Log)(Vg_IO.Volume)(ExpandVolume)
  let create ~disk () =
    fatal_error "creating ToLVM queue" (R.Producer.create ~disk ())
  let rec attach ~disk () =
    fatal_error "attaching to ToLVM queue" (R.Consumer.attach ~disk ())
  let state t =
    fatal_error "querying ToLVM state" (R.Consumer.state t)
  let rec suspend t =
    R.Consumer.suspend t
    >>= function
    | `Error (`Msg msg) -> fatal_error_t msg
    | `Error `Suspended -> return ()
    | `Error `Retry ->
      Lwt_unix.sleep 5.
      >>= fun () ->
      suspend t
    | `Ok () ->
      let rec wait () =
        R.Consumer.state t
        >>= function
        | `Error _ -> fatal_error_t "reading state of ToLVM"
        | `Ok `Running -> wait ()
        | `Ok `Suspended -> return () in
      wait ()
  let rec resume t =
    R.Consumer.resume t
    >>= function
    | `Error (`Msg msg) -> fatal_error_t msg
    | `Error `Retry ->
      Lwt_unix.sleep 5.
      >>= fun () ->
      resume t
    | `Error `Suspended -> return ()
    | `Ok () ->
      let rec wait () =
        R.Consumer.state t
        >>= function
        | `Error _ -> fatal_error_t "reading state of ToLVM"
        | `Ok `Suspended -> wait ()
        | `Ok `Running -> return () in
      wait ()
  let rec pop t =
    fatal_error "ToLVM.pop"
      (R.Consumer.fold ~f:(fun item acc -> item :: acc) ~t ~init:[] ())
    >>= fun (position, rev_items) ->
      let items = List.rev rev_items in
      return (position, items)
  let advance t position =
    fatal_error "toLVM.advance" (R.Consumer.advance ~t ~position ())
end
module FromLVM = struct
  module R = Shared_block.Ring.Make(Log)(Vg_IO.Volume)(FreeAllocation)
  let create ~disk () =
    fatal_error "FromLVM.create" (R.Producer.create ~disk ())
  let attach ~disk () =
    fatal_error "FromLVM.attach" (R.Producer.attach ~disk ())
  let state t = fatal_error "FromLVM.state" (R.Producer.state t)
  let rec push t item = R.Producer.push ~t ~item () >>= function
  | `Error (`Msg x) -> fatal_error_t (Printf.sprintf "Error pushing to the FromLVM queue: %s" x)
  | `Error `Retry ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    push t item
  | `Error `Suspended ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    push t item
  | `Ok x -> return x
  let advance t position =
    fatal_error "FromLVM.advance" (R.Producer.advance ~t ~position ())
end

module VolumeManager = struct
  let myvg, myvg_u = Lwt.task ()
  let lock = Lwt_mutex.create ()

  let vgopen ~devices:devices' =
    Lwt_list.map_s
      (fun filename -> Block.connect filename >>= function
        | `Error _ -> fatal_error_t ("open " ^ filename)
        | `Ok x -> return x
      ) devices'
    >>= fun devices' ->
    Vg_IO.connect ~flush_interval:5. devices' `RW >>|= fun vg ->
    Lwt.wakeup_later myvg_u vg;
    return ()

  let read fn =
    Lwt_mutex.with_lock lock (fun () -> 
      myvg >>= fun myvg ->
      fn (Vg_IO.metadata_of myvg)
    )

  let write fn =
    Lwt_mutex.with_lock lock (fun () -> 
      myvg >>= fun myvg ->
      fn (Vg_IO.metadata_of myvg)
      >>*= fun (_, op) ->
      Vg_IO.update myvg [ op ]
      >>|= fun () ->
      Lwt.return ()
    )

  let sync () =
    Lwt_mutex.with_lock lock (fun () ->
      myvg >>= fun myvg ->
      Vg_IO.sync myvg
      >>|= fun () ->
      Lwt.return ()
    )

  let to_LVMs = ref []
  let from_LVMs = ref []
  let free_LVs = ref []

  (* Conventional names of the metadata volumes *)
  let toLVM host = host ^ "-toLVM"
  let fromLVM host = host ^ "-fromLVM"
  let freeLVM host = host ^ "-free"

  module Host = struct
    let create name =
      let freeLVM = freeLVM name in
      let toLVM = toLVM name in
      let fromLVM = fromLVM name in
      myvg >>= fun vg ->
      match Vg_IO.find vg freeLVM with
      |	Some lv ->
	debug "Found freeLVM exists already";
	return () (* We've already done this *)
      | None -> begin
	  debug "No freeLVM volume";
	  let size = Int64.(mul 4L (mul 1024L 1024L)) in
          write (fun vg ->
            Lvm.Vg.create vg toLVM size
          ) >>= fun () ->
          write (fun vg ->
            Lvm.Vg.create vg fromLVM size
          ) >>= fun () ->
          (* The local allocator needs to see the volumes now *)
          sync () >>= fun () ->
          myvg >>= fun vg ->
          ( match Vg_IO.find vg toLVM with
            | Some lv -> return lv
            | None -> assert false ) >>= fun v ->
          Vg_IO.Volume.connect v
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
          | `Ok disk ->
          ToLVM.create ~disk ()
          >>= fun () ->
          Vg_IO.Volume.disconnect disk
          >>= fun () ->
          ( match Vg_IO.find vg fromLVM with
            | Some lv -> return lv
            | None -> assert false ) >>= fun v ->
          Vg_IO.Volume.connect v
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
          | `Ok disk ->
          FromLVM.create ~disk ()
          >>= fun () ->
          Vg_IO.Volume.disconnect disk >>= fun () ->
          (* Create the freeLVM LV at the end - we can use the existence
             of this as a flag to show that we've finished host creation *)
          write (fun vg ->
            Lvm.Vg.create vg freeLVM size
          ) >>= fun () ->
          sync ()
	end
      
    let connect name =
      myvg >>= fun vg ->
      info "Registering host %s" name;
      let toLVM = toLVM name in
      let fromLVM = fromLVM name in
      let freeLVM = freeLVM name in
      ( try
	  Lwt.return (Lvm.Vg.LVs.find_by_name freeLVM (Vg_IO.metadata_of vg).Lvm.Vg.lvs).Lvm.Lv.id
        with _ ->
	  fail Xenvm_interface.HostNotCreated ) >>= fun freeLVMid ->
      ( match Vg_IO.find vg toLVM with
        | Some lv -> return lv
        | None -> assert false ) >>= fun v ->
      Vg_IO.Volume.connect v
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
      | `Ok disk ->
      ToLVM.attach ~disk ()
      >>= fun to_LVM ->
      ToLVM.resume to_LVM
      >>= fun () ->
      ( match Vg_IO.find vg fromLVM with
        | Some lv -> return lv
        | None -> assert false ) >>= fun v ->
      Vg_IO.Volume.connect v
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
      | `Ok disk ->
      FromLVM.attach ~disk ()
      >>= fun from_LVM ->
      to_LVMs := (name, to_LVM) :: !to_LVMs;
      from_LVMs := (name, from_LVM) :: !from_LVMs;
      free_LVs := (name, (freeLVM,freeLVMid)) :: !free_LVs;
      return ()

    let flush name =
      if not(List.mem_assoc name !to_LVMs)
      then return ()
      else begin
        let to_lvm = List.assoc name !to_LVMs in
        ToLVM.pop to_lvm
        >>= fun (pos, items) ->
        Lwt_list.iter_s (function { ExpandVolume.volume; segments } ->
          write (fun vg ->
	      let id = (Lvm.Vg.LVs.find_by_name volume vg.Lvm.Vg.lvs).Lvm.Lv.id in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvExpand(id, { lvex_segments = segments })))
          ) >>= fun () ->
          write (fun vg ->
            let (_,freeid) = (List.assoc name !free_LVs) in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvCrop(freeid, { lvc_segments = segments })))
          )
        ) items
        >>= fun () ->
        ToLVM.advance to_lvm pos
      end
  
    let disconnect name =
      if not(List.mem_assoc name !to_LVMs)
      then return () (* already disconnected *)
      else
        let to_lvm = List.assoc name !to_LVMs in
        debug "Suspending ToLVM queue for %s" name;
        ToLVM.suspend to_lvm
        >>= fun () ->
        (* There may still be updates in the ToLVM queue *)
        flush name
        >>= fun () ->
        debug "ToLVM queue for %s has been suspended and flushed" name;
        to_LVMs := List.filter (fun (n, _) -> n <> name) !to_LVMs;
        from_LVMs := List.filter (fun (n, _) -> n <> name) !from_LVMs;
        free_LVs := List.filter (fun (n, _) -> n <> name) !free_LVs;
        return ()

    let destroy name =
      disconnect name
      >>= fun () ->
      let toLVM = toLVM name in
      let fromLVM = fromLVM name in
      let freeLVM = freeLVM name in
      write (fun vg ->
        Lvm.Vg.remove vg toLVM
      ) >>= fun () ->
      write (fun vg ->
        Lvm.Vg.remove vg fromLVM
      ) >>= fun () ->
      write (fun vg ->
        Lvm.Vg.remove vg freeLVM
      )
  
    let all () =
      Lwt_list.map_s
        (fun (name, _) ->
          let lv = toLVM name in
          let t = List.assoc name !to_LVMs in
          ( ToLVM.state t >>= function
            | `Suspended -> return true
            | `Running -> return false ) >>= fun suspended ->
          let toLVM = { Xenvm_interface.lv; suspended } in
          let lv = fromLVM name in
          let t = List.assoc name !from_LVMs in
          ( FromLVM.state t >>= function
            | `Suspended -> return true
            | `Running -> return false ) >>= fun suspended ->
          let fromLVM = { Xenvm_interface.lv; suspended } in
          read (fun vg ->
            try
              let lv = Lvm.Vg.LVs.find_by_name (freeLVM name) vg.Lvm.Vg.lvs in
              return (Lvm.Lv.size_in_extents lv)
            with Not_found -> return 0L
          ) >>= fun freeExtents ->
          return { Xenvm_interface.name; fromLVM; toLVM; freeExtents }
        ) !to_LVMs
  end

  let shutdown () =
    Lwt_list.iter_s
      (fun (host, _) ->
        Host.disconnect host
      ) !from_LVMs
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
      | Some from_lvm, Some (freename,freeid)  ->
        VolumeManager.write
          (fun vg ->
             match (try Some(Lvm.Vg.LVs.find freeid vg.Lvm.Vg.lvs) with Not_found -> None) with
             | None ->
               `Error (`Msg (Printf.sprintf "Failed to find volume %s" freename))
             | Some lv ->
               let size = Lvm.Lv.size_in_extents lv in
               let segments = Lvm.Lv.Segment.linear size allocation in
               Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvExpand(freeid, { lvex_segments = segments })))
          )
        >>= fun () ->
        FromLVM.push from_lvm allocation
        >>= fun pos ->
        FromLVM.advance from_lvm pos
      | _, _ ->
        info "unable to push block update to host %s because it has disappeared" host;
        return ()
      end

  let perform ops =
    Lwt_list.iter_s perform ops
    >>= fun () ->
    return (`Ok ())

  module J = Shared_block.Journal.Make(Log)(Vg_IO.Volume)(Time)(Clock)(Op)

  let journal = ref None

  let start name =
    VolumeManager.myvg >>= fun vg ->
    debug "Opening LV '%s' to use as a freePool journal" name;
    ( match Vg_IO.find vg name with
      | Some lv -> return lv
      | None -> assert false ) >>= fun v ->
    ( Vg_IO.Volume.connect v >>= function
      | `Error _ -> fatal_error_t ("open " ^ name)
      | `Ok x -> return x )
    >>= fun device ->
    J.start device perform
    >>|= fun j' ->
    journal := Some j';
    return ()

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
      (fun (host, (freename,freeid)) ->
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
            ( match try Some(Lvm.Vg.LVs.find freeid lvm.Lvm.Vg.lvs) with _ -> None with
              | Some lv -> return (`Ok (Lvm.Lv.to_allocation lv))
              | None -> return (`Error (`Msg (Printf.sprintf "Failed to find LV %s" freename))) )
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
       (fun (host, (freename,freeid)) ->
         match try Some(Lvm.Vg.LVs.find freeid x.Lvm.Vg.lvs) with _ -> None with
         | Some lv ->
           let size_mib = Int64.mul (Lvm.Lv.size_in_extents lv) extent_size_mib in
           if size_mib < config.Config.host_low_water_mark then begin
             info "LV %s is %Ld MiB < low_water_mark %Ld MiB; allocating %Ld MiB"
               freename size_mib config.Config.host_low_water_mark config.Config.host_allocation_quantum;
             (* find free space in the VG *)
             begin match !journal, Lvm.Pv.Allocator.find x.Lvm.Vg.free_space Int64.(div config.Config.host_allocation_quantum extent_size_mib) with
             | _, `Error (`OnlyThisMuchFree free_extents) ->
               info "LV %s is %Ld MiB but total space free (%Ld MiB) is less than allocation quantum (%Ld MiB)"
                 freename size_mib Int64.(mul free_extents extent_size_mib) config.Config.host_allocation_quantum;
               (* try again later *)
               return ()
             | Some j, `Ok allocated_extents ->
               J.push j (Op.FreeAllocation (host, allocated_extents))
               >>|= fun wait ->
               (* The operation is now in the journal *)
               wait ()
               (* The operation has been performed *)
             | None, `Ok _ ->
               error "Unable to extend LV %s because the journal is not configured" freename;
               return ()
             end
           end else return ()
         | None ->
           error "Failed to find host %s free LV %s" host freename;
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
    fatal_error "get" (VolumeManager.read (fun x -> return (`Ok x)))
  
  let create context ~name ~size ~tags =
    VolumeManager.write (fun vg ->
      Lvm.Vg.create vg name ~tags size
    )

  let rename context ~oldname ~newname = 
    VolumeManager.write (fun vg ->
      Lvm.Vg.rename vg oldname newname
    )

  let remove context ~name =
    VolumeManager.write (fun vg ->
      Lvm.Vg.remove vg name
    )

  let resize context ~name ~size =
    VolumeManager.write (fun vg ->
      Lvm.Vg.resize vg name size
    )

  let set_status context ~name ~readonly =
    VolumeManager.write (fun vg ->
      Lvm.Vg.set_status vg name Lvm.Lv.Status.(if readonly then [Read] else [Read; Write])
    )

  let get_lv context ~name =
    let open Lvm in
    fatal_error "get_lv"
      (VolumeManager.read (fun vg ->
        let lv = Lvm.Vg.LVs.find_by_name name vg.Vg.lvs in
        return (`Ok ({ vg with Vg.lvs = Vg.LVs.empty }, lv))
      ))

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
    let connect context ~name = VolumeManager.Host.connect name
    let disconnect context ~name = VolumeManager.Host.disconnect name
    let destroy context ~name = VolumeManager.Host.destroy name
    let all context () = VolumeManager.Host.all ()
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
      Lwt_list.iter_s
        (fun (host, _) ->
          VolumeManager.Host.flush host
        ) !VolumeManager.to_LVMs
      >>= fun () ->

      debug "sleeping for 5s";
      Lwt_unix.sleep 5.
      >>= fun () ->
      service_queues () in

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


