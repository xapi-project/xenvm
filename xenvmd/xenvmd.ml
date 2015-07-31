open Sexplib.Std
open Lwt
open Log
open Errors

module Config = struct
  type t = {
    listenPort: int option; (* TCP port number to listen on *)
    listenPath: string option; (* path of a unix-domain socket to listen on *)
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

let connected_tag = "xenvm_connected"

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
  let attach ~name ~disk () =
    fatal_error "attaching to ToLVM queue" (R.Consumer.attach ~queue:(name ^ " ToLVM Consumer") ~client:"xenvmd" ~disk ())
  let state t =
    fatal_error "querying ToLVM state" (R.Consumer.state t)
  let debug_info t =
    fatal_error "querying ToLVM debug_info" (R.Consumer.debug_info t)
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
        | `Ok `Running ->
          Lwt_unix.sleep 5.
          >>= fun () ->
          wait ()
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
        | `Ok `Suspended ->
          Lwt_unix.sleep 5.
          >>= fun () ->
          wait ()
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
  let attach ~name ~disk () =
    let initial_state = ref `Running in
    let rec loop () = R.Producer.attach ~queue:(name ^ " FromLVM Producer") ~client:"xenvmd" ~disk () >>= function
      | `Error `Suspended ->
        Lwt_unix.sleep 5.
        >>= fun () ->
        initial_state := `Suspended;
        loop ()
      | x -> fatal_error "FromLVM.attach" (return x) in
    loop ()
    >>= fun x ->
    return (!initial_state, x)
  let state t = fatal_error "FromLVM.state" (R.Producer.state t)
  let debug_info t =
    fatal_error "querying FromLVM debug_info" (R.Producer.debug_info t)
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
    let module Label_IO = Lvm.Label.Make(Block) in
    Lwt_list.iter_s
      (fun device ->
        Label_IO.read device >>= function
        | `Error (`Msg m) ->
          error "Failed to read PV label from device: %s" m;
          fail (Failure "Failed to read PV label from device")
        | `Ok label ->
          begin match Lvm.Label.Label_header.magic_of label.Lvm.Label.label_header with
          | Some `Lvm ->
            error "Device has normal LVM PV label. I will only open devices with the new PV label.";
            fail (Failure "Device has wrong LVM PV label")
          | Some `Journalled ->
            return ()
          | _ ->
            error "Device has an unrecognised LVM PV label. I will only open devices with the new PV label.";
            fail (Failure "Device has wrong PV label")
          end
      ) devices'
    >>= fun () ->
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
          let creation_host = Unix.gethostname () in
          let creation_time = Unix.gettimeofday () |> Int64.of_float in
          write (fun vg ->
            Lvm.Vg.create vg toLVM size ~creation_host ~creation_time
          ) >>= fun () ->
          write (fun vg ->
            Lvm.Vg.create vg fromLVM size ~creation_host ~creation_time
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
          let module Eraser = Lvm.EraseBlock.Make(Vg_IO.Volume) in
          Eraser.erase ~pattern:(Printf.sprintf "xenvmd erased the %s volume" toLVM) disk
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to erase %s" toLVM))
          | `Ok () ->
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
          Eraser.erase ~pattern:(Printf.sprintf "xenvmd erased the %s volume" fromLVM) disk
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to erase %s" fromLVM))
          | `Ok () ->
          FromLVM.create ~disk ()
          >>= fun () ->
          Vg_IO.Volume.disconnect disk >>= fun () ->
          (* Create the freeLVM LV at the end - we can use the existence
             of this as a flag to show that we've finished host creation *)
          write (fun vg ->
            Lvm.Vg.create vg freeLVM size ~creation_host ~creation_time
          ) >>= fun () ->
          sync ()
	end
   
    let sexp_of_exn e = Sexplib.Sexp.Atom (Printexc.to_string e)
 
    let host_connections = Hashtbl.create 7

    let connect name =
      myvg >>= fun vg ->
      info "Registering host %s" name;
      let toLVM = toLVM name in
      let fromLVM = fromLVM name in
      let freeLVM = freeLVM name in

      let try_again =
        if Hashtbl.mem host_connections name then begin
          match Hashtbl.find host_connections name with
          | Xenvm_interface.Failed msg ->
            info "Connection to host %s has failed with %s: retrying" name msg;
            true
          | x ->
            info "Connction to host %s in state %s" name (Jsonrpc.to_string (Xenvm_interface.rpc_of_connection_state x));
            false
        end else true in

      if not try_again then begin
        return ()
      end else begin
        match Vg_IO.find vg toLVM, Vg_IO.find vg fromLVM, Vg_IO.find vg freeLVM with
        | Some toLVM_id, Some fromLVM_id, Some freeLVM_id ->
          (* Persist at this point that we're going to connect this host *)
          (* All of the following logic is idempotent *)
          write (fun vg ->
              Lvm.Vg.add_tag vg toLVM connected_tag
            ) >>= fun () -> 
          Hashtbl.replace host_connections name Xenvm_interface.Resuming_to_LVM;
          let background_t () = 
            Vg_IO.Volume.connect toLVM_id
            >>= function
            | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
            | `Ok disk ->
            ToLVM.attach ~name ~disk ()
            >>= fun toLVM_q ->
            ToLVM.state toLVM_q
            >>= fun state ->
            debug "ToLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended");
            ToLVM.resume toLVM_q
            >>= fun () ->

            Vg_IO.Volume.connect fromLVM_id
            >>= function
            | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
            | `Ok disk ->
            FromLVM.attach ~name ~disk ()
            >>= fun (initial_state, fromLVM_q) ->
            ( if initial_state = `Suspended then begin
              Hashtbl.replace host_connections name Xenvm_interface.Resending_free_blocks;

              debug "The FromLVM queue was already suspended: resending the free blocks";
              let allocation = Lvm.Lv.to_allocation (Vg_IO.Volume.metadata_of freeLVM_id) in
              FromLVM.push fromLVM_q allocation
              >>= fun pos ->
              FromLVM.advance fromLVM_q pos
              >>= fun () ->
              debug "Free blocks pushed";
              return ()
            end else begin
              debug "The FromLVM queue was running: no need to resend the free blocks";
              return ()
            end )
            >>= fun () ->
            debug "querying state";
            FromLVM.state fromLVM_q
            >>= fun state ->
            debug "FromLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended");
            return (toLVM_q, fromLVM_q, freeLVM_id) in

          (* Run the blocking stuff in the background *)
          Lwt.async
            (fun () ->
              Lwt.catch
                (fun () ->
                  background_t ()
                  >>= fun (toLVM_q, fromLVM_q, freeLVM_id) ->
                  Hashtbl.replace host_connections name Xenvm_interface.Connected;
                  to_LVMs := (name, toLVM_q) :: !to_LVMs;
                  from_LVMs := (name, fromLVM_q) :: !from_LVMs;
                  let freeLVM_uuid = (Vg_IO.Volume.metadata_of freeLVM_id).Lvm.Lv.id in
                  free_LVs := (name, (freeLVM,freeLVM_uuid)) :: !free_LVs;
                  return ()
                ) (fun e ->
                  let msg = Printexc.to_string e in
                  error "Connecting to %s failed with: %s" name msg;
                  Hashtbl.replace host_connections name (Xenvm_interface.Failed msg);
                  return ())
            );
          return ()
      | _, _, _ ->
          info "At least one of host %s's volumes does not exist" name;
          Hashtbl.remove host_connections name;
          fail Xenvm_interface.HostNotCreated
      end

    (* Hold this mutex when actively flushing from the ToLVM queues *)
    let flush_m = Lwt_mutex.create ()

    let flush_already_locked name =
      if not(List.mem_assoc name !to_LVMs)
      then return ()
      else begin
        let to_lvm = List.assoc name !to_LVMs in
        ToLVM.pop to_lvm
        >>= fun (pos, items) ->
        debug "FromLVM queue %s has %d items" name (List.length items);
        Lwt_list.iter_s (function { ExpandVolume.volume; segments } ->
          write (fun vg ->
              debug "Expanding volume %s" volume;
	      let id = (Lvm.Vg.LVs.find_by_name volume vg.Lvm.Vg.lvs).Lvm.Lv.id in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvExpand(id, { lvex_segments = segments })))
          ) >>= fun () ->
          write (fun vg ->
            debug "Removing free blocks from %s free LV" name;
            let (_,freeid) = (List.assoc name !free_LVs) in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvCrop(freeid, { lvc_segments = segments })))
          )
        ) items
        >>= fun () ->
        ToLVM.advance to_lvm pos
      end
  
    let disconnect ~cooperative name =
      if Hashtbl.mem host_connections name then begin
        match Hashtbl.find host_connections name with
        | Xenvm_interface.Connected ->
          let to_lvm = List.assoc name !to_LVMs in
          ( if cooperative then begin
              debug "Cooperative disconnect: suspending ToLVM queue for %s" name;
              ToLVM.suspend to_lvm
            end else return ()
          ) >>= fun () ->
          (* There may still be updates in the ToLVM queue *)
          debug "Flushing ToLVM queue for %s" name;
          Lwt_mutex.with_lock flush_m (fun () -> flush_already_locked name)
          >>= fun () ->
          to_LVMs := List.filter (fun (n, _) -> n <> name) !to_LVMs;
          from_LVMs := List.filter (fun (n, _) -> n <> name) !from_LVMs;
          free_LVs := List.filter (fun (n, _) -> n <> name) !free_LVs;
          let toLVM = toLVM name in
          write (fun vg ->
              Lvm.Vg.remove_tag vg toLVM connected_tag
            ) >>= fun () ->
          Hashtbl.remove host_connections name;
          return ()
        | x ->
          fail (Xenvm_interface.(HostStillConnecting (Jsonrpc.to_string (rpc_of_connection_state x))))
      end else return ()

    let destroy name =
      disconnect ~cooperative:false name
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
          ToLVM.debug_info t
          >>= fun debug_info ->
          let toLVM = { Xenvm_interface.lv; suspended; debug_info } in
          let lv = fromLVM name in
          let t = List.assoc name !from_LVMs in
          ( FromLVM.state t >>= function
            | `Suspended -> return true
            | `Running -> return false ) >>= fun suspended ->
          FromLVM.debug_info t
          >>= fun debug_info ->
          let fromLVM = { Xenvm_interface.lv; suspended; debug_info } in
          read (fun vg ->
            try
              let lv = Lvm.Vg.LVs.find_by_name (freeLVM name) vg.Lvm.Vg.lvs in
              return (Lvm.Lv.size_in_extents lv)
            with Not_found -> return 0L
          ) >>= fun freeExtents ->
          let connection_state =
            if Hashtbl.mem host_connections name
            then Some (Hashtbl.find host_connections name)
            else None in
          return { Xenvm_interface.name; connection_state; fromLVM; toLVM; freeExtents }
        ) !to_LVMs

    let reconnect_all () =
      read (fun vg ->
          debug "Reconnecting";
          Lvm.Vg.LVs.fold (fun key v acc ->
              debug "Checking LV: %s" v.Lvm.Lv.name;
              let name = v.Lvm.Lv.name in
              match Stringext.split name ~on:'-' |> List.rev with
              | "toLVM" :: host_bits ->
                let host = String.concat "-" (List.rev host_bits) in
                debug "This is a 'toLVM' LV";
                (* It's a toLVM - check to see whether it has the 'connected' flag *)
                let tags = List.map Lvm.Name.Tag.to_string v.Lvm.Lv.tags in
                let was_connected = List.mem connected_tag tags in
                debug "host=%s was_connected=%b" host was_connected;
                (host,was_connected)::acc
              | e ->
                debug "got list: %s" (String.concat "," e);
                acc)
            vg.Lvm.Vg.lvs [] |> Lwt.return
        ) >>= fun host_states ->
      Lwt_list.iter_s (fun (host, was_connected) ->
          if was_connected then connect host else disconnect ~cooperative:false host) host_states
        
  end

  let flush_all () =
    Lwt_mutex.with_lock Host.flush_m
      (fun () ->
        Lwt_list.iter_s
          (fun (host, _) ->
            Host.flush_already_locked host
          ) !to_LVMs
      )

  let shutdown () =
    Lwt_list.iter_s
      (fun (host, _) ->
        Host.disconnect ~cooperative:true host
      ) !from_LVMs
    >>= fun () ->
    sync ()
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
    J.start ~client:"xenvmd" ~name:"allocation journal" device perform
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
             | _, `Error (`OnlyThisMuchFree (needed_extents, free_extents)) ->
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

  type context = {
    stoppers : (unit Lwt.u) list
  }

  let get context () =
    fatal_error "get" (VolumeManager.read (fun x -> return (`Ok x)))
  
  let create context ~name ~size ~creation_host ~creation_time ~tags =
    VolumeManager.write (fun vg ->
      Lvm.Vg.create vg name ~creation_host ~creation_time ~tags size
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

  let add_tag context ~name ~tag =
    VolumeManager.write (fun vg ->
      Lvm.Vg.add_tag vg name tag
    )

  let remove_tag context ~name ~tag =
    VolumeManager.write (fun vg ->
      Lvm.Vg.remove_tag vg name tag
    )

  let get_lv context ~name =
    let open Lvm in
    fatal_error "get_lv"
      (VolumeManager.read (fun vg ->
        let lv = Lvm.Vg.LVs.find_by_name name vg.Vg.lvs in
        return (`Ok ({ vg with Vg.lvs = Vg.LVs.empty }, lv))
      ))

  let flush context ~name =
    (* We don't know where [name] is attached so we have to flush everything *)
    VolumeManager.flush_all ()

  let shutdown context () =
    List.iter (fun u -> Lwt.wakeup u ()) context.stoppers;
    VolumeManager.shutdown ()
    >>= fun () ->
    FreePool.shutdown ()
    >>= fun () ->
    let (_: unit Lwt.t) =
      Lwt_unix.sleep 1.
      >>= fun () ->
      exit 0 in
    return (Unix.getpid ())

  module Host = struct
    let create context ~name = VolumeManager.Host.create name
    let connect context ~name = VolumeManager.Host.connect name
    let disconnect context ~cooperative ~name = VolumeManager.Host.disconnect ~cooperative name
    let destroy context ~name = VolumeManager.Host.destroy name
    let all context () = VolumeManager.Host.all ()
  end

end

module XenvmServer = Xenvm_interface.ServerM(Impl)

open Cohttp_lwt_unix

let handler ~info stoppers (ch,conn) req body =
  Cohttp_lwt_body.to_string body >>= fun bodystr ->
  XenvmServer.process {Impl.stoppers} (Jsonrpc.call_of_string bodystr) >>= fun result ->
  Server.respond_string ~status:`OK ~body:(Jsonrpc.string_of_response result) ()

let maybe_write_pid config =
  match config.Config.listenPath with
  | None ->
      (* don't need a lock file because we'll fail to bind to the port *)
    ()
  | Some path ->
    info "Writing pidfile to %s" path;
    Pidfile.write_pid (path ^ ".lock")

let run port sock_path config =
  maybe_write_pid config;
  
  let t =
    info "Started with configuration: %s" (Sexplib.Sexp.to_string_hum (Config.sexp_of_t config));
    VolumeManager.vgopen ~devices:config.Config.devices
    >>= fun () ->
    FreePool.start Xenvm_interface._journal_name
    >>= fun () ->
    VolumeManager.Host.reconnect_all ()
    >>= fun () -> 
    
    let rec service_queues () =
      (* 0. Have any local allocators restarted? *)
      FreePool.resend_free_volumes config
      >>= fun () ->
      (* 1. Do any of the host free LVs need topping up? *)
      FreePool.top_up_free_volumes config
      >>= fun () ->

      (* 2. Are there any pending LVM updates from hosts? *)
      VolumeManager.flush_all ()
      >>= fun () ->

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
      match config.Config.listenPort with
      | Some port -> [`TCP (`Port port)]
      | None -> []
    in
    
    begin
      match config.Config.listenPath with
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
      begin match config.Config.listenPort with
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
      begin match config.Config.listenPath with
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
  let config = Config.t_of_sexp (Sexplib.Sexp.load_sexp config) in
  let config = { config with Config.listenPort = match port with None -> config.Config.listenPort | Some x -> Some x } in
  let config = { config with Config.listenPath = match sock_path with None -> config.Config.listenPath | Some x -> Some x } in

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


