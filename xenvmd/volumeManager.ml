(*
 * Copyright (C) 2015 Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)
open Sexplib.Std
open Lwt
open Log
open Errors

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end

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


let myvg, myvg_u = Lwt.task ()
let lock = Lwt_mutex.create ()

let vgopen ~devices =
  Lwt_list.map_s
    (fun filename ->
       Printf.printf "filename: %s\n%!" filename;
       Block.connect filename >>= function
      | `Error _ -> fatal_error_t ("open " ^ filename)
      | `Ok x -> return x
    ) devices
  >>= fun devices' ->
  let module Label_IO = Lvm.Label.Make(Block) in
  Lwt_list.iter_s
    (fun (filename, device) ->
      Label_IO.read device >>= function
      | `Error (`Msg m) ->
        error "Failed to read PV label from device: %s" m;
        fail (Failure "Failed to read PV label from device")
      | `Ok label ->
        info "opened %s: %s" filename (Lvm.Label.to_string label);
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
    ) (List.combine devices devices')
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

type connected_host = {
  mutable state : Xenvm_interface.connection_state;
  to_LVM : ToLVM.R.Consumer.t;
  from_LVM : FromLVM.R.Producer.t;
  free_LV : string;
  free_LV_uuid : Lvm.Vg.LVs.key;
}

module StringSet = Set.Make(String)
let connecting_hosts : StringSet.t ref = ref StringSet.empty
let connected_hosts : (string, connected_host) Hashtbl.t = Hashtbl.create 11

module Host = struct
  let connected_tag = "xenvm_connected"

  (* Conventional names of the metadata volumes *)
  let toLVM host = host ^ "-toLVM"
  let fromLVM host = host ^ "-fromLVM"
  let freeLVM host = host ^ "-free"

  let create name =
    let freeLVM = freeLVM name in
    let toLVM = toLVM name in
    let fromLVM = fromLVM name in
    myvg >>= fun vg ->
    match Vg_IO.find vg freeLVM with
    | Some lv ->
      debug "Found freeLVM exists already";
      return () (* We've already done this *)
    | None ->
      begin
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

  let connect name =
    myvg >>= fun vg ->
    info "Registering host %s" name;
    let toLVM = toLVM name in
    let fromLVM = fromLVM name in
    let freeLVM = freeLVM name in

    let is_connecting = StringSet.mem name (!connecting_hosts) in

    let try_again =
      if Hashtbl.mem connected_hosts name then begin
        let connected_host = Hashtbl.find connected_hosts name in
        match connected_host.state with
        | Xenvm_interface.Failed msg ->
          info "Connection to host %s has failed with %s: retrying" name msg;
          true
        | x ->
          info "Connction to host %s in state %s" name (Jsonrpc.to_string (Xenvm_interface.rpc_of_connection_state x));
          false
      end else true in

    if is_connecting || (not try_again) then begin
      return ()
    end else begin
      connecting_hosts := StringSet.add name !connecting_hosts;
      match Vg_IO.find vg toLVM, Vg_IO.find vg fromLVM, Vg_IO.find vg freeLVM with
      | Some toLVM_id, Some fromLVM_id, Some freeLVM_id ->
        (* Persist at this point that we're going to connect this host *)
        (* All of the following logic is idempotent *)
        write (fun vg ->
            Lvm.Vg.add_tag vg toLVM connected_tag
          ) >>= fun () ->
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
          let connected_host = {
            state = Xenvm_interface.Resuming_to_LVM;
            from_LVM = fromLVM_q;
            to_LVM = toLVM_q;
            free_LV = freeLVM;
            free_LV_uuid = (Vg_IO.Volume.metadata_of freeLVM_id).Lvm.Lv.id
          } in
          Hashtbl.replace connected_hosts name connected_host;
          connecting_hosts := StringSet.remove name !connecting_hosts;
          ( if initial_state = `Suspended
            then begin
            connected_host.state <- Xenvm_interface.Resending_free_blocks;
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
          return connected_host in

        (* Run the blocking stuff in the background *)
        Lwt.async
          (fun () ->
            Lwt.catch
              (fun () ->
                background_t ()
                >>= fun connected_host ->
                connected_host.state <- Xenvm_interface.Connected;
                return ()
              ) (fun e ->
                let msg = Printexc.to_string e in
                error "Connecting to %s failed with: %s" name msg;
                begin
                  try
                    let connected_host = Hashtbl.find connected_hosts name in
                    connected_host.state <- Xenvm_interface.Failed msg;
                  with Not_found -> ()
                end;
                return ())
          );
        return ()
    | _, _, _ ->
        info "At least one of host %s's volumes does not exist" name;
        connecting_hosts := StringSet.remove name !connecting_hosts;
        fail Xenvm_interface.HostNotCreated
    end

  (* Hold this mutex when actively flushing from the ToLVM queues *)
  let flush_m = Lwt_mutex.create ()

  let flush_already_locked name =
    if not (Hashtbl.mem connected_hosts name)
    then return ()
    else begin
      let connected_host = Hashtbl.find connected_hosts name in
      let to_lvm = connected_host.to_LVM in
      ToLVM.pop to_lvm
      >>= fun (pos, items) ->
      debug "FromLVM queue %s has %d items" name (List.length items);
      Lwt_list.iter_s (function { ExpandVolume.volume; segments } ->
        write (fun vg ->
          debug "Expanding volume %s" volume;
          let id = (Lvm.Vg.LVs.find_by_name volume vg.Lvm.Vg.lvs).Lvm.Lv.id in
          let free_id = connected_host.free_LV_uuid in
          Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvTransfer(free_id, id, segments)))
        )
      ) items
      >>= fun () ->
      ToLVM.advance to_lvm pos
    end

  let disconnect ~cooperative name =
    if StringSet.mem name !connecting_hosts then
      fail (Xenvm_interface.(HostStillConnecting (Jsonrpc.to_string (rpc_of_connection_state Xenvm_interface.Resuming_to_LVM))))
    else
    if Hashtbl.mem connected_hosts name then begin
      let connected_host = Hashtbl.find connected_hosts name in
      match connected_host.state with
      | Xenvm_interface.Connected ->
        let to_lvm = connected_host.to_LVM in
        ( if cooperative then begin
            debug "Cooperative disconnect: suspending ToLVM queue for %s" name;
            ToLVM.suspend to_lvm
          end else return ()
        ) >>= fun () ->
        (* There may still be updates in the ToLVM queue *)
        debug "Flushing ToLVM queue for %s" name;
        Lwt_mutex.with_lock flush_m (fun () -> flush_already_locked name)
        >>= fun () ->
        let toLVM = toLVM name in
        write (fun vg ->
            Lvm.Vg.remove_tag vg toLVM connected_tag
          ) >>= fun () ->
        Hashtbl.remove connected_hosts name;
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
    let list = Hashtbl.fold (fun n c acc -> (n,c)::acc) connected_hosts [] in
    Lwt_list.map_s
      (fun (name, connected_host) ->
        let lv = toLVM name in
        let t = connected_host.to_LVM in
        ( ToLVM.state t >>= function
          | `Suspended -> return true
          | `Running -> return false ) >>= fun suspended ->
        ToLVM.debug_info t
        >>= fun debug_info ->
        let toLVM = { Xenvm_interface.lv; suspended; debug_info } in
        let lv = fromLVM name in
        let t = connected_host.from_LVM in
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
        let connection_state = Some connected_host.state in
        return { Xenvm_interface.name; connection_state; fromLVM; toLVM; freeExtents }
     ) list

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

let flush_one host =
  Lwt_mutex.with_lock Host.flush_m
    (fun () -> Host.flush_already_locked host)

let flush_all () =
  let hosts = Hashtbl.fold (fun h _ acc -> h::acc) connected_hosts [] in
  Lwt_list.iter_s flush_one hosts

let shutdown () =
  let hosts = Hashtbl.fold (fun h _ acc -> h::acc) connected_hosts [] in
  Lwt_list.iter_s (Host.disconnect ~cooperative:true) hosts
  >>= fun () ->
  sync ()

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
      try
        let connected_host = Hashtbl.find connected_hosts host in
        let from_lvm = connected_host.from_LVM in
        let freename = connected_host.free_LV in
        let freeid = connected_host.free_LV_uuid in
        write
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
      with Not_found ->
        info "unable to push block update to host %s because it has disappeared" host;
        return ()

  let perform ops =
    Lwt_list.iter_s perform ops
    >>= fun () ->
    return (`Ok ())

  module J = Shared_block.Journal.Make(Log)(Vg_IO.Volume)(Time)(Clock)(Op)

  let journal = ref None

  let start name =
    myvg >>= fun vg ->
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
    Device.read_sector_size config.Config.Xenvmd.devices
    >>= fun sector_size ->

    fatal_error "resend_free_volumes unable to read LVM metadata"
      ( read (fun x -> return (`Ok x)) )
    >>= fun lvm ->

    let hosts = Hashtbl.fold (fun k v acc -> (k,v)::acc) connected_hosts [] in
    Lwt_list.iter_s
      (fun (host, connected_host) ->
        (* XXX: need to lock the host somehow. Ideally we would still service
           other queues while one host is locked. *)
        let from_lvm = connected_host.from_LVM in
        let freeid = connected_host.free_LV_uuid in
        let freename = connected_host.free_LV in
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
      ) hosts

  let top_up_free_volumes config =
    let open Config.Xenvmd in
    Device.read_sector_size config.devices
    >>= fun sector_size ->

    read (fun x -> return (`Ok x))
    >>= function
    | `Error _ -> return () (* skip if there's no LVM to read *)
    | `Ok x ->
      let extent_size = x.Lvm.Vg.extent_size in (* in sectors *)
      let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in

      let hosts = Hashtbl.fold (fun k v acc -> (k,v)::acc) connected_hosts [] in
      (* XXX: avoid double-allocating the same free blocks *)
      Lwt_list.iter_s
       (fun (host, connected_host) ->
         match try Some(Lvm.Vg.LVs.find connected_host.free_LV_uuid x.Lvm.Vg.lvs) with _ -> None with
         | Some lv ->
           let size_mib = Int64.mul (Lvm.Lv.size_in_extents lv) extent_size_mib in
           if size_mib < config.host_low_water_mark then begin
             info "LV %s is %Ld MiB < low_water_mark %Ld MiB; allocating %Ld MiB"
               connected_host.free_LV size_mib config.host_low_water_mark config.host_allocation_quantum;
             (* find free space in the VG *)
             begin match !journal, Lvm.Pv.Allocator.find x.Lvm.Vg.free_space Int64.(div config.host_allocation_quantum extent_size_mib) with
             | _, `Error (`OnlyThisMuchFree (needed_extents, free_extents)) ->
               info "LV %s is %Ld MiB but total space free (%Ld MiB) is less than allocation quantum (%Ld MiB)"
                 connected_host.free_LV size_mib Int64.(mul free_extents extent_size_mib) config.host_allocation_quantum;
               (* try again later *)
               return ()
             | Some j, `Ok allocated_extents ->
               J.push j (Op.FreeAllocation (host, allocated_extents))
               >>|= fun wait ->
               (* The operation is now in the journal *)
               wait.J.sync ()
               (* The operation has been performed *)
             | None, `Ok _ ->
               error "Unable to extend LV %s because the journal is not configured" connected_host.free_LV;
               return ()
             end
           end else return ()
         | None ->
           error "Failed to find host %s free LV %s" host connected_host.free_LV;
           return ()
       ) hosts
end
