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

let xenvmd_generation_tag = "xenvmd_gen"

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

let sector_size, sector_size_u = Lwt.task ()
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
  Device.read_sector_size devices
  >>= fun sector_size ->
  Lwt.wakeup_later sector_size_u sector_size;
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

let maybe_write fn =
  Lwt_mutex.with_lock lock (fun () ->
      myvg >>= fun myvg ->
      fn (Vg_IO.metadata_of myvg)
      >>*= (function
      | Some ops ->
        Vg_IO.update myvg ops
      | None ->
        Lwt.return (`Ok ()))
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
      type fa = {
        host : string;
        old_allocation : FreeAllocation.t;
        extra_size : int64;
      } with sexp
      type t =
        | FreeAllocation of fa
      (* Assign a block allocation to a host *)
      with sexp
    end

    include SexpToCstruct.Make(T)
    include T
  end

  let op_of_free_allocation vg host allocation =
    let connected_host = Hashtbl.find connected_hosts host in
    let freeid = connected_host.free_LV_uuid in
    let lv = Lvm.Vg.LVs.find freeid vg.Lvm.Vg.lvs in
    let size = Lvm.Lv.size_in_extents lv in
    let segments = Lvm.Lv.Segment.linear size allocation in
    Lvm.Redo.Op.(LvExpand(freeid, { lvex_segments = segments }))

  let allocation_of_lv vg lv_id =
    let open Lvm in
    Vg.LVs.find lv_id vg.Vg.lvs |> Lv.to_allocation

  let generation_of_tag tag =
    match Stringext.split ~on:':' (Lvm.Name.Tag.to_string tag) with
    | [ x ; n ] when x=xenvmd_generation_tag -> (try Some (int_of_string n) with _ -> None)
    | _ -> None

  let tag_of_generation n =
    match Lvm.Name.Tag.of_string (Printf.sprintf "%s:%d" xenvmd_generation_tag n) with
    | `Ok x -> x
    | `Error (`Msg y) -> failwith y

  let perform t =
    let open Op in
    debug "%s" (sexp_of_t t |> Sexplib.Sexp.to_string_hum);
    match t with
    | FreeAllocation fa ->
      Lwt.catch (fun () ->
        let connected_host = Hashtbl.find connected_hosts fa.host in
        sector_size >>= fun sector_size ->

        (* Two operations to perform for this one journalled operation.
           So we need to be careful to ensure either that we only do
           each bit once, or that doing it twice is not harmful.

           Firstly, we've got to increase the allocation of the free
           pool for the host. We have numerical size increase
           journalled, and we have the allocation of the pool at the
           point we decided to do the operation. Therefore we can tell
           whether we've done this already or not by checking to see
           whether there are any new blocks allocated in the current
           LVM metadata.

           The second thing we need to do is tell the local allocator
           precisely which new blocks have been allocated for it.  We
           can't tell if we've done this already, so we need to ensure
           that the message is idempotent. Since if the LA has already
           received this update and may have already allocated blocks
           from it, it is imperative that there needs to be enough
           information in the message to allow the LA to ignore it
           away if it has already received it - this is the function
           of the generation count. We store the generation count in
           the LV tags so that it can be updated atomically alongside
           the increase in size. *)

        maybe_write
          (fun vg ->
             let current_allocation = allocation_of_lv vg connected_host.free_LV_uuid in
             let new_space = Lvm.Pv.Allocator.sub current_allocation fa.old_allocation |> Lvm.Pv.Allocator.size in
             if new_space = 0L then begin
               try
                 let lv = Lvm.Vg.LVs.find connected_host.free_LV_uuid vg.Lvm.Vg.lvs in (* Not_found here caught by the try-catch block *)
                 let extent_size = vg.Lvm.Vg.extent_size in (* in sectors *)
                 let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in
                 let old_gen = List.fold_left
                     (fun acc tag ->
                        match generation_of_tag tag with
                        | None -> acc
                        | x -> x) None lv.Lvm.Lv.tags
                 in
                 let allocation =
                   match Lvm.Pv.Allocator.find vg.Lvm.Vg.free_space Int64.(div fa.extra_size extent_size_mib) with
                   | `Ok allocation -> allocation
                   | `Error (`OnlyThisMuchFree (needed_extents, free_extents)) ->
                     info "LV %s expansion required, but number of free extents (%Ld) is less than needed extents (%Ld)" connected_host.free_LV free_extents needed_extents;
                     info "Expanding to use all the available space.";
                     vg.Lvm.Vg.free_space
                 in
                 match Lvm.Vg.do_op vg (op_of_free_allocation vg fa.host allocation) with
                 | `Ok (_,op1) ->
                   let genops =
                     match old_gen
                     with
                     | Some g -> [
                         Lvm.Redo.Op.LvRemoveTag (connected_host.free_LV_uuid, tag_of_generation g);
                         Lvm.Redo.Op.LvAddTag (connected_host.free_LV_uuid, tag_of_generation (g+1))]
                     | None -> [
                         Lvm.Redo.Op.LvAddTag (connected_host.free_LV_uuid, tag_of_generation 1)]
                   in
                   `Ok (Some (op1::genops))
                 | `Error x -> `Error x
               with
               | Not_found ->
                 error "Couldn't find the free LV for host: %s" connected_host.free_LV;
                 error "This is fatal for this host's update.";
                 `Error (`Msg "not found")
             end else `Ok None)
        >>= fun () ->
        read (fun vg ->
            let current_allocation = allocation_of_lv vg connected_host.free_LV_uuid in
            let old_allocation = fa.old_allocation in
            let new_extents = Lvm.Pv.Allocator.sub current_allocation old_allocation in
            Lwt.return new_extents)
        >>= fun allocation ->
        FromLVM.push connected_host.from_LVM allocation
        >>= fun pos ->
        FromLVM.advance connected_host.from_LVM pos)
        (fun e ->
           match e with
           | Failure "not found" ->
             info "unable to push block update to host %s because it has disappeared" fa.host;
             return ()
           | e ->
             error "Unhandled error when replaying journal entry for host %s" fa.host;
             fail e)

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

  let top_up_host config host =
    let open Config.Xenvmd in
    sector_size >>= fun sector_size ->
    let connected_host = Hashtbl.find connected_hosts host in
    read (Lwt.return) >>= fun vg ->
    match try Some(Lvm.Vg.LVs.find connected_host.free_LV_uuid vg.Lvm.Vg.lvs) with _ -> None with
    | Some lv ->
      let extent_size = vg.Lvm.Vg.extent_size in (* in sectors *)
      let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in
      let size_mib = Int64.mul (Lvm.Lv.size_in_extents lv) extent_size_mib in
      if size_mib < config.host_low_water_mark then begin
        info "LV %s is %Ld MiB < low_water_mark %Ld MiB; allocating"
          connected_host.free_LV size_mib config.host_low_water_mark;
        match !journal with
        | Some j ->
          J.push j
            (Op.FreeAllocation
               Op.{ host;
                    old_allocation=Lvm.Lv.to_allocation lv;
                    extra_size=config.host_allocation_quantum })
          >>|= fun wait ->
          wait.J.sync ()
        | None ->
          error "No journal configured!";
          Lwt.return ()
      end else return ()
    | None ->
      error "Host has disappeared!";
      Lwt.return ()

  let top_up_free_volumes config =
    let hosts = Hashtbl.fold (fun k v acc -> (k,v)::acc) connected_hosts [] in
    Lwt_list.iter_s
      (fun (host, connected_host) ->
         top_up_host config host
      ) hosts
end
