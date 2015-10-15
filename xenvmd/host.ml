open Lwt
open Log

let connected_tag = "xenvm_connected"

(* Conventional names of the metadata volumes *)
let toLVM host = host ^ "-toLVM"
let fromLVM host = host ^ "-fromLVM"
let freeLVM host = host ^ "-free"

let create name =
  let freeLVM = freeLVM name in
  let toLVM = toLVM name in
  let fromLVM = fromLVM name in
  Vg_io.myvg >>= fun vg ->
  match Vg_io.find vg freeLVM with
  | Some lv ->
    debug "Found freeLVM exists already"
    >>= fun () ->
    return () (* We've already done this *)
  | None ->
    begin
      debug "No freeLVM volume"
      >>= fun () ->
      let size = Int64.(mul 4L (mul 1024L 1024L)) in
      let creation_host = Unix.gethostname () in
      let creation_time = Unix.gettimeofday () |> Int64.of_float in
      Vg_io.write (fun vg ->
          Lvm.Vg.create vg toLVM size ~creation_host ~creation_time
        ) >>= fun _ ->
      Vg_io.write (fun vg ->
          Lvm.Vg.create vg fromLVM size ~creation_host ~creation_time
        ) >>= fun _ ->
      (* The local allocator needs to see the volumes now *)
      Vg_io.sync () >>= fun () ->
      Vg_io.myvg >>= fun vg ->

      ( match Vg_io.find vg toLVM with
        | Some lv -> return lv
        | None -> assert false ) >>= fun v ->
      Vg_io.Volume.connect v
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
      | `Ok disk ->
        let module Eraser = Lvm.EraseBlock.Make(Vg_io.Volume) in
        Eraser.erase ~pattern:(Printf.sprintf "xenvmd erased the %s volume" toLVM) disk
        >>= function
        | `Error _ -> fail (Failure (Printf.sprintf "Failed to erase %s" toLVM))
        | `Ok () ->
          Rings.ToLVM.create ~disk ()
          >>= fun () ->
          Vg_io.Volume.disconnect disk
          >>= fun () ->

          ( match Vg_io.find vg fromLVM with
            | Some lv -> return lv
            | None -> assert false ) >>= fun v ->
          Vg_io.Volume.connect v
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
          | `Ok disk ->
            Eraser.erase ~pattern:(Printf.sprintf "xenvmd erased the %s volume" fromLVM) disk
            >>= function
            | `Error _ -> fail (Failure (Printf.sprintf "Failed to erase %s" fromLVM))
            | `Ok () ->
              Rings.FromLVM.create ~disk ()
              >>= fun () ->
              Vg_io.Volume.disconnect disk >>= fun () ->
              (* Create the freeLVM LV at the end - we can use the existence
                  of this as a flag to show that we've finished host creation *)
              Vg_io.write (fun vg ->
                  Lvm.Vg.create vg freeLVM size ~creation_host ~creation_time
                ) >>= fun _ ->
              Vg_io.sync ()
    end

let sexp_of_exn e = Sexplib.Sexp.Atom (Printexc.to_string e)

let connect name =
  Vg_io.myvg >>= fun vg ->
  info "Registering host %s" name
  >>= fun () ->
  let toLVM = toLVM name in
  let fromLVM = fromLVM name in
  let freeLVM = freeLVM name in

  begin match Hostdb.get name with
    | Hostdb.Connected connected_host -> begin
        match connected_host.Hostdb.state with
        | Xenvm_interface.Failed msg ->
          info "Connection to host %s has failed with %s: retrying" name msg
          >>= fun () ->
          Lwt.return true
        | x ->
          info "Connction to host %s in state %s" name (Jsonrpc.to_string (Xenvm_interface.rpc_of_connection_state x))
          >>= fun () ->
          Lwt.return false
      end
    | Hostdb.Disconnected -> return true (* We should try again *)
    | Hostdb.Connecting -> return false (* If we're already connecting, no need to do anything *)
  end
  >>= fun try_again ->

  if (not try_again) then begin
    return ()
  end else begin
    Hostdb.set name Hostdb.Connecting;
    match Vg_io.find vg toLVM, Vg_io.find vg fromLVM, Vg_io.find vg freeLVM with
    | Some toLVM_id, Some fromLVM_id, Some freeLVM_id ->
      (* Persist at this point that we're going to connect this host *)
      (* All of the following logic is idempotent *)
      Vg_io.write (fun vg ->
          Lvm.Vg.add_tag vg toLVM connected_tag
        ) >>= fun _ ->
      let background_t () =
        Vg_io.Volume.connect toLVM_id
        >>= function
        | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" toLVM))
        | `Ok disk ->
          Rings.ToLVM.attach_as_consumer ~name ~disk ()
          >>= fun toLVM_q ->
          Rings.ToLVM.c_state toLVM_q
          >>= fun state ->
          debug "Rings.ToLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended")
          >>= fun () ->
          Rings.ToLVM.resume toLVM_q
          >>= fun () ->

          Vg_io.Volume.connect fromLVM_id
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
          | `Ok disk ->
            Rings.FromLVM.attach_as_producer ~name ~disk ()
            >>= fun (initial_state, fromLVM_q) ->
            let connected_host = Hostdb.({
              state = Xenvm_interface.Resuming_to_LVM;
              from_LVM = fromLVM_q;
              to_LVM = toLVM_q;
              free_LV = freeLVM;
              free_LV_uuid = (Vg_io.Volume.metadata_of freeLVM_id).Lvm.Lv.id
            }) in
            Hostdb.set name (Hostdb.Connected connected_host);
            ( if initial_state = `Suspended
              then begin
                connected_host.Hostdb.state <- Xenvm_interface.Resending_free_blocks;
                debug "The Rings.FromLVM queue was already suspended: resending the free blocks"
                >>= fun () ->
                let allocation = Lvm.Lv.to_allocation (Vg_io.Volume.metadata_of freeLVM_id) in
                Rings.FromLVM.push fromLVM_q allocation
                >>= fun pos ->
                Rings.FromLVM.p_advance fromLVM_q pos
                >>= fun () ->
                debug "Free blocks pushed"
              end else begin
                debug "The Rings.FromLVM queue was running: no need to resend the free blocks"
              end )
            >>= fun () ->
            debug "querying state";
            >>= fun () ->
            Rings.FromLVM.p_state fromLVM_q
            >>= fun state ->
            debug "Rings.FromLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended")
            >>= fun () ->
            return connected_host in

      (* Run the blocking stuff in the background *)
      Lwt.async
        (fun () ->
           Lwt.catch
             (fun () ->
                background_t ()
                >>= fun connected_host ->
                connected_host.Hostdb.state <- Xenvm_interface.Connected;
                return ()
             ) (fun e ->
                 let msg = Printexc.to_string e in
                 error "Connecting to %s failed with: %s" name msg
                 >>= fun () ->
                 begin
                   match Hostdb.get name with
                   | Hostdb.Connected ch -> ch.Hostdb.state <- Xenvm_interface.Failed msg
                   | _ -> ()
                 end;
                 return ())
        );
      return ()
    | _, _, _ ->
      info "At least one of host %s's volumes does not exist" name
      >>= fun () ->
      Hostdb.set name Hostdb.Disconnected;
      fail Xenvm_interface.HostNotCreated
  end

(* Hold this mutex when actively flushing from the Rings.ToLVM queues *)
let flush_m = Lwt_mutex.create ()

let flush_already_locked name =
  match Hostdb.get name with
  | Hostdb.Disconnected | Hostdb.Connecting -> return ()
  | Hostdb.Connected connected_host ->
    let to_lvm = connected_host.Hostdb.to_LVM in
    Rings.ToLVM.pop to_lvm
    >>= fun (pos, items) ->
    debug "Rings.FromLVM queue %s has %d items" name (List.length items)
    >>= fun () ->
    Lwt_list.iter_s (function { ExpandVolume.volume; segments } ->
        debug "Expanding volume %s" volume
        >>= fun () ->
        Vg_io.write (fun vg ->
            let id = (Lvm.Vg.LVs.find_by_name volume vg.Lvm.Vg.lvs).Lvm.Lv.id in
            let free_id = connected_host.Hostdb.free_LV_uuid in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvTransfer(free_id, id, segments)))
          ) >>= fun _ -> Lwt.return ()
      ) items
    >>= fun () ->
    Rings.ToLVM.c_advance to_lvm pos

let disconnect ~cooperative name =
  match Hostdb.get name with
  | Hostdb.Disconnected -> return ()
  | Hostdb.Connecting ->
    fail (Xenvm_interface.(HostStillConnecting (Jsonrpc.to_string (rpc_of_connection_state Xenvm_interface.Resuming_to_LVM))))
  | Hostdb.Connected connected_host ->
    begin
      match connected_host.Hostdb.state with
      | Xenvm_interface.Connected ->
        let to_lvm = connected_host.Hostdb.to_LVM in
        ( if cooperative then begin
              debug "Cooperative disconnect: suspending Rings.ToLVM queue for %s" name
              >>= fun () ->
              Rings.ToLVM.suspend to_lvm
            end else return ()
        ) >>= fun () ->
        (* There may still be updates in the Rings.ToLVM queue *)
        debug "Flushing Rings.ToLVM queue for %s" name
        >>= fun () ->
        Lwt_mutex.with_lock flush_m (fun () -> flush_already_locked name)
        >>= fun () ->
        let toLVM = toLVM name in
        Vg_io.write (fun vg ->
            Lvm.Vg.remove_tag vg toLVM connected_tag
          ) >>= fun _ ->
        Hostdb.set name Hostdb.Disconnected;
        return ()
      | x ->
        fail (Xenvm_interface.(HostStillConnecting (Jsonrpc.to_string (rpc_of_connection_state x))))
    end

let destroy name =
  disconnect ~cooperative:false name
  >>= fun () ->
  let toLVM = toLVM name in
  let fromLVM = fromLVM name in
  let freeLVM = freeLVM name in
  Vg_io.write (fun vg ->
      Lvm.Vg.remove vg toLVM
    ) >>= fun _ ->
  Vg_io.write (fun vg ->
      Lvm.Vg.remove vg fromLVM
    ) >>= fun _ ->
  Vg_io.write (fun vg ->
      Lvm.Vg.remove vg freeLVM
    ) >>= fun _ ->
  Lwt.return ()

let all () =
  let all = Hostdb.all () in
  Lwt_list.map_s
    (fun (name,state) ->
       match state with
       | Hostdb.Connected connected_host ->
         let lv = toLVM name in
         let t = connected_host.Hostdb.to_LVM in
         ( Rings.ToLVM.c_state t >>= function
             | `Suspended -> return true
             | `Running -> return false ) >>= fun suspended ->
         Rings.ToLVM.c_debug_info t
         >>= fun debug_info ->
         let toLVM = { Xenvm_interface.lv; suspended; debug_info } in
         let lv = fromLVM name in
         let t = connected_host.Hostdb.from_LVM in
         ( Rings.FromLVM.p_state t >>= function
             | `Suspended -> return true
             | `Running -> return false ) >>= fun suspended ->
         Rings.FromLVM.p_debug_info t
         >>= fun debug_info ->
         let fromLVM = { Xenvm_interface.lv; suspended; debug_info } in
         Vg_io.read (fun vg ->
             try
               let lv = Lvm.Vg.LVs.find_by_name (freeLVM name) vg.Lvm.Vg.lvs in
               return (Lvm.Lv.size_in_extents lv)
             with Not_found -> return 0L
           ) >>= fun freeExtents ->
         let connection_state = Some connected_host.Hostdb.state in
         return (Some ({ Xenvm_interface.name; connection_state; fromLVM; toLVM; freeExtents }))
       | _ -> return None
    ) all
  >>= fun l -> 
  Lwt.return (List.rev (List.fold_left (fun acc entry -> match entry with Some e -> e::acc | None -> acc) [] l))


let reconnect_all () =
  Vg_io.read (fun vg ->
      debug "Reconnecting"
      >>= fun () ->
      Lvm.Vg.LVs.fold (fun key v acc ->
          let name = v.Lvm.Lv.name in
          match Stringext.split name ~on:'-' |> List.rev with
          | "toLVM" :: host_bits ->
            let host = String.concat "-" (List.rev host_bits) in
            (* It's a toLVM - check to see whether it has the 'connected' flag *)
            let tags = List.map Lvm.Name.Tag.to_string v.Lvm.Lv.tags in
            let was_connected = List.mem connected_tag tags in
            (host,was_connected)::acc
          | e ->
            acc)
        vg.Lvm.Vg.lvs [] |> Lwt.return
    ) >>= fun host_states ->
  Lwt_list.iter_s (fun (host, was_connected) ->
      if was_connected then connect host else disconnect ~cooperative:false host) host_states

let flush_one host =
  Lwt_mutex.with_lock flush_m
    (fun () -> flush_already_locked host)

let flush_all () =
  let all = Hostdb.all () in
  Lwt_list.iter_s
    (fun (h, state) ->
       match state with
       | Hostdb.Connected _ -> flush_one h
       | _ -> return ()) all

let shutdown () =
  let all = Hostdb.all () in
  Lwt_list.iter_s
    (fun (h, state) ->
       match state with
       | Hostdb.Connected _ -> disconnect ~cooperative:true h
       | _ -> return ()) all
  >>= fun () ->
  Vg_io.sync ()
