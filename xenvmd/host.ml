open Lwt
open Log

type connected_host = {
  mutable state : Xenvm_interface.connection_state;
  to_LVM : Rings.ToLVM.consumer;
  from_LVM : Rings.FromLVM.producer;
  free_LV : string;
  free_LV_uuid : Lvm.Vg.LVs.key;
}

module StringSet = Set.Make(String)
let connecting_hosts : StringSet.t ref = ref StringSet.empty
let connected_hosts : (string, connected_host) Hashtbl.t = Hashtbl.create 11

let get_connected_hosts () =
  Hashtbl.fold (fun k v acc -> (k,v)::acc) connected_hosts []

let get_connected_host host =
  try
    Some (Hashtbl.find connected_hosts host)
  with Not_found ->
    None

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
    debug "Found freeLVM exists already";
    return () (* We've already done this *)
  | None ->
    begin
      debug "No freeLVM volume";
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
          debug "Rings.ToLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended");
          Rings.ToLVM.resume toLVM_q
          >>= fun () ->

          Vg_io.Volume.connect fromLVM_id
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" fromLVM))
          | `Ok disk ->
            Rings.FromLVM.attach_as_producer ~name ~disk ()
            >>= fun (initial_state, fromLVM_q) ->
            let connected_host = {
              state = Xenvm_interface.Resuming_to_LVM;
              from_LVM = fromLVM_q;
              to_LVM = toLVM_q;
              free_LV = freeLVM;
              free_LV_uuid = (Vg_io.Volume.metadata_of freeLVM_id).Lvm.Lv.id
            } in
            Hashtbl.replace connected_hosts name connected_host;
            connecting_hosts := StringSet.remove name !connecting_hosts;
            ( if initial_state = `Suspended
              then begin
                connected_host.state <- Xenvm_interface.Resending_free_blocks;
                debug "The Rings.FromLVM queue was already suspended: resending the free blocks";
                let allocation = Lvm.Lv.to_allocation (Vg_io.Volume.metadata_of freeLVM_id) in
                Rings.FromLVM.push fromLVM_q allocation
                >>= fun pos ->
                Rings.FromLVM.p_advance fromLVM_q pos
                >>= fun () ->
                debug "Free blocks pushed";
                return ()
              end else begin
                debug "The Rings.FromLVM queue was running: no need to resend the free blocks";
                return ()
              end )
            >>= fun () ->
            debug "querying state";
            Rings.FromLVM.p_state fromLVM_q
            >>= fun state ->
            debug "Rings.FromLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended");
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

(* Hold this mutex when actively flushing from the Rings.ToLVM queues *)
let flush_m = Lwt_mutex.create ()

let flush_already_locked name =
  if not (Hashtbl.mem connected_hosts name)
  then return ()
  else begin
    let connected_host = Hashtbl.find connected_hosts name in
    let to_lvm = connected_host.to_LVM in
    Rings.ToLVM.pop to_lvm
    >>= fun (pos, items) ->
    debug "Rings.FromLVM queue %s has %d items" name (List.length items);
    Lwt_list.iter_s (function { ExpandVolume.volume; segments } ->
        Vg_io.write (fun vg ->
            debug "Expanding volume %s" volume;
            let id = (Lvm.Vg.LVs.find_by_name volume vg.Lvm.Vg.lvs).Lvm.Lv.id in
            let free_id = connected_host.free_LV_uuid in
            Lvm.Vg.do_op vg (Lvm.Redo.Op.(LvTransfer(free_id, id, segments)))
          ) >>= fun _ -> Lwt.return ()
      ) items
    >>= fun () ->
    Rings.ToLVM.c_advance to_lvm pos
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
            debug "Cooperative disconnect: suspending Rings.ToLVM queue for %s" name;
            Rings.ToLVM.suspend to_lvm
          end else return ()
      ) >>= fun () ->
      (* There may still be updates in the Rings.ToLVM queue *)
      debug "Flushing Rings.ToLVM queue for %s" name;
      Lwt_mutex.with_lock flush_m (fun () -> flush_already_locked name)
      >>= fun () ->
      let toLVM = toLVM name in
      Vg_io.write (fun vg ->
          Lvm.Vg.remove_tag vg toLVM connected_tag
        ) >>= fun _ ->
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
  let list = Hashtbl.fold (fun n c acc -> (n,c)::acc) connected_hosts [] in
  Lwt_list.map_s
    (fun (name, connected_host) ->
       let lv = toLVM name in
       let t = connected_host.to_LVM in
       ( Rings.ToLVM.c_state t >>= function
           | `Suspended -> return true
           | `Running -> return false ) >>= fun suspended ->
       Rings.ToLVM.c_debug_info t
       >>= fun debug_info ->
       let toLVM = { Xenvm_interface.lv; suspended; debug_info } in
       let lv = fromLVM name in
       let t = connected_host.from_LVM in
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
       let connection_state = Some connected_host.state in
       return { Xenvm_interface.name; connection_state; fromLVM; toLVM; freeExtents }
    ) list

let reconnect_all () =
  Vg_io.read (fun vg ->
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

let flush_one host =
  Lwt_mutex.with_lock flush_m
    (fun () -> flush_already_locked host)

let flush_all () =
  let hosts = Hashtbl.fold (fun h _ acc -> h::acc) connected_hosts [] in
  Lwt_list.iter_s flush_one hosts

let shutdown () =
  let hosts = Hashtbl.fold (fun h _ acc -> h::acc) connected_hosts [] in
  Lwt_list.iter_s (disconnect ~cooperative:true) hosts
  >>= fun () ->
  Vg_io.sync ()
