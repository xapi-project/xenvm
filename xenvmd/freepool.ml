open Log
open Lwt
open Vg_io
open Errors
    
(* Manage the Free LVs *)

let xenvmd_generation_tag = "xenvmd_gen"
  
let lvm_op_of_free_allocation vg host allocation =
  let connected_host = Hashtbl.find Host.connected_hosts host in
  let freeid = connected_host.Host.free_LV_uuid in
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
  let open Journal.Op in
  debug "%s" (sexp_of_t t |> Sexplib.Sexp.to_string_hum);
  match t with
  | ExpandFree ef ->
    Lwt.catch (fun () ->
        let connected_host = Hashtbl.find Host.connected_hosts ef.host in
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
             let current_allocation = allocation_of_lv vg connected_host.Host.free_LV_uuid in
             let new_space = Lvm.Pv.Allocator.sub current_allocation ef.old_allocation |> Lvm.Pv.Allocator.size in
             if new_space = 0L then begin
               try
                 let lv = Lvm.Vg.LVs.find connected_host.Host.free_LV_uuid vg.Lvm.Vg.lvs in (* Not_found here caught by the try-catch block *)
                 let extent_size = vg.Lvm.Vg.extent_size in (* in sectors *)
                 let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in
                 let old_gen = List.fold_left
                     (fun acc tag ->
                        match generation_of_tag tag with
                        | None -> acc
                        | x -> x) None lv.Lvm.Lv.tags
                 in
                 let allocation =
                   match Lvm.Pv.Allocator.find vg.Lvm.Vg.free_space Int64.(div ef.extra_size extent_size_mib) with
                   | `Ok allocation -> allocation
                   | `Error (`OnlyThisMuchFree (needed_extents, free_extents)) ->
                     info "LV %s expansion required, but number of free extents (%Ld) is less than needed extents (%Ld)" connected_host.Host.free_LV free_extents needed_extents;
                     info "Expanding to use all the available space.";
                     vg.Lvm.Vg.free_space
                 in
                 match Lvm.Vg.do_op vg (lvm_op_of_free_allocation vg ef.host allocation) with
                 | `Ok (_,op1) ->
                   let genops =
                     match old_gen
                     with
                     | Some g -> [
                         Lvm.Redo.Op.LvRemoveTag (connected_host.Host.free_LV_uuid, tag_of_generation g);
                         Lvm.Redo.Op.LvAddTag (connected_host.Host.free_LV_uuid, tag_of_generation (g+1))]
                     | None -> [
                         Lvm.Redo.Op.LvAddTag (connected_host.Host.free_LV_uuid, tag_of_generation 1)]
                   in
                   `Ok (Some (op1::genops))
                 | `Error x -> `Error x
               with
               | Not_found ->
                 error "Couldn't find the free LV for host: %s" connected_host.Host.free_LV;
                 error "This is fatal for this host's update.";
                 `Error (`Msg "not found")
             end else `Ok None)
        >>= fun () ->
        read (fun vg ->
            let current_allocation = allocation_of_lv vg connected_host.Host.free_LV_uuid in
            let old_allocation = ef.old_allocation in
            let new_extents = Lvm.Pv.Allocator.sub current_allocation old_allocation in
            Lwt.return new_extents)
        >>= fun allocation ->
        Rings.FromLVM.push connected_host.Host.from_LVM allocation
        >>= fun pos ->
        Rings.FromLVM.advance connected_host.Host.from_LVM pos)
      (fun e ->
         match e with
         | Failure "not found" ->
           info "unable to push block update to host %s because it has disappeared" ef.host;
           return ()
         | e ->
           error "Unhandled error when replaying journal entry for host %s" ef.host;
           fail e)

let perform ops =
  Lwt_list.iter_s perform ops
  >>= fun () ->
  return (`Ok ())

let journal = ref None

let start name =
  Vg_io.myvg >>= fun vg ->
  debug "Opening LV '%s' to use as a freePool journal" name;
  ( match Vg_IO.find vg name with
    | Some lv -> return lv
    | None -> assert false ) >>= fun v ->
  ( Vg_IO.Volume.connect v >>= function
      | `Error _ -> fatal_error_t ("open " ^ name)
      | `Ok x -> return x )
  >>= fun device ->
  Journal.J.start ~client:"xenvmd" ~name:"allocation journal" device perform
  >>|= fun j' ->
  journal := Some j';
  return ()

let shutdown () =
  match !journal with
  | Some j ->
    Journal.J.shutdown j
  | None ->
    return ()

let resend_free_volumes config =
  fatal_error "resend_free_volumes unable to read LVM metadata"
    ( read (fun x -> return (`Ok x)) )
  >>= fun lvm ->

  let hosts = Hashtbl.fold (fun k v acc -> (k,v)::acc) Host.connected_hosts [] in
  Lwt_list.iter_s
    (fun (host, connected_host) ->
       (* XXX: need to lock the host somehow. Ideally we would still service
          other queues while one host is locked. *)
       let from_lvm = connected_host.Host.from_LVM in
       let freeid = connected_host.Host.free_LV_uuid in
       let freename = connected_host.Host.free_LV in
       Rings.FromLVM.state from_lvm
       >>= function
       | `Running -> return ()
       | `Suspended ->
         let rec wait () =
           Rings.FromLVM.state from_lvm
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
         Rings.FromLVM.push from_lvm allocation
         >>= fun pos ->
         Rings.FromLVM.advance from_lvm pos
    ) hosts

let top_up_host config host =
  let open Config.Xenvmd in
  sector_size >>= fun sector_size ->
  let connected_host = Hashtbl.find Host.connected_hosts host in
  read (Lwt.return) >>= fun vg ->
  match try Some(Lvm.Vg.LVs.find connected_host.Host.free_LV_uuid vg.Lvm.Vg.lvs) with _ -> None with
  | Some lv ->
    let extent_size = vg.Lvm.Vg.extent_size in (* in sectors *)
    let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in
    let size_mib = Int64.mul (Lvm.Lv.size_in_extents lv) extent_size_mib in
    if size_mib < config.host_low_water_mark then begin
      info "LV %s is %Ld MiB < low_water_mark %Ld MiB; allocating"
        connected_host.Host.free_LV size_mib config.host_low_water_mark;
      match !journal with
      | Some j ->
        let open Journal.Op in
        Journal.J.push j
          (ExpandFree
             { host;
               old_allocation=Lvm.Lv.to_allocation lv;
               extra_size=config.host_allocation_quantum })
        >>|= fun wait ->
        wait.Journal.J.sync ()
      | None ->
        error "No journal configured!";
        Lwt.return ()
    end else return ()
  | None ->
    error "Host has disappeared!";
    Lwt.return ()

let top_up_free_volumes config =
  let hosts = Hashtbl.fold (fun k v acc -> (k,v)::acc) Host.connected_hosts [] in
  Lwt_list.iter_s
    (fun (host, connected_host) ->
       top_up_host config host
    ) hosts
