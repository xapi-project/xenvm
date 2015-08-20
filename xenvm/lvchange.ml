(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt
open Xenvm_common
open Errors

(* lvchange -a[n|y] /dev/VGNAME/LVNAME *)

let dev_path_of vg_name lv_name =
  Printf.sprintf "/dev/%s/%s" vg_name lv_name

let activate vg lv local_device =
  let module Devmapper = (val !Xenvm_common.dm: Devmapper.S.DEVMAPPER) in
  let path = dev_path_of vg.Lvm.Vg.name lv.Lvm.Lv.name in
  Lwt.catch (fun () -> Lwt_unix.mkdir (Filename.dirname path) 0x755) (fun _ -> Lwt.return ()) >>= fun () -> 
  Mapper.read [ local_device ]
  >>= fun devices ->
  let targets = Mapper.to_targets devices vg lv in
  let name = Mapper.name_of vg lv in
  (* Don't recreate it if it already exists *)
  let all = Devmapper.ls () in
  if not(List.mem name all)
  then Devmapper.create name targets;
  (* Recreate the device node *)
  Lwt.catch (fun () -> Lwt_unix.unlink path) (fun _ -> Lwt.return ()) >>= fun () ->
  Devmapper.mknod name path 0o0600;
  return ()

let lvchange_activate copts vg_name lv_name physical_device (offline:bool) : unit =
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let local_device : string = match (info,physical_device) with
      | _, Some d -> d (* cmdline overrides default for the VG *)
      | Some info, None -> info.local_device (* If we've got a default, use that *)
      | None, None -> failwith "Need to know the local device!" in
    ( if offline then begin
        with_block local_device
          (fun x ->
            let module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock) in
            Vg_IO.connect [ x ] `RO >>|= fun vg ->
            match Vg_IO.find vg lv_name with
            | None -> failwith (Printf.sprintf "Failed to find LV %s" lv_name)
            | Some vol ->
              return (Vg_IO.metadata_of vg, Vg_IO.Volume.metadata_of vol)
         )
      end else Client.get_lv ~name:lv_name 
    ) >>= fun (vg, lv) ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid URI";
    activate vg lv local_device
  )

let deactivate vg lv =
  let module Devmapper = (val !Xenvm_common.dm : Devmapper.S.DEVMAPPER) in
  let open Xenvm_common in
  let name = Mapper.name_of vg lv in
  (* This can fail with an EBUSY *)
  let rec retry n =
    let all = Devmapper.ls () in
    let result =
      try
        if List.mem name all then Devmapper.remove name;
        return (`Ok ())
      with e ->
        stderr "Caught %s while removing dm device %s" (Printexc.to_string e) name
        >>= fun () ->
        if n = 0 then raise e;
        return `Retry in
    result >>= function
    | `Ok () -> return ()
    | `Retry ->
      Unix.sleep 1;
      retry (n - 1) in
  retry 30
  >>= fun () ->
  (* Delete the device node *)
  let path = dev_path_of vg.Lvm.Vg.name lv.Lvm.Lv.name in
  Lwt.catch (fun () -> Lwt_unix.unlink path) (fun _ -> Lwt.return ()) >>= fun () ->
  Client.flush ~name:lv.Lvm.Lv.name

let reload vg lv local_device =
  let module Devmapper = (val !Xenvm_common.dm : Devmapper.S.DEVMAPPER) in
  let open Xenvm_common in
  Mapper.read [ local_device ]
  >>= fun devices ->
  let targets = Mapper.to_targets devices vg lv in
  let name = Mapper.name_of vg lv in
  Devmapper.suspend name;
  Devmapper.reload name targets;
  Devmapper.resume name;
  return ()

let lvchange_deactivate copts vg_name lv_name =
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get_lv ~name:lv_name >>= fun (vg, lv) ->
    deactivate vg lv
  )

let lvchange_refresh copts vg_name lv_name physical_device =
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get_lv ~name:lv_name >>= fun (vg, lv) ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid URI";
    let local_device = match (info,physical_device) with
      | _, Some d -> d (* cmdline overrides default for the VG *)
      | Some info, None -> info.local_device (* If we've got a default, use that *)
      | None, None -> failwith "Need to know the local device!"
    in
    reload vg lv local_device
  )

let lvchange copts (vg_name,lv_name_opt) physical_device action perm refresh add_tag del_tag offline =
  let lv_name = match lv_name_opt with Some l -> l | None -> failwith "Need LV name" in
  (match action with
  | Some Activate -> lvchange_activate copts vg_name lv_name physical_device offline
  | Some Deactivate -> lvchange_deactivate copts vg_name lv_name
  | None -> ());
  (if refresh then lvchange_refresh copts vg_name lv_name physical_device);
  (match add_tag with
  | Some tag ->
    Lwt_main.run (
      let open Xenvm_common in
      get_vg_info_t copts vg_name >>= fun info ->
      set_uri copts info;
      Client.add_tag ~name:lv_name ~tag)
  | None -> ());
  (match del_tag with
  | Some tag ->
    Lwt_main.run (
      let open Xenvm_common in
      get_vg_info_t copts vg_name >>= fun info ->
      set_uri copts info;
      Client.remove_tag ~name:lv_name ~tag)
  | None -> ());
  (match perm with
  | Some x ->
    let readonly =
      match x with
      | "r" -> true
      | "rw" -> false
      | _ -> failwith "Invalid permissions"
    in
    Lwt_main.run (
        let open Xenvm_common in
	get_vg_info_t copts vg_name >>= fun info ->
	set_uri copts info;
        Client.set_status ~name:lv_name ~readonly)
  | None -> ())
    
let perm_arg =
  let doc = "Change the permissions of logical volume. Possible values are 'r' or 'rw'" in
  Arg.(value & opt (some string) None & info ["p"] ~docv:"PERMISSION" ~doc)

let refresh_arg =
  let doc = "If the logical volume is active, reload its metadata" in
  Arg.(value & flag & info ["refresh"] ~docv:"REFRESH" ~doc)

let add_tag_arg =
  let doc = "Add the given tag to the LV" in
  Arg.(value & opt (some string) None & info ["addtag"] ~docv:"ADDTAG" ~doc)

let del_tag_arg =
  let doc = "Remove the given tag from the LV" in
  Arg.(value & opt (some string) None & info ["deltag"] ~docv:"DELTAG" ~doc)

let offline_arg =
  let doc = "Assume xenvmd is offline and read metadata from the disk" in
  Arg.(value & flag & info [ "offline" ] ~docv:"OFFLINE" ~doc)
 
let lvchange_cmd =
  let doc = "Change the attributes of a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvchange allows you to change the attributes of a logical volume including making them known to the kernel ready for use."
  ] in
  Term.(pure lvchange $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.physical_device_arg $ action_arg $ perm_arg $ refresh_arg $ add_tag_arg $ del_tag_arg $ offline_arg),
  Term.info "lvchange" ~sdocs:"COMMON OPTIONS" ~doc ~man
