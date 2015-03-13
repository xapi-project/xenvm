(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

type action = Activate | Deactivate

(* lvchange -a[n|y] /dev/VGNAME/LVNAME *)

let lvchange_activate copts (vg_name,lv_name_opt) physical_device =
  let lv_name = match lv_name_opt with Some l -> l | None -> failwith "Need to know an LV name" in
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
    let path = Printf.sprintf "/dev/%s/%s" vg_name lv_name in
    Lwt.catch (fun () -> Lwt_unix.mkdir (Filename.dirname path) 0x755) (fun _ -> Lwt.return ()) >>= fun () -> 
    Mapper.read [ local_device ]
    >>= fun devices ->
    let targets = Mapper.to_targets devices vg lv in
    let name = Mapper.name_of vg lv in
    Devmapper.create name targets;
    Devmapper.mknod name path 0o0600;
    return ())

let lvchange_deactivate copts (vg_name,lv_name_opt) =
  let lv_name = match lv_name_opt with Some l -> l | None -> failwith "Need LV name" in
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get_lv ~name:lv_name >>= fun (vg, lv) ->
    let name = Mapper.name_of vg lv in
    Devmapper.remove name;
    return ())

let lvchange copts name physical_device action =
  match action with
  | Some Activate -> lvchange_activate copts name physical_device
  | Some Deactivate -> lvchange_deactivate copts name
  | None -> ()
    
let action_arg =
  let parse_action c =
    match c with
    | Some 'y' -> Some Activate
    | Some 'n' -> Some Deactivate
    | Some _ -> failwith "Unknown activation argument"
    | None -> None
  in
  let doc = "Controls the availability of the logical volumes for use.  Communicates with the kernel device-mapper driver via libdevmapper to activate (-ay) or deactivate (-an) the logical volumes.

Activation  of a logical volume creates a symbolic link /dev/VolumeGroupName/LogicalVolumeName pointing to the device node.  This link is removed on deactivation.  All software and scripts should access the device through this symbolic link and present this as the name of the device.  The location and name of the underlying device node may depend on the  distribution and configuration (e.g. udev) and might change from release to release." in
  let a = Arg.(value & opt (some char) None & info ["a"] ~docv:"ACTIVATE" ~doc) in
  Term.(pure parse_action $ a)

let lvchange_cmd =
  let doc = "Change the attributes of a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvchange allows you to change the attributes of a logical volume including making them known to the kernel ready for use."
  ] in
  Term.(pure lvchange $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.physical_device_arg $ action_arg),
  Term.info "lvchange" ~sdocs:"COMMON OPTIONS" ~doc ~man
