(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let vgchange copts (vg_name,_) physical_device action =
  Lwt_main.run (
    let open Xenvm_common in
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get vg_name
    >>= fun vg ->
    let names = List.map (fun (_, lv) -> lv.Lvm.Lv.name) @@ Lvm.Vg.LVs.bindings vg.Lvm.Vg.lvs in
    Lwt_list.iter_s (fun lv_name ->
      (match action with
      | Some Activate -> Lvchange.lvchange_activate copts vg_name lv_name physical_device false
      | Some Deactivate -> Lvchange.lvchange_deactivate copts vg_name lv_name
      | None -> ());
      return ()
    ) names
  )
 
let vgchange_cmd =
  let doc = "Activate or deactivate all logical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "vgchange allows you to activate or deactivate all the logical volumes in a volume group en-masse."
  ] in
  Term.(pure vgchange $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.physical_device_arg $ Xenvm_common.action_arg),
  Term.info "vgchange" ~sdocs:"COMMON OPTIONS" ~doc ~man
