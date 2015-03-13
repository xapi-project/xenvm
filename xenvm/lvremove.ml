(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let lvremove copts (vg_name,lv_opt) force =
  let lv_name = match lv_opt with | Some l -> l | None -> failwith "Need an LV name" in
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid VG name";
    Client.remove lv_name)

let lvremove_cmd =
  let doc = "Remove a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvcreate creates a new logical volume in a volume group by allocating logical extents from the free physical extent pool of that volume group.  If there are not enough free physical extents then the volume group can be extended with other physical volumes or by reducing existing logical volumes of this volume group in size."
  ] in
  Term.(pure lvremove $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.force_arg),
  Term.info "lvremove" ~sdocs:"COMMON OPTIONS" ~doc ~man

  
