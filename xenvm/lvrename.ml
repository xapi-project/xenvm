(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let lvrename copts (vg_name,lv_opt) newname =
  let lv_name = match lv_opt with | Some l -> l | None -> failwith "Need an LV name" in
  (* It seems you can say "vg/lv" or "lv" *)
  let newname = match newname with
  | _, Some lv -> lv
  | lv, _ -> lv in
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid VG name";
    Client.rename ~oldname:lv_name ~newname:newname)

let new_name_arg =
  let doc = "New name for the LV" in
  let n = Arg.(required & pos 1 (some string) None & info [] ~docv:"NEWNAME" ~doc) in
  Term.(pure Xenvm_common.parse_name $ n)

let lvrename_cmd =
  let doc = "Rename a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvrename renames an existing logical volume with a new name. The contents of the logical volume are unchanged."
  ] in
  Term.(pure lvrename $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ new_name_arg),
  Term.info "lvrename" ~sdocs:"COMMON OPTIONS" ~doc ~man

  
