(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let lvresize copts (vg_name,lv_opt) real_size percent_size =
  let lv_name = match lv_opt with | Some l -> l | None -> failwith "Need an LV name" in
  let open Xenvm_common in
  let size = parse_size real_size percent_size in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid VG name";
    Client.resize lv_name size)

let lvresize_cmd =
  let doc = "Resize a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvresize will resize an existing logical volume.";
  ] in
  Term.(pure lvresize $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.real_size_arg $ Xenvm_common.percent_size_arg),
  Term.info "lvresize" ~sdocs:"COMMON OPTIONS" ~doc ~man

  
