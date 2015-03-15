(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt

let default_fields = [
 "lv_name";
 "vg_name";
 "lv_attr";
 "lv_size";
 "pool_lv";
 "data_percent";
 "metadata_percent";
 "move_pv";
 "mirror_log";
 "copy_percent";
 "convert_lv"]


let lvs copts noheadings units fields (vg_name,lv_name_opt) =
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->

    let headings = headings_of fields in
    let rows =
      let lvs = Lvm.Vg.LVs.fold (fun _ lv acc -> lv :: acc) vg.Lvm.Vg.lvs [] in
      match lv_name_opt with
      | None -> List.map (fun lv -> row_of (vg,Some lv) false units fields) lvs
      | Some lv_name ->
        let lv = List.find (fun lv -> lv.Lvm.Lv.name = lv_name) lvs in
        [row_of (vg,Some lv) false units fields]
    in
    print_table noheadings (" "::headings) (List.map (fun r -> " "::r) rows);
    Lwt.return ()
  )

let lvs_cmd =
  let doc = "report information about logical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "lvs produces formatted output about logical volumes";
  ] in
  Term.(pure lvs $ Xenvm_common.copts_t $ noheadings_arg $ units_arg $ output_arg default_fields $ Xenvm_common.name_arg),
  Term.info "lvs" ~sdocs:"COMMON OPTIONS" ~doc ~man
