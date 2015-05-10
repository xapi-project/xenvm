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


let lvs copts noheadings nosuffix units fields (vg_name,lv_name_opt) =
  let open Xenvm_common in
  let segs = has_seg_field fields in
  let do_row dev vg lv =
    if not segs
    then [row_of (vg,None,Some lv,None) nosuffix units fields]
    else begin
      List.map (fun seg ->
	let seg =
   match seg.Lvm.Lv.Segment.cls with
   | Lvm.Lv.Segment.Linear x ->
     {seg with Lvm.Lv.Segment.cls = Lvm.Lv.Segment.Linear {x with Lvm.Lv.Linear.name = match Lvm.Pv.Name.of_string dev with `Ok x -> x | _ -> failwith "Bad name"}} in
	row_of (vg,None,Some lv,Some seg) nosuffix units fields)
	lv.Lvm.Lv.segments
    end in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let dev = match info with | Some i -> i.local_device | None -> "<unknown>" in
    Lwt.catch
      (Client.get)
      (fun _ ->
        Printf.fprintf stderr "  Volume group \"%s\" not found\n" vg_name;
        Printf.fprintf stderr "  Skipping volume group %s\n%!" vg_name;
        exit 1)
    >>= fun vg ->

    let headings = headings_of fields in
    let rows =
      let lvs = Lvm.Vg.LVs.fold (fun _ lv acc -> lv :: acc) vg.Lvm.Vg.lvs [] in
      match lv_name_opt with
      | None -> List.concat (List.map (do_row dev vg) lvs)
      | Some lv_name ->
        let lv = List.find (fun lv -> lv.Lvm.Lv.name = lv_name) lvs in
	do_row dev vg lv
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
  Term.(pure lvs $ Xenvm_common.copts_t $ noheadings_arg $ nosuffix_arg $ units_arg $ output_arg default_fields $ Xenvm_common.name_arg),
  Term.info "lvs" ~sdocs:"COMMON OPTIONS" ~doc ~man
