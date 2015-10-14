(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt
open Errors

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


let lvs copts noheadings nosuffix units fields offline physical_device (vg_name,lv_name_opt) =
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
    let dev : string = match (info,physical_device) with
      | _, Some d -> d (* cmdline overrides default for the VG *)
      | Some info, None -> info.local_device (* If we've got a default, use that *)
      | None, None -> failwith "Need to know the local device!" in

    Lwt.catch
      (fun () ->
        if offline then begin
          with_block dev
            (fun x ->
              let module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock) in
              Vg_IO.connect [ x ] `RO >>|= fun vg ->
              return (Vg_IO.metadata_of vg)
            )
        end else Client.get ()
      ) (fun _ ->
        stderr "  Volume group \"%s\" not found" vg_name
        >>= fun () ->
        stderr "  Skipping volume group %s" vg_name
        >>= fun () ->
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
    let inner l = Lwt_list.fold_left_s (fun acc s -> s >>= fun s -> Lwt.return (s::acc)) [] l in
    inner (List.map inner rows) >>= fun rows ->
    let lines = print_table noheadings (" "::headings) (List.map (fun r -> " "::r) rows) in
    Lwt_list.iter_s (fun x -> stdout "%s" x) lines
  )

let lvs_cmd =
  let doc = "report information about logical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "lvs produces formatted output about logical volumes";
  ] in
  Term.(pure lvs $ Xenvm_common.copts_t $ noheadings_arg $ nosuffix_arg $ units_arg $ output_arg default_fields $ Xenvm_common.offline_arg $ Xenvm_common.physical_device_arg $ Xenvm_common.name_arg),
  Term.info "lvs" ~sdocs:"COMMON OPTIONS" ~doc ~man
