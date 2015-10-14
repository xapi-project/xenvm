(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt
open Errors

let default_fields = [
 "pv_name";
 "vg_name";
 "pv_fmt";
 "pv_attr";
 "pv_size";
 "pv_free";
]
open Lvm
module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock)

let pvs copts noheadings nosuffix units fields devices =
  let open Xenvm_common in
  Lwt_main.run (
    let read device =
      with_block device
        (fun x ->
          Vg_IO.connect [ x ] `RO >>|= fun vg ->
          return vg 
        ) in
    Lwt_list.map_s read devices
    >>= fun vgs ->
    let do_row vg =
      List.map (fun pv ->
        row_of (vg,Some pv,None,None) nosuffix units fields)
      vg.Lvm.Vg.pvs in
    let headings = headings_of fields in
    let rows = List.concat (List.map (fun vg -> do_row (Vg_IO.metadata_of vg)) vgs) in
    let inner l = Lwt_list.fold_left_s (fun acc s -> s >>= fun s -> Lwt.return (s::acc)) [] l in
    inner (List.map inner rows) >>= fun rows ->
    let lines = print_table noheadings (" "::headings) (List.map (fun r -> " "::r) rows) in
    Lwt_list.iter_s (fun x -> stdout "%s" x) lines
  )

let pvs_cmd =
  let doc = "report information about physical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "pvs produces formatted output about physical volumes";
  ] in
  Term.(pure pvs $ Xenvm_common.copts_t $ noheadings_arg $ nosuffix_arg $ units_arg $ output_arg default_fields $ Xenvm_common.devices_arg),
  Term.info "pvs" ~sdocs:"COMMON OPTIONS" ~doc ~man
