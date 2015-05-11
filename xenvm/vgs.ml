(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt
open Xenvm_common
open Lvm
module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock)
  
let default_fields = [
  "vg_name";
  "pv_count";
  "lv_count";
  "snap_count";
  "vg_attr";
  "vg_size";
  "vg_free"; ]


let vgs copts noheadings nosuffix units fields vg_names =
  let open Xenvm_common in
  let pvs = has_pv_field fields in
  let do_row (info,vg) =
    if not pvs
    then [row_of (vg,None,None,None) nosuffix units fields]
    else begin
      List.map (fun pv ->
	let pv = match info with
   | Some i -> {pv with Lvm.Pv.name = match Lvm.Pv.Name.of_string i.local_device with `Ok x -> x | _ -> failwith "Bad name"}
   | None -> pv
        in
   row_of (vg,Some pv,None,None) nosuffix units fields)
	vg.Lvm.Vg.pvs
    end
  in
  Lwt_main.run (
    let headings = headings_of fields in

    Lwt_list.map_s (fun (vg_name,_) ->
	get_vg_info_t copts vg_name >>= fun info ->
	set_uri copts info;
        let local_device = match info with
        | Some info -> info.local_device (* If we've got a default, use that *)
        | None -> failwith "Need to know the local device!" in
        Lwt.catch
          (fun () ->
            with_block local_device
              (fun x ->
                Vg_IO.connect [ x ] `RO >>|= fun vg ->
                return (Vg_IO.metadata_of vg) 
              )
          )
          (fun _ ->
            Printf.fprintf stderr "  Volume group \"%s\" not found\n" vg_name;
            Printf.fprintf stderr "  Skipping volume group %s\n%!" vg_name;
            exit 1)
        >>= fun vg ->
      Lwt.return (info,vg)) vg_names >>= fun vgs ->
    let rows = List.concat (List.map do_row vgs) in
    print_table noheadings (" "::headings) (List.map (fun r -> " "::r) rows);
    Lwt.return ()
  )

let vgs_cmd =
  let doc = "report information about a volume group" in
  let man = [
    `S "DESCRIPTION";
    `P "vgs produces formatted output about volume groups.";
  ] in
  Term.(pure vgs $ copts_t $ noheadings_arg $ nosuffix_arg $ units_arg $ output_arg default_fields $ names_arg),
  Term.info "vgs" ~sdocs:"COMMON OPTIONS" ~doc ~man
