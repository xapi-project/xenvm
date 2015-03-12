(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

(* see https://git.fedorahosted.org/cgit/lvm2.git/tree/lib/metadata/lv.c?id=v2_02_117#n643 
   for canonical description of this field. *)

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

    
let lvs copts noheadings units fields vg_name =
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->

    let headings = headings_of fields in
    let rows = List.map (fun lv -> row_of (vg,lv) units fields) vg.Lvm.Vg.lvs in
    print_table (" "::headings) (List.map (fun r -> " "::r) rows);
    Lwt.return ()
  )

let parse_output output_opt =
  match output_opt with
  | Some output ->
    if String.length output=0 then default_fields else begin
      let default,rest =
	if output.[0]='+'
	then default_fields,String.sub output 1 (String.length output - 1)
	else [],output
      in
      default @ (Stringext.split rest ',')
    end
  | None -> default_fields
    
let noheadings_arg =
  let doc = "Suppress the headings line that is normally the first line of output.  Useful if grepping the output." in
  Arg.(value & flag & info ["noheadings"] ~doc)

let units_arg =
  let doc = "All sizes are output in these units: (h)uman-readable, (b)ytes, (s)ectors, (k)ilobytes, (m)egabytes, (g)igabytes, (t)erabytes, (p)etabytes, (e)xabytes.  Capitalise to use multiples of 1000 (S.I.) instead of  1024." in
  Arg.(value & opt string "b" & info ["units"] ~doc)

let output_arg =
  let doc = "Comma-separated ordered list of columns.  Precede the list with '+' to append to the default selection of columns instead of replacing it." in
  let a = Arg.(value & opt (some string) None & info ["o";"options"] ~doc) in
  Term.(pure parse_output $ a)

let lvs_cmd =
  let doc = "report information about logical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "lvs produces formatted output about logical volumes";
  ] in
  Term.(pure lvs $ Xenvm_common.copts_t $ noheadings_arg $ units_arg $ output_arg $ Xenvm_common.name_arg),
  Term.info "lvs" ~sdocs:"COMMON OPTIONS" ~doc ~man
