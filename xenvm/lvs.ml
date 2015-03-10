(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

(* see https://git.fedorahosted.org/cgit/lvm2.git/tree/lib/metadata/lv.c?id=v2_02_117#n643 
   for canonical description of this field. *)
let attr_of_lv vg lv =
  let name = Mapper.name_of vg lv in
  let info = Devmapper.stat name in
  Printf.sprintf "%c%c%c%c%c%c%c%c%c%c"
    ('-')
    (if List.mem Lvm.Lv.Status.Write lv.Lvm.Lv.status
     then 'w'
     else if List.mem Lvm.Lv.Status.Read lv.Lvm.Lv.status
     then 'r'
     else '-')
    ('i')
    ('-')
    (match info with
    | Some i ->
      if i.Devmapper.suspended then 's'
      else if i.Devmapper.live_table <> 0 then 'a'
      else if i.Devmapper.inactive_table <> 0 then 'i'
      else 'd'
    | None -> '-')
    (match info with
    | Some i -> if i.Devmapper.open_count > 0l then 'o' else '-'
    | None -> '-')
    ('-')
    ('-')
    ('-')
    ('-')

let convert_size vg units size =
  Printf.sprintf "%LdB" (Int64.mul (Int64.mul 512L vg.Lvm.Vg.extent_size) size)

type fieldty =
  | Literal of string
  | Size of int64 (* Extents *)
      
type field = { key: string; name: string; fn:Lvm.Vg.metadata * Lvm.Lv.t -> fieldty }
	     
let all_fields = [
  {key="lv_name"; name="LV"; fn=(fun (_,lv) -> Literal lv.Lvm.Lv.name) };
  {key="vg_name"; name="VG"; fn=(fun (vg,_) -> Literal vg.Lvm.Vg.name) };
  {key="lv_attr"; name="Attr"; fn=(fun (vg,lv) -> Literal (attr_of_lv vg lv)) };
  {key="lv_size"; name="LSize"; fn=(fun (_,lv) -> Size (Lvm.Lv.size_in_extents lv)) };
  {key="pool_lv"; name="Pool"; fn=(fun _ -> Literal "")};
  {key="data_percent"; name="Data%"; fn=(fun _ -> Literal "")};
  {key="metadata_percent"; name="Meta%"; fn=(fun _ -> Literal "")};
  {key="move_pv"; name="Move"; fn=(fun _ -> Literal "")};
  {key="mirror_log"; name="Log"; fn=(fun _ -> Literal "")};
  {key="copy_percent"; name="Cpy%Sync"; fn=(fun _ -> Literal "")};
  {key="convert_lv"; name="Convert"; fn=(fun _ -> Literal "")};
  {key="lv_tags"; name="LV Tags"; fn=(fun (_,lv) -> Literal (String.concat "," (List.map Lvm.Tag.to_string lv.Lvm.Lv.tags)))}
]

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

let row_of_lv (vg,lv) units output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> (field.fn (vg,lv))::acc
    | None -> acc) [] output |>
    List.map (function
    | Literal x -> x
    | Size y -> convert_size vg units y) |> List.rev
      
let headings_of output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> field.name::acc
    | None -> acc) [] output |> List.rev
    
let lvs copts noheadings units fields vg_name =
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->

    let headings = headings_of fields in
    let rows = List.map (fun lv -> row_of_lv (vg,lv) units fields) vg.Lvm.Vg.lvs in
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
    
let parse_vg_name name_arg =
  let comps = Stringext.split name_arg '/' in
  match comps with
  | ["";"dev";vg] -> vg
  | [vg] -> vg
  | _ -> failwith "failed to parse vg name"

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

let name_arg =
  let doc = "Path to the volume group. Usually of the form /dev/VGNAME" in
  let n = Arg.(required & pos 0 (some string) None & info [] ~docv:"VOLUMEGROUP" ~doc) in
  Term.(pure parse_vg_name $ n)

let lvs_cmd =
  let doc = "report information about logical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "lvs produces formatted output about logical volumes";
  ] in
  Term.(pure lvs $ Xenvm_common.copts_t $ noheadings_arg $ units_arg $ output_arg $ name_arg),
  Term.info "lvs" ~sdocs:"COMMON OPTIONS" ~doc ~man
