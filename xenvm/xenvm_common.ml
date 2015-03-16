open Sexplib.Std
open Cmdliner
open Lwt
open Errors

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end
  
type fieldty =
  | Literal of string
  | Size of int64 (* Extents *)

let convert_size vg nosuffix units size =
  Printf.sprintf "%Ld%s" (Int64.mul (Int64.mul 512L vg.Lvm.Vg.extent_size) size) (if nosuffix then "" else "B")

type fieldfn =
  | Lv_fun of (Lvm.Lv.t -> fieldty)
  | Vg_fun of (Lvm.Vg.metadata -> fieldty)
  | VgLv_fun of (Lvm.Vg.metadata * Lvm.Lv.t -> fieldty)
  | Pv_fun of (Lvm.Pv.t -> fieldty)

type field = { key: string; name: string; fn:fieldfn }

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

let attr_of_vg vg =
  let open Lvm.Vg in
  Printf.sprintf "%c%c%c%c%c%c"
    (if List.mem Status.Write vg.status
     then 'w'
     else if List.mem Status.Read vg.status
     then 'r'
     else '-') (* Permissions: (w)riteable, (r)ead-only *)
    ('z') (* Resi(z)eable *)
    ('-') (* E(x)ported *)
    ('-') (* (p)artial: one or more physical volumes belonging to the volume group are missing from the system *)
    ('n') (* Allocation policy - (c)ontiguous, c(l)ing, (n)ormal, (a)nywhere *)
    ('-') (* clustered *)

let all_fields = [
  {key="lv_name"; name="LV"; fn=Lv_fun (fun lv -> Literal lv.Lvm.Lv.name) };
  {key="vg_name"; name="VG"; fn=Vg_fun (fun vg -> Literal vg.Lvm.Vg.name) };
  {key="lv_attr"; name="Attr"; fn=VgLv_fun (fun (vg,lv) -> Literal (attr_of_lv vg lv)) };
  {key="lv_size"; name="LSize"; fn=Lv_fun (fun lv -> Size (Lvm.Lv.size_in_extents lv)) };
  {key="pool_lv"; name="Pool"; fn=Lv_fun (fun _ -> Literal "")};
  {key="origin"; name="Origin"; fn=Lv_fun (fun _ -> Literal "")};
  {key="data_percent"; name="Data%"; fn=Lv_fun (fun _ -> Literal "")};
  {key="metadata_percent"; name="Meta%"; fn=Lv_fun (fun _ -> Literal "")};
  {key="move_pv"; name="Move"; fn=Lv_fun (fun _ -> Literal "")};
  {key="mirror_log"; name="Log"; fn=Lv_fun (fun _ -> Literal "")};
  {key="copy_percent"; name="Cpy%Sync"; fn=Lv_fun (fun _ -> Literal "")};
  {key="convert_lv"; name="Convert"; fn=Lv_fun (fun _ -> Literal "")};
  {key="lv_tags"; name="LV Tags"; fn=Lv_fun (fun lv -> Literal (String.concat "," (List.map Lvm.Name.Tag.to_string lv.Lvm.Lv.tags)))};
  
  {key="pv_count"; name="#PV"; fn=Vg_fun (fun vg -> Literal (string_of_int (List.length vg.Lvm.Vg.pvs)))};
  {key="lv_count"; name="#LV"; fn=Vg_fun (fun vg -> Literal (string_of_int (Lvm.Vg.LVs.cardinal vg.Lvm.Vg.lvs)))};
  {key="snap_count"; name="#SN"; fn=Vg_fun (fun _ -> Literal "0")};
  {key="vg_attr"; name="Attr"; fn=Vg_fun (fun vg -> Literal (attr_of_vg vg))};
  {key="vg_size"; name="VSize"; fn=Vg_fun (fun vg -> Size (List.fold_left (fun acc pv -> Int64.add acc pv.Lvm.Pv.pe_count) 0L vg.Lvm.Vg.pvs))};
  {key="vg_free"; name="VFree"; fn=Vg_fun (fun vg -> Size (Lvm.Pv.Allocator.size vg.Lvm.Vg.free_space))};
]

let row_of (vg,pv_opt,lv_opt) nosuffix units output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> (
	match field.fn,pv_opt,lv_opt with
        | (Vg_fun f),_ ,_-> (f vg)::acc
        | (VgLv_fun f),_,Some lv -> (f (vg,lv))::acc
        | (Lv_fun f), _,Some lv -> (f lv)::acc
        | (Pv_fun f), Some pv, _ -> (f pv)::acc
        | _,_,_ -> acc)
    | None -> acc) [] output |>
    List.map (function
    | Literal x -> x
    | Size y -> convert_size vg nosuffix units y) |> List.rev
      
let headings_of output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> field.name::acc
    | None -> acc) [] output |> List.rev



module Client = Xenvm_client.Client

let copts_sect = "COMMON OPTIONS"

type copts_t = {
  uri_override : string option; (* CLI set URI override *)
  config : string; 
}

let make_copts config uri_override = {uri_override; config}

let config =
  let doc = "Path to the config directory" in
  Arg.(value & opt dir "/etc/xenvm.d" & info [ "configdir" ] ~docv:"CONFIGDIR" ~doc)

let uri_arg =
  let doc = "Overrides the URI of the XenVM daemon in charge of the volume group." in
  Arg.(value & opt (some string) None & info ["u"; "uri"] ~docv:"URI" ~doc)

let uri_arg_required =
  let doc = "Overrides the URI of the XenVM daemon in charge of the volume group." in
  Arg.(required & opt (some string) None & info ["u"; "uri"] ~docv:"URI" ~doc)

let physical_device_arg =
    let doc = "Path to the (single) physical PV" in
    Arg.(value & opt (some string) None & info ["pvpath"] ~docv:"PV" ~doc)

let physical_device_arg_required =
    let doc = "Path to the (single) physical PV" in
    Arg.(required & opt (some string) None & info ["pvpath"] ~docv:"PV" ~doc)

let parse_name name_arg =
  let comps = Stringext.split name_arg '/' in
  match comps with
  | ["";"dev";vg;lv] -> (vg,Some lv)
  | ["";"dev";vg] -> (vg,None)
  | [vg;lv] -> (vg,Some lv)
  | [vg] -> (vg,None)
  | _ -> failwith "failed to parse vg name"

let name_arg =
  let doc = "Path to the volume group (and optionally LV). Usually of the form /dev/VGNAME[/LVNAME]" in
  let n = Arg.(required & pos 0 (some string) None & info [] ~docv:"NAME" ~doc) in
  Term.(pure parse_name $ n)

let names_arg =
  let doc = "Path to the volume groups. Usually of the form /dev/VGNAME" in
  let n = Arg.(non_empty & pos_all string [] & info [] ~docv:"VOLUMEGROUP" ~doc) in
  Term.(pure (List.map parse_name) $ n)

let noheadings_arg =
  let doc = "Suppress the headings line that is normally the first line of output.  Useful if grepping the output." in
  Arg.(value & flag & info ["noheadings"] ~doc)

let nosuffix_arg =
  let doc = "Suppress the printing of a suffix indicating the units of the sizes" in
  Arg.(value & flag & info ["nosuffix"] ~doc)

let force_arg =
  let doc = "Force the operation" in
  Arg.(value & flag & info ["force";"f"] ~doc)

let units_arg =
  let doc = "All sizes are output in these units: (h)uman-readable, (b)ytes, (s)ectors, (k)ilobytes, (m)egabytes, (g)igabytes, (t)erabytes, (p)etabytes, (e)xabytes.  Capitalise to use multiples of 1000 (S.I.) instead of  1024." in
  Arg.(value & opt string "b" & info ["units"] ~doc)

let parse_output default_fields output_opt =
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
    
let output_arg default_fields =
  let doc = "Comma-separated ordered list of columns.  Precede the list with '+' to append to the default selection of columns instead of replacing it." in
  let a = Arg.(value & opt (some string) None & info ["o";"options"] ~doc) in
  Term.(pure (parse_output default_fields) $ a)

let copts_t =
  Term.(pure make_copts $ config $ uri_arg)



type vg_info_t = {
  uri : string;
  local_device : string;
} with sexp

let set_vg_info_t copts uri local_device (vg_name,_) =
  let info = {uri; local_device} in
  let filename = Filename.concat copts.config vg_name in
  let s = sexp_of_vg_info_t info |> Sexplib.Sexp.to_string in
  Lwt.catch (fun () -> Lwt_io.with_file ~mode:Lwt_io.Output filename (fun f ->
      Lwt_io.fprintf f "%s" s))
    (function
    | Unix.Unix_error(Unix.ENOENT, _, s) ->
      Printf.fprintf stderr "Unable to open file: Does the config dir '%s' exist?\n" copts.config;
      exit 1
    | Unix.Unix_error(Unix.EACCES, _, _) ->
      Printf.fprintf stderr "Permission denied. You may need to rerun with 'sudo'\n";
      exit 1
    |e -> Lwt.fail e)

let run_set_vg_info_t config uri local_device vg_name =
  let copts = make_copts config (Some uri) in
  Lwt_main.run (set_vg_info_t copts uri local_device vg_name)
  
let get_vg_info_t copts vg_name =
  let open Lwt in
  let lift f = fun x -> Lwt.return (f x) in
  let filename = Filename.concat copts.config vg_name in
  Lwt.catch (fun () ->
    Lwt_io.with_file ~mode:Lwt_io.Input filename Lwt_io.read >>=
    lift Sexplib.Sexp.of_string >>=
    lift vg_info_t_of_sexp >>=
    lift (fun s -> Some s))
    (fun e -> Lwt.return None)


let set_vg_info_cmd =
  let doc = "Set the host-wide VG info" in
  let man = [
    `S "DESCRIPTION";
    `P "This command takes a physical device path and a URI, and will write these to the
filesystem. Subsequent xenvm commands will use these as defaults.";
  ] in
  Term.(pure run_set_vg_info_t $ config $ uri_arg_required $ physical_device_arg_required $ name_arg),
  Term.info "set-vg-info" ~sdocs:copts_sect ~doc ~man




let set_uri copts vg_info_opt =
  let uri = 
    match copts.uri_override with
    | Some uri -> uri
    | None ->
      match vg_info_opt with
      | Some info -> info.uri
      | None -> "http://127.0.0.1:4000/"
  in
  Xenvm_client.Rpc.uri := uri



let padto blank n s =
  let result = String.make n blank in
  String.blit s 0 result 0 (min n (String.length s));
  result

let print_table noheadings header rows =
  let nth xs i = try List.nth xs i with Not_found -> "" in
  let width_of_column i =
    let values = nth header i :: (List.map (fun r -> nth r i) rows) in
    let widths = List.map String.length values in
    List.fold_left max 0 widths in
  let widths = List.rev (snd(List.fold_left (fun (i, acc) _ -> (i + 1, (width_of_column i) :: acc)) (0, []) header)) in
  let print_row row =
    List.iter (fun (n, s) -> Printf.printf "%s " (padto ' ' n s)) (List.combine widths row);
    Printf.printf "\n" in
  if not noheadings then print_row header;
  List.iter print_row rows



