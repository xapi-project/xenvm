open Sexplib.Std
open Cmdliner
open Lwt
  
let (>>|=) m f = m >>= function
  | `Error e -> fail (Failure e)
  | `Ok x -> f x

type fieldty =
  | Literal of string
  | Size of int64 (* Extents *)

let convert_size vg units size =
  Printf.sprintf "%LdB" (Int64.mul (Int64.mul 512L vg.Lvm.Vg.extent_size) size)

type fieldfn =
  | Lv_fun of (Lvm.Lv.t -> fieldty)
  | Vg_fun of (Lvm.Vg.metadata -> fieldty)
  | VgLv_fun of (Lvm.Vg.metadata * Lvm.Lv.t -> fieldty)

type field = { key: string; name: string; fn:fieldfn }

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
  {key="lv_tags"; name="LV Tags"; fn=Lv_fun (fun lv -> Literal (String.concat "," (List.map Lvm.Tag.to_string lv.Lvm.Lv.tags)))};
]

let row_of (vg,lv_opt) units output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> (
	match field.fn,lv_opt with
        | (Vg_fun f),_ -> (f vg)::acc
        | (VgLv_fun f),Some lv -> (f (vg,lv))::acc
        | (Lv_fun f), Some lv -> (f lv)::acc
        | _,_ -> acc)
    | None -> acc) [] output |>
    List.map (function
    | Literal x -> x
    | Size y -> convert_size vg units y) |> List.rev
      
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

let parse_vg_name name_arg =
  let comps = Stringext.split name_arg '/' in
  match comps with
  | ["";"dev";vg] -> vg
  | [vg] -> vg
  | _ -> failwith "failed to parse vg name"

let name_arg =
  let doc = "Path to the volume group. Usually of the form /dev/VGNAME" in
  let n = Arg.(required & pos 0 (some string) None & info [] ~docv:"VOLUMEGROUP" ~doc) in
  Term.(pure parse_vg_name $ n)

let copts_t =
  Term.(pure make_copts $ config $ uri_arg)



type vg_info_t = {
  uri : string;
  local_device : string;
} with sexp

let set_vg_info_t copts uri local_device vg_name =
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

let print_table header rows =
  let nth xs i = try List.nth xs i with Not_found -> "" in
  let width_of_column i =
    let values = nth header i :: (List.map (fun r -> nth r i) rows) in
    let widths = List.map String.length values in
    List.fold_left max 0 widths in
  let widths = List.rev (snd(List.fold_left (fun (i, acc) _ -> (i + 1, (width_of_column i) :: acc)) (0, []) header)) in
  let print_row row =
    List.iter (fun (n, s) -> Printf.printf "%s " (padto ' ' n s)) (List.combine widths row);
    Printf.printf "\n" in
  print_row header;
  List.iter print_row rows



