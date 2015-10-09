open Sexplib.Std
open Cmdliner
open Lwt
open Errors

let dm = ref (module Retrymapper.Make(Devmapper.Linux) : S.RETRYMAPPER)

let syslog = Lwt_log.syslog ~facility:`Daemon ()

let stdout fmt = Printf.ksprintf (fun s ->
  Printf.printf "%s\n%!" s;
  Lwt_log.log ~logger:syslog ~level:Lwt_log.Notice ("stdout:" ^ s)
) fmt
let stderr fmt = Printf.ksprintf (fun s ->
  Printf.fprintf stderr "%s\n%!" s;
  Lwt_log.log ~logger:syslog ~level:Lwt_log.Notice ("stderr:" ^ s)
) fmt

let mkdir_rec dir perm =
  let mkdir_safe dir perm =
    try Unix.mkdir dir perm with Unix.Unix_error (Unix.EEXIST, _, _) -> () in
  let rec p_mkdir dir =
    let p_name = Filename.dirname dir in
    if p_name <> "/" && p_name <> "."
    then p_mkdir p_name;
    mkdir_safe dir perm in
  p_mkdir dir

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end
  
type fieldty =
  | Literal of string
  | LiteralLwt of string Lwt.t
  | Size of int64 (* Sectors *)

let convert_size vg nosuffix units size =
  Printf.sprintf "%Ld%s" (Int64.mul 512L size) (if nosuffix then "" else "B")

type fieldfn =
  | Lv_fun of (Lvm.Lv.t -> fieldty)
  | Vg_fun of (Lvm.Vg.metadata -> fieldty)
  | VgLv_fun of (Lvm.Vg.metadata * Lvm.Lv.t -> fieldty)
  | Pv_fun of (Lvm.Pv.t -> fieldty)
  | VgPv_fun of (Lvm.Vg.metadata * Lvm.Pv.t -> fieldty)
  | Seg_fun of (Lvm.Lv.Segment.t -> fieldty)
  | VgSeg_fun of (Lvm.Vg.metadata * Lvm.Lv.Segment.t -> fieldty)

type field = { key: string; name: string; fn:fieldfn }

(* see https://git.fedorahosted.org/cgit/lvm2.git/tree/lib/metadata/lv.c?id=v2_02_117#n643 
   for canonical description of this field. *)

let attr_of_lv vg lv =
  let module Devmapper = (val !dm: S.RETRYMAPPER) in

  (* Since we're single-shot, cache the active devices here *)
  let devmapper_ls =
    let cache = ref None in
    fun () ->
      match !cache with
      | None ->
        let ls = Devmapper.ls () in
        cache := Some ls;
        ls
      | Some ls ->
        ls
  in

  let devmapper_stat name =
    devmapper_ls () >>= fun all ->
    if List.mem name all
    then Devmapper.stat name
    else Lwt.return None
  in

  let name = Mapper.name_of vg.Lvm.Vg.name lv.Lvm.Lv.name in

  devmapper_stat name >>= fun info ->

  Lwt.return (Printf.sprintf "%c%c%c%c%c%c%c%c%c%c"
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
    ('-'))

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

let attr_of_pv pv =
  let open Lvm.Pv in
  Printf.sprintf "%c--"
    (if List.mem Status.Allocatable pv.status
     then 'a'
     else '-')

let devices_of_seg seg =
  let open Lvm.Lv in
  match seg.Segment.cls with
  | Segment.Linear x ->
    Printf.sprintf "%s(%Ld)" (Lvm.Pv.Name.to_string x.Linear.name) x.Linear.start_extent

let all_fields = [
  {key="lv_name"; name="LV"; fn=Lv_fun (fun lv -> Literal lv.Lvm.Lv.name) };
  {key="vg_name"; name="VG"; fn=Vg_fun (fun vg -> Literal vg.Lvm.Vg.name) };
  {key="vg_extent_size"; name="Ext"; fn=Vg_fun (fun vg -> Size vg.Lvm.Vg.extent_size) };
  {key="lv_attr"; name="Attr"; fn=VgLv_fun (fun (vg,lv) -> LiteralLwt (attr_of_lv vg lv)) };
  {key="lv_size"; name="LSize"; fn=VgLv_fun (fun (vg,lv) -> Size (Int64.mul vg.Lvm.Vg.extent_size (Lvm.Lv.size_in_extents lv))) };
  {key="pool_lv"; name="Pool"; fn=Lv_fun (fun _ -> Literal "")};
  {key="origin"; name="Origin"; fn=Lv_fun (fun _ -> Literal "")};
  {key="data_percent"; name="Data%"; fn=Lv_fun (fun _ -> Literal "")};
  {key="metadata_percent"; name="Meta%"; fn=Lv_fun (fun _ -> Literal "")};
  {key="move_pv"; name="Move"; fn=Lv_fun (fun _ -> Literal "")};
  {key="mirror_log"; name="Log"; fn=Lv_fun (fun _ -> Literal "")};
  {key="copy_percent"; name="Cpy%Sync"; fn=Lv_fun (fun _ -> Literal "")};
  {key="convert_lv"; name="Convert"; fn=Lv_fun (fun _ -> Literal "")};
  {key="pv_fmt"; name="Fmt"; fn=Pv_fun (fun _ -> Literal "lvm2")};
  {key="pv_attr"; name="Attr"; fn=Pv_fun (fun pv -> Literal (attr_of_pv pv))};

  {key="pv_size"; name="PSize"; fn=VgPv_fun (fun (vg, pv) -> Size (Int64.mul vg.Lvm.Vg.extent_size pv.Lvm.Pv.pe_count))};
  {key="pv_free"; name="PFree"; fn=VgPv_fun (fun (vg, pv) -> Size (Int64.mul vg.Lvm.Vg.extent_size (Lvm.Pv.Allocator.size (List.filter (fun (name, _) -> name = pv.Lvm.Pv.name) vg.Lvm.Vg.free_space))))};

  {key="lv_tags"; name="LV Tags"; fn=Lv_fun (fun lv -> Literal (String.concat "," (List.map Lvm.Name.Tag.to_string lv.Lvm.Lv.tags)))};
  
  {key="pv_count"; name="#PV"; fn=Vg_fun (fun vg -> Literal (string_of_int (List.length vg.Lvm.Vg.pvs)))};
  {key="lv_count"; name="#LV"; fn=Vg_fun (fun vg -> Literal (string_of_int (Lvm.Vg.LVs.cardinal vg.Lvm.Vg.lvs)))};
  {key="snap_count"; name="#SN"; fn=Vg_fun (fun _ -> Literal "0")};
  {key="vg_attr"; name="Attr"; fn=Vg_fun (fun vg -> Literal (attr_of_vg vg))};
  {key="vg_size"; name="VSize"; fn=Vg_fun (fun vg -> Size (Int64.mul vg.Lvm.Vg.extent_size (List.fold_left (fun acc pv -> Int64.add acc pv.Lvm.Pv.pe_count) 0L vg.Lvm.Vg.pvs)))};
  {key="vg_free"; name="VFree"; fn=Vg_fun (fun vg -> Size (Int64.mul vg.Lvm.Vg.extent_size (Lvm.Pv.Allocator.size vg.Lvm.Vg.free_space)))};
  {key="pv_name"; name="PV"; fn=Pv_fun (fun pv -> Literal (Lvm.Pv.Name.to_string pv.Lvm.Pv.name))};
  {key="pe_start"; name="1st PE"; fn=Pv_fun (fun pv -> Size pv.Lvm.Pv.pe_start)};
  {key="segtype"; name="Type"; fn=Seg_fun (fun seg -> Literal (match seg.Lvm.Lv.Segment.cls with | Lvm.Lv.Segment.Linear _ -> "linear"))};
  {key="seg_count"; name="#Seg"; fn=Lv_fun (fun lv -> Literal (string_of_int (List.length lv.Lvm.Lv.segments)))};
  {key="seg_start"; name="Start"; fn=VgSeg_fun (fun (vg,seg) -> Size (Int64.mul vg.Lvm.Vg.extent_size seg.Lvm.Lv.Segment.start_extent))};
  {key="seg_size"; name="SSize"; fn=VgSeg_fun (fun (vg,seg) -> Size (Int64.mul vg.Lvm.Vg.extent_size seg.Lvm.Lv.Segment.extent_count))};
  {key="devices"; name="Devices"; fn=Seg_fun (fun seg -> Literal (devices_of_seg seg))};
  
]

let has_pv_field fields =
  List.exists (fun field_name ->
    try
      let l = List.find (fun f -> f.key = field_name) all_fields in
      match l.fn with
      | Pv_fun _ -> true
      | _ -> false
    with _ ->
      false) fields

let has_seg_field fields =
  List.exists (fun field_name ->
    try
      let l = List.find (fun f -> f.key = field_name) all_fields in
      match l.fn with
      | Seg_fun _ 
      | VgSeg_fun _ -> true
      | _ -> false
    with _ ->
      false) fields



let row_of (vg,pv_opt,lv_opt,seg_opt) nosuffix units output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> (
	match field.fn,pv_opt,lv_opt,seg_opt with
        | (Vg_fun f),_,_,_-> (f vg)::acc
        | (VgLv_fun f),_,Some lv,_ -> (f (vg,lv))::acc
        | (Lv_fun f),_,Some lv,_ -> (f lv)::acc
        | (Pv_fun f),Some pv,_,_ -> (f pv)::acc
        | (VgPv_fun f),Some pv,_,_ -> (f (vg, pv))::acc
        | (Seg_fun f),_,_,Some s -> (f s)::acc
        | (VgSeg_fun f),_,_,Some s -> (f (vg,s))::acc
        | _,_,_,_ -> acc)
    | None -> acc) [] output |>
    List.map (function
    | Literal x -> Lwt.return x
    | Size y -> Lwt.return (convert_size vg nosuffix units y)
    | LiteralLwt x -> x) |> List.rev
      
let headings_of output =
  List.fold_left (fun acc name ->
    match (try Some (List.find (fun f -> f.key=name) all_fields) with _ -> None) with
    | Some field -> field.name::acc
    | None -> acc) [] output |> List.rev



module Client = Xenvm_client.Client

let copts_sect = "COMMON OPTIONS"

type copts_t = {
  uri_override : string option; (* CLI set URI override *)
  sockpath_override : string option; (* CLI set unix domain socket path override *)
  config : string;
}

let make_copts config uri_override sockpath_override mock_dm =
  dm := if mock_dm then (module Retrymapper.Make(Devmapper.Mock) : S.RETRYMAPPER) else (module Retrymapper.Make(Devmapper.Linux) : S.RETRYMAPPER);
  { uri_override; config; sockpath_override }

let config =
  let doc = "Path to the config directory" in
  Arg.(value & opt dir "/var/run/nonpersistent/xenvm.d" & info [ "configdir" ] ~docv:"CONFIGDIR" ~doc)

let uri_arg =
  let doc = "Overrides the URI of the XenVM daemon in charge of the volume group." in
  Arg.(value & opt (some string) None & info ["u"; "uri"] ~docv:"URI" ~doc)

let uri_arg_required =
  let doc = "Overrides the URI of the XenVM daemon in charge of the volume group." in
  Arg.(required & opt (some string) None & info ["u"; "uri"] ~docv:"URI" ~doc)

let sock_path_arg =
  let doc = "Path to the local domain socket. Only used for file://local/ URIs" in
  Arg.(value & opt (some string) None & info [ "S"; "sockpath"] ~docv:"PATH" ~doc)

let local_allocator_path =
  let doc = "Path to the Unix domain socket where the local allocator is running." in
  Arg.(value & opt (some string) None & info [ "local-allocator-path" ] ~docv:"LOCAL" ~doc)

let physical_device_arg =
    let doc = "Path to the (single) physical PV" in
    Arg.(value & opt (some string) None & info ["pvpath"] ~docv:"PV" ~doc)

let physical_device_arg_required =
    let doc = "Path to the (single) physical PV" in
    Arg.(required & opt (some string) None & info ["pvpath"] ~docv:"PV" ~doc)

let parse_name name_arg =
  let comps = Stringext.split name_arg '/' in
  match comps with
  | ["";"dev";vg;""]
  | ["";"dev";vg] -> (vg,None)
  | ["";"dev";vg;lv] -> (vg,Some lv)
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

let devices_arg =
  let doc = "Path to the devices" in
  Arg.(non_empty & pos_all file [] & info [] ~docv:"DEVICE" ~doc)

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

let mock_dm_arg =
  let doc = "Enable mock interfaces on device mapper." in
  Arg.(value & flag & info ["mock-devmapper"] ~doc)

let copts_t =
  Term.(pure make_copts $ config $ uri_arg $ sock_path_arg $ mock_dm_arg)

let kib = 1024L
let sectors = 512L
let mib = Int64.mul kib 1024L
let gib = Int64.mul mib 1024L
let tib = Int64.mul gib 1024L
  
let parse_size_string =
  let re = Re_emacs.compile_pat "\\([0-9]+\\)\\(.*\\)" in
  function s -> 
    try
      let substrings = Re.exec re s in
      let multiplier =
        match Re.get substrings 2 with
        | "" | "M" | "m" -> mib
        | "s" | "S" -> sectors
        | "b" | "B" -> 1L
        | "k" | "K" -> kib
        | "g" | "G" -> gib
        | "t" | "T" -> tib
        | _ -> failwith "Unknown suffix"
      in
      Re.get substrings 1 |> Int64.of_string |> Int64.mul multiplier
    with
    | Not_found ->
      failwith "Expecting a size of the form [0-9]+[mMbBkKgGtTsS]?"

(* LogicalExtentsNumber[%{VG|PVS|FREE|ORIGIN}] in the man page
   but the command will apparently also accept '100%F' *)
let parse_percent_size_string s =
  try
    let i = String.index s '%' in
    let percent = Int64.of_string (String.sub s 0 i) in
    match String.sub s (i + 1) (String.length s - i - 1) with
    | "F" | "FREE" -> `Free percent
    | "VG" -> failwith "Creating an LV with a %age of VG is not implemented yet"
    | "PVS" -> failwith "Creating an LV with a %age of PVS is not implemented yet"
    | "ORIGIN" -> failwith "Creating an LV with a %age of ORIGIN is not implemented yet"
    | x -> failwith (Printf.sprintf "I don't understand the %%age size string: %s; expected [VG|PVS|FREE|ORIGIN]" x)
  with Not_found ->
    `Extents (Int64.of_string s)

let parse_size real_size percent_size = match real_size, percent_size with
  | Some x, None ->
    begin match x.[0] with
    | '+' -> `IncreaseBy (parse_size_string (String.sub x 1 (String.length x - 1)))
    | '-' -> `DecreaseBy (parse_size_string (String.sub x 1 (String.length x - 1)))
    | _ -> `Absolute (parse_size_string x)
    end
  | None, Some y -> parse_percent_size_string y
  | Some _, Some _ -> failwith "Please don't give two sizes!"
  | None, None -> failwith "Need a size!"

let real_size_arg =
  let doc = "Gives the size to  allocate for the logical volume. A size suffix of B for bytes, S for sectors as 512 bytes, K for kilobytes, M for megabytes, G for gigabytes, T for terabytes. Default unit is megabytes." in
  Arg.(value & opt (some string) None & info ["L"; "size"] ~docv:"SIZE" ~doc)

let percent_size_arg =
  let doc = "Gives the size to allocate for the expressed as a percentage of the total space in the Volume Group with the suffix %VG, or as a percentage of the remaining free space in the Volume Group with the suffix %FREE" in
  Arg.(value & opt (some string) None & info ["l"] ~docv:"PERCENTAGE%{VG|FREE}" ~doc)

type vg_info_t = {
  uri : string;
  local_device : string;
  local_allocator_path : string option;
  unix_domain_sock_path : string option;
} with sexp

let set_vg_info_t copts uri local_device local_allocator_path unix_domain_sock_path (vg_name,_) =
  let info = {uri; local_device; local_allocator_path; unix_domain_sock_path } in
  let filename = Filename.concat copts.config vg_name in
  let s = sexp_of_vg_info_t info |> Sexplib.Sexp.to_string in
  Lwt.catch (fun () -> Lwt_io.with_file ~mode:Lwt_io.Output filename (fun f ->
      Lwt_io.fprintf f "%s" s))
    (function
    | Unix.Unix_error(Unix.ENOENT, _, s) ->
      stderr "Unable to open file: Does the config dir '%s' exist?" copts.config
      >>= fun () ->
      exit 1
    | Unix.Unix_error(Unix.EACCES, _, _) ->
      stderr "Permission denied. You may need to rerun with 'sudo'"
      >>= fun () ->
      exit 1
    |e -> Lwt.fail e)

let run_set_vg_info_t config uri local_allocator_path local_device unix_domain_sock_path vg_name mock_dm =
  let copts = make_copts config (Some uri) unix_domain_sock_path mock_dm in
  Lwt_main.run (set_vg_info_t copts uri local_device local_allocator_path unix_domain_sock_path vg_name)
  
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
  Term.(pure run_set_vg_info_t $ config $ uri_arg_required $ local_allocator_path $ physical_device_arg_required $ sock_path_arg $ name_arg $ mock_dm_arg),
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
  Xenvm_client.Rpc.uri := uri;
  match copts.sockpath_override with
  | Some x -> Xenvm_client.unix_domain_socket_path := x
  | None ->
    match vg_info_opt with
    | Some { unix_domain_sock_path=Some x } ->
      Xenvm_client.unix_domain_socket_path := x
    | _ -> ()

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
    String.concat "" (List.map (fun (n, s) -> Printf.sprintf "%s " (padto ' ' n s)) (List.combine widths row)) in
  if noheadings
  then List.map print_row rows
  else print_row header :: (List.map print_row rows)

let with_block filename f =
  let open Lwt in
  Block.connect filename
  >>= function
  | `Error _ -> fail (Failure (Printf.sprintf "Unable to read %s" filename))
  | `Ok x ->
    Lwt.catch (fun () -> f x) (fun e -> Block.disconnect x >>= fun () -> fail e)

type action = Activate | Deactivate

let action_arg =
  let parse_action c =
    match c with
    | Some 'y' -> Some Activate
    | Some 'n' -> Some Deactivate
    | Some _ -> failwith "Unknown activation argument"
    | None -> None
  in
  let doc = "Controls the availability of the logical volumes for use.  Communicates with the kernel device-mapper driver via libdevmapper to activate (-ay) or deactivate (-an) the logical volumes.

Activation  of a logical volume creates a symbolic link /dev/VolumeGroupName/LogicalVolumeName pointing to the device node.  This link is removed on deactivation.  All software and scripts should access the device through this symbolic link and present this as the name of the device.  The location and name of the underlying device node may depend on the  distribution and configuration (e.g. udev) and might change from release to release." in
  let a = Arg.(value & opt (some char) None & info ["a"] ~docv:"ACTIVATE" ~doc) in
  Term.(pure parse_action $ a)

let offline_arg =
  let doc = "Assume xenvmd is offline and read metadata from the disk"
  in
  Arg.(value & flag & info [ "offline" ] ~docv:"OFFLINE" ~doc)
