(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let uri_arg =
  let doc = "Gives the URI of the XenVM daemon in charge of the volume group." in
  Arg.(value & opt string "http://localhost:4000/" & info ["u"; "uri"] ~docv:"URI" ~doc)



(* lvcreate -n <name> vgname -l <size_in_percent> -L <size_in_mb> --addtag tag *)
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

let parse_percent_size_string s = failwith "Unimplemented"
	
let lvcreate uri lv_name real_size percent_size tags vg_name =
  let size = match real_size, percent_size with
    | Some x, None -> parse_size_string x
    | None, Some y -> parse_percent_size_string y
    | Some _, Some _ -> failwith "Please don't give two sizes!"
    | None, None -> failwith "Need a size!" in
  let open Xenvm_common in
  Lwt_main.run (
    set_uri uri;
    Client.get () >>= fun vg ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid VG name";
    Client.create lv_name size)

let lv_name_arg =
  let doc = "Gives the name of the LV to be created. This must be unique within the volume group. " in
  Arg.(value & opt string "lv" & info ["n"; "name"] ~docv:"LVNAME" ~doc)

let real_size_arg =
  let doc = "Gives the size to  allocate for the new logical volume. A size suffix of B for bytes, S for sectors as 512 bytes, K for kilobytes, M for megabytes, G for gigabytes, T for terabytes. Default unit is megabytes." in
  Arg.(value & opt (some string) None & info ["L"; "size"] ~docv:"SIZE" ~doc)

let percent_size_arg =
  let doc = "Gives the size to allocate for the expressed as a percentage of the total space in the Volume Group with the suffix %VG, or as a percentage of the remaining free space in the Volume Group with the suffix %FREE" in
  Arg.(value & opt (some string) None & info ["l"] ~docv:"PERCENTAGE%{VG|FREE}" ~doc)

let vg_name_arg =
  let doc = "Specify the volume group in which to create the logical volume." in
  Arg.(required & pos 0 (some string) None & info [] ~docv:"VOLUMEGROUP" ~doc)

let tags_arg =
  let doc = "Specify that a tag should be added to the LV when it is created. This may be specified more than once to add multiple tags." in
  Arg.(value & opt_all string [] & info ["addtag"] ~docv:"TAG" ~doc)

let lvcreate_cmd =
  let doc = "Create a logical volume" in    
  let man = [
    `S "DESCRIPTION";
    `P "lvcreate creates a new logical volume in a volume group by allocating logical extents from the free physical extent pool of that volume group.  If there are not enough free physical extents then the volume group can be extended with other physical volumes or by reducing existing logical volumes of this volume group in size."
  ] in
  Term.(pure lvcreate $ uri_arg $ lv_name_arg $ real_size_arg $ percent_size_arg $ tags_arg $ vg_name_arg),
  Term.info "lvcreate" ~sdocs:"COMMON OPTIONS" ~doc ~man

  
