open Cohttp_lwt_unix
open Lwt
open Lvm
open Xenvm_common

let (>>|=) m f = m >>= function
  | `Error e -> fail (Failure e)
  | `Ok x -> f x

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
    List.iter (fun (n, s) -> Printf.printf "%s |" (padto ' ' n s)) (List.combine widths row);
    Printf.printf "\n" in
  print_row header;
  List.iter (fun (n, _) -> Printf.printf "%s-|" (padto '-' n "")) (List.combine widths header);
  Printf.printf "\n";
  List.iter print_row rows

let add_prefix x xs = List.map (function
  | [] -> []
  | y :: ys -> (x ^ "/" ^ y) :: ys
) xs

let table_of_pv_header prefix pvh = add_prefix prefix [
  [ "id"; Uuid.to_string pvh.Label.Pv_header.id; ];
  [ "device_size"; Int64.to_string pvh.Label.Pv_header.device_size; ];
  [ "extents"; string_of_int (List.length pvh.Label.Pv_header.extents) ];
  [ "metadata_areas"; string_of_int (List.length pvh.Label.Pv_header.metadata_areas) ];
]

let table_of_pv pv = add_prefix (Pv.Name.to_string pv.Pv.name) [
  [ "name"; Pv.Name.to_string pv.Pv.name; ];
  [ "id"; Uuid.to_string pv.Pv.id; ];
  [ "status"; String.concat ", " (List.map Pv.Status.to_string pv.Pv.status) ];
  [ "size_in_sectors"; Int64.to_string pv.Pv.size_in_sectors ];
  [ "pe_start"; Int64.to_string pv.Pv.pe_start ];
  [ "pe_count"; Int64.to_string pv.Pv.pe_count; ]
] @ (table_of_pv_header (Pv.Name.to_string pv.Pv.name ^ "/label") pv.Pv.label.Label.pv_header)

let table_of_lv lv = add_prefix lv.Lv.name [
  [ "name"; lv.Lv.name; ];
  [ "id"; Uuid.to_string lv.Lv.id; ];
  [ "tags"; String.concat ", " (List.map Tag.to_string lv.Lv.tags) ];
  [ "status"; String.concat ", " (List.map Lv.Status.to_string lv.Lv.status) ];
  [ "segments"; string_of_int (List.length lv.Lv.segments) ];
]

let table_of_vg vg =
  let pvs = List.flatten (List.map table_of_pv vg.Vg.pvs) in
  let lvs = List.flatten (List.map table_of_lv vg.Vg.lvs) in [
  [ "name"; vg.Vg.name ];
  [ "id"; Uuid.to_string vg.Vg.id ];
  [ "status"; String.concat ", " (List.map Vg.Status.to_string vg.Vg.status) ];
  [ "extent_size"; Int64.to_string vg.Vg.extent_size ];
  [ "max_lv"; string_of_int vg.Vg.max_lv ];
  [ "max_pv"; string_of_int vg.Vg.max_pv ];
] @ pvs @ lvs @ [
  [ "free_space"; Int64.to_string (Pv.Allocator.size vg.Vg.free_space) ];
]

let lvs config =
  Lwt_main.run 
    (Client.get () >>= fun vg ->
     print_table [ "key"; "value" ] (table_of_vg vg);
     Lwt.return ())

let format config name filenames =
    let t =
      let module Vg_IO = Vg.Make(Block) in
      Lwt_list.map_s
        (fun filename ->
          Block.connect filename
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" filename))
          | `Ok x -> return x
        ) filenames
      >>= fun blocks ->
      let pvs = List.mapi (fun i block ->
        let name = match Pv.Name.of_string (Printf.sprintf "pv%d" i) with
        | `Ok x -> x
        | `Error x -> failwith x in
        (name,block)
      ) blocks in
      Vg_IO.format name ~magic:`Journalled pvs >>|= fun () ->
      return () in
  Lwt_main.run t

let vgopen config filenames =
  Lwt_main.run
    (Client.vgopen ~devices:filenames)

let create config name size =
  Lwt_main.run
    (let size_in_bytes = Int64.mul 1048576L size in
     Client.create name size_in_bytes)

let activate config lvname path pv =
  Lwt_main.run
    (Client.get_lv ~name:lvname
     >>= fun (vg, lv) ->
     (* To compute the targets, I need to be able to map PV ids
        onto local block devices. *)
     Mapper.read [ pv ]
     >>= fun devices ->
     let targets = Mapper.to_targets devices vg lv in
     let name = Mapper.name_of vg lv in
     Devmapper.create name targets;
     Devmapper.mknod name path 0o0600;
     return ())

let set_redo_log config filename =
  Lwt_main.run (Client.set_redo_log filename)

let set_journal config filename =
  Lwt_main.run (Client.set_journal filename)

let register config host =
  Lwt_main.run (Client.register host)

let shutdown config =
  Lwt_main.run
    (Client.shutdown ())

let benchmark config =
  let t =
    let mib = Int64.mul 1048576L 4L in
    let n = 1000 in
    let start = Unix.gettimeofday () in
    let rec fori f = function
    | 0 -> return ()
    | n ->
      f n
      >>= fun () ->
      fori f (n - 1) in
    fori (fun i -> Client.create (Printf.sprintf "test-lv-%d" i) mib) n
    >>= fun () ->
    let time = Unix.gettimeofday () -. start in
    Printf.printf "%d creates in %.1f s\n" n time;
    return () in
  Lwt_main.run t

let help config =
  Printf.printf "help - %s %s %d\n" config.config (match config.host with Some s -> s | None -> "<unset>") (match config.port with Some d -> d | None -> -1)

  

open Cmdliner
let info =
  let doc =
    "XenVM LVM utility" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "xenvm" ~version:"0.1-alpha" ~doc ~man

let copts config host port = let copts = {Xenvm_common.host; port; config} in set_uri_from_copts copts

let config =
  let doc = "Path to the config file" in
  Arg.(value & opt file "xenvm.conf" & info [ "config" ] ~docv:"CONFIG" ~doc)

let port =
  let doc = "TCP port of xenvmd server" in
  Arg.(value & opt (some int) (Some 4000) & info [ "port" ] ~docv:"PORT" ~doc)

let host = 
  let doc = "Hostname of xenvmd server" in
  Arg.(value & opt (some string) (Some "127.0.0.1") & info [ "host" ] ~docv:"HOST" ~doc)

let filenames =
  let doc = "Path to the files" in
  Arg.(non_empty & pos_all file [] & info [] ~docv:"FILENAMES" ~doc)

let filename =
  let doc = "Path to the files" in
  Arg.(required & pos 0 (some file) None & info [] ~docv:"FILENAME" ~doc)

let vgname =
  let doc = "Name of the volume group" in
  Arg.(value & opt string "vg" & info ["vg"] ~docv:"VGNAME" ~doc)

let lvname =
  let doc = "Name of the logical volume" in
  Arg.(value & opt string "lv" & info ["lv"] ~docv:"LVNAME" ~doc)

let size =
  let doc = "Size of the LV in megs" in
  Arg.(value & opt int64 4L & info ["size"] ~docv:"SIZE" ~doc)


let copts_sect = "COMMON OPTIONS"

let copts_t =
  let docs = copts_sect in
  Term.(pure copts $ config $ host $ port)

let host_t =
  let hostname =
    let doc = "Name of the host" in
    Arg.(value & opt string "host" & info [ "name" ] ~docv:"NAME" ~doc) in
  let fromLVM =
    let doc = "Name of the queue containing updates from LVM (i.e. new free block allocations)" in
    Arg.(value & opt string "fromLVM" & info [ "from" ] ~docv:"FROM" ~doc) in
  let toLVM =
    let doc = "Name of the queue containing updates sent to LVM (i.e. new block allocations for a user LV)" in
    Arg.(value & opt string "toLVM" & info [ "to" ] ~docv:"TO" ~doc) in
  let freeLV =
    let doc = "Name of the LV containing the host's free blocks" in
    Arg.(value & opt string "free" & info [ "free" ] ~docv:"FREE" ~doc) in
  let make name fromLVM toLVM freeLV =
    let open Xenvm_interface in
    { name; fromLVM; toLVM; freeLV } in
  Term.(pure make $ hostname $ fromLVM $ toLVM $ freeLV)


let lvs_cmd =
  let doc = "List the logical volumes in the VG" in
  let man = [
    `S "DESCRIPTION";
    `P "Contacts the XenVM LVM daemon and retreives a list of
      all of the currently defined logical volumes";
  ] in
  Term.(pure lvs $ copts_t),
  Term.info "lvs" ~sdocs:copts_sect ~doc ~man

let format_cmd =
  let doc = "Format the specified file as a VG" in
  let man = [
    `S "DESCRIPTION";
    `P "Places volume group metadata into the file specified"
  ] in
  Term.(pure format $ copts_t $ vgname $ filenames),
  Term.info "format" ~sdocs:copts_sect ~doc ~man

let create_cmd =
  let doc = "Create a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "Creates a logical volume";
  ] in
  Term.(pure create $ copts_t $ lvname $ size),
  Term.info "create" ~sdocs:copts_sect ~doc ~man

let open_cmd =
  let doc = "Open the VG specified in the devices" in
  let man = [
    `S "DESCRIPTION";
    `P "Places volume group metadata into the file specified"
  ] in
  Term.(pure vgopen $ copts_t $ filenames),
  Term.info "open" ~sdocs:copts_sect ~doc ~man

let activate_cmd = 
  let doc = "Activate a logical volume on the host on which the daemon is running" in
  let man = [
    `S "DESCRIPTION";
    `P "Activates a logical volume";
  ] in
  let path =
    let doc = "Path to the new device node" in
    Arg.(required & pos 0 (some string) None & info [] ~docv:"PATH" ~doc) in
  let physical =
    let doc = "Path to the (single) physical PV" in
    Arg.(required & pos 1 (some string) None & info [] ~docv:"PV" ~doc) in
  Term.(pure activate $ copts_t $ lvname $ path $ physical),
  Term.info "activate" ~sdocs:copts_sect ~doc ~man

let set_redo_log_cmd =
  let doc = "Start updating the metadata using a redo log rather than synchronously" in
  let man = [
    `S "DESCRIPTION";
    `P "Starts the redo log on the named LV";
  ] in
  Term.(pure set_redo_log $ copts_t $ lvname),
  Term.info "set_redo_log" ~sdocs:copts_sect ~doc ~man

let set_journal_cmd =
  let doc = "Set where we journal our free block allocations" in
  let man = [
    `S "DESCRIPTION";
    `P "Maintains a journal of free block allocations on the named device. This must be done before a host is registered.";
  ] in
  Term.(pure set_journal $ copts_t $ lvname),
  Term.info "set_journal" ~sdocs:copts_sect ~doc ~man

let register_cmd =
  let doc = "Register a host with the daemon" in
  let man = [
    `S "DESCRIPTION";
    `P "Register a host with the daemon. The daemon will start servicing block updates from the shared queues.";
  ] in
  Term.(pure register $ copts_t $ host_t),
  Term.info "register" ~sdocs:copts_sect ~doc ~man

let shutdown_cmd =
  let doc = "Shut the daemon down cleanly" in
  let man = [
    `S "DESCRIPTION";
    `P "Flushes the redo log and shuts down, leaving the system in a consistent state.";
  ] in
  Term.(pure shutdown $ copts_t),
  Term.info "shutdown" ~sdocs:copts_sect ~doc ~man

let benchmark_cmd =
  let doc = "Perform some microbenchmarks" in
  let man = [
    `S "DESCRIPTION";
    `P "Perform some microbenchmarks and print the results.";
  ] in
  Term.(pure benchmark $ copts_t),
  Term.info "benchmark" ~sdocs:copts_sect ~doc ~man

let default_cmd =
  let doc = "A fast, journalled LVM-compatible volume manager" in
  let man = [] in
  Term.(pure help $ copts_t), info
      
let cmds = [
  lvs_cmd; format_cmd; open_cmd; create_cmd; activate_cmd;
  set_redo_log_cmd; set_journal_cmd; shutdown_cmd; register_cmd; benchmark_cmd;
  Lvmcompat.lvcreate_cmd
]

let () = match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1 | _ -> exit 0

