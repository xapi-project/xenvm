open Cohttp_lwt_unix
open Lwt
open Lvm

type copts_t = {
  host : string option;
  port : int option;
  config : string; 
}

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

let table_of_pv pv = add_prefix pv.Pv.name [
  [ "name"; pv.Pv.name; ];
  [ "id"; Uuid.to_string pv.Pv.id; ];
  [ "stored_device"; pv.Pv.stored_device ];
  [ "real_device"; pv.Pv.real_device ];
  [ "status"; String.concat ", " (List.map Pv.Status.to_string pv.Pv.status) ];
  [ "size_in_sectors"; Int64.to_string pv.Pv.size_in_sectors ];
  [ "pe_start"; Int64.to_string pv.Pv.pe_start ];
  [ "pe_count"; Int64.to_string pv.Pv.pe_count; ]
] @ (table_of_pv_header (pv.Pv.name ^ "/label") pv.Pv.label.Label.pv_header)

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



module Rpc = struct
  include Lwt

  let uri = ref ""

  (* Retry up to 5 times with 1s intervals *)
  let rpc call =
    let body = Jsonrpc.string_of_call call |> Cohttp_lwt_body.of_string in
    let rec retry attempts_remaining last_exn = match attempts_remaining, last_exn with
    | 0, Some e -> fail e
    | _, _ ->
      begin
        Lwt.catch
          (fun () ->
            Client.post (Uri.of_string !uri) ~body
            >>= fun x ->
            return (`Ok x))
          (function
           | Unix.Unix_error(Unix.ECONNREFUSED, _, _) as e ->
             return (`Retry e)
           | e ->
           return (`Error e))
        >>= function
        | `Ok (resp, body) ->
          let status = Response.status resp in
          Cohttp_lwt_body.to_string body >>= fun body ->
          return (Jsonrpc.response_of_string body) 
        | `Retry e ->
          Lwt_unix.sleep 1.
          >>= fun () ->
          retry (max 0 (attempts_remaining - 1)) (Some e)
        | `Error e ->
          fail e
      end in
    retry 5 None
end

module Client = Xenvm_interface.ClientM(Rpc)

let set_uri copts =
  match copts.host, copts.port with
  | Some h, Some p -> 
    Rpc.uri := Printf.sprintf "http://%s:%d/" h p;
    copts
  | _, _ -> 
    failwith "Unset host"

let lvs config =
  Lwt_main.run 
    (Client.get () >>= fun vg ->
     print_table [ "key"; "value" ] (table_of_vg vg);
     Lwt.return ())

let format config name filenames =
  Lwt_main.run 
    (
      let pvs = List.mapi (fun i filename -> (filename,Printf.sprintf "pv%d" i)) filenames in
      Client.format ~name ~pvs)

let vgopen config filenames =
  Lwt_main.run
    (Client.vgopen ~devices:filenames)

let create config name size =
  Lwt_main.run
    (let size_in_bytes = Int64.mul 1048576L size in
     Client.create name size_in_bytes)

let activate config lvname =
  Lwt_main.run
    (Client.activate ~name:lvname)

let start_journal config filename =
  Lwt_main.run
    (Client.start_journal filename)

let shutdown config =
  Lwt_main.run
    (Client.shutdown ())

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

let copts config host port = let copts = {host; port; config} in set_uri copts

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
  Arg.(required & pos 0 (some file) None & info [] ~docv:"FILENAMES" ~doc)

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
  Term.(pure activate $ copts_t $ lvname),
  Term.info "activate" ~sdocs:copts_sect ~doc ~man

let start_journal_cmd =
  let doc = "Start updating the metadata using a journal rather than synchronously" in
  let man = [
    `S "DESCRIPTION";
    `P "Starts the journal on the named device";
  ] in
  Term.(pure start_journal $ copts_t $ filename),
  Term.info "start_journal" ~sdocs:copts_sect ~doc ~man

let shutdown_cmd =
  let doc = "Shut the daemon down cleanly" in
  let man = [
    `S "DESCRIPTION";
    `P "Flushes the redo log and shuts down, leaving the system in a consistent state.";
  ] in
  Term.(pure shutdown $ copts_t),
  Term.info "shutdown" ~sdocs:copts_sect ~doc ~man

let default_cmd =
  let doc = "A fast, journalled LVM-compatible volume manager" in
  let man = [] in
  Term.(pure help $ copts_t), info


      
let cmds = [lvs_cmd; format_cmd; open_cmd; create_cmd; activate_cmd; start_journal_cmd; shutdown_cmd]

let () = match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1 | _ -> exit 0

