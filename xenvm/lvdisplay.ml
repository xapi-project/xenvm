(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt

let print_verbose vg lv =
  let read_write =
    let all =
      (if List.mem Lvm.Lv.Status.Read lv.Lvm.Lv.status
       then [ "read" ] else []) @
      (if List.mem Lvm.Lv.Status.Write lv.Lvm.Lv.status
       then [ "write" ] else []) in
    String.concat "/" all in
  let size = Int64.mul vg.Lvm.Vg.extent_size (Lvm.Lv.size_in_extents lv) in
  let creation_time =
    let open Unix in
    let tm = gmtime (Int64.to_float lv.Lvm.Lv.creation_time) in
    Printf.sprintf "%d-%02d-%02d %02d:%02d:%02d +0000"
      (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday
      tm.tm_hour tm.tm_min tm.tm_sec in

  let device =
    let module Devmapper = (val !dm: S.RETRYMAPPER) in
    let name = Mapper.name_of vg.Lvm.Vg.name lv.Lvm.Lv.name in
    Devmapper.stat name >>= fun s ->
    match s with
    | Some info ->
      Lwt.return (Some (Printf.sprintf "%ld:%ld" info.Devmapper.major info.Devmapper.minor))
    | None ->
      Lwt.return (None) in
  device >>= fun device ->

  let lines = [
    "--- Logical volume ---";
    Printf.sprintf "LV Path                /dev/%s/%s" vg.Lvm.Vg.name lv.Lvm.Lv.name;
    Printf.sprintf "LV Name                %s" lv.Lvm.Lv.name;
    Printf.sprintf "VG Name                %s" vg.Lvm.Vg.name;
    Printf.sprintf "LV UUID                %s" (Lvm.Uuid.to_string lv.Lvm.Lv.id);
    Printf.sprintf "LV Write Access        %s" read_write;
    Printf.sprintf "LV Creation host, time %s, %s" lv.Lvm.Lv.creation_host creation_time;
    Printf.sprintf "LV Status              %s" (if List.mem Lvm.Lv.Status.Visible lv.Lvm.Lv.status then "available" else "");
    Printf.sprintf "# open                 uknown";
    Printf.sprintf "LV Size                %Lds" size;
    Printf.sprintf "Current LE             %Ld" (Lvm.Lv.size_in_extents lv);
    Printf.sprintf "Segments               %d" (List.length lv.Lvm.Lv.segments);
    Printf.sprintf "Allocation             inherit";
    Printf.sprintf "Read ahead sectors     auto";
    (*
    - currently set to     256
    *)
  ] @ (match device with
       | Some device -> [ Printf.sprintf "Block device           %s" device ]
       | None -> []) @ [
    "";
  ] in
  Lwt_list.iter_s (fun line -> stdout "  %s" line) lines

(* Example output:
  /dev/packer-virtualbox-iso-vg/root:packer-virtualbox-iso-vg:3:1:-1:1:132661248:16194:-1:0:-1:252:0
*)
let print_colon vg lv =
  let sectors = Int64.mul vg.Lvm.Vg.extent_size (Lvm.Lv.size_in_extents lv) in
  let majorminor =
    let module Devmapper = (val !dm: S.RETRYMAPPER) in
    let name = Mapper.name_of vg.Lvm.Vg.name lv.Lvm.Lv.name in
    Devmapper.stat name >>= fun s ->
    match s with
    | Some info ->
      Lwt.return (Int32.to_string info.Devmapper.major, Int32.to_string info.Devmapper.minor)
    | None ->
      Lwt.return ("-1", "-1") in
  majorminor >>= fun (major,minor) ->
  let parts = [
    Printf.sprintf "/dev/%s/%s" vg.Lvm.Vg.name lv.Lvm.Lv.name;
    vg.Lvm.Vg.name;
    "?"; (* access *)
    "?"; (* volume status *)
    "?"; (* internal logical volume number *)
    "?"; (* open count *)
    Int64.to_string sectors; (* size in sectors *)
    "?"; (* current size in extents *)
    "?"; (* allocated extents *)
    "?"; (* allocation policy *)
    "?"; (* read ahead sectors *)
    major;
    minor;
  ] in
  stdout "  %s" (String.concat ":" parts)

let lvdisplay copts colon (vg_name,lv_display_opt) =
  let open Xenvm_common in

  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get () >>= fun vg ->
    let print = if colon then print_colon else print_verbose in
    let to_print =
      Lvm.Vg.LVs.bindings vg.Lvm.Vg.lvs
      |> List.map snd (* only interested in the LV, not the id *)
      |> List.filter (function { Lvm.Lv.name } -> match lv_display_opt with
        | None -> true
        | Some lv' when lv' = name -> true
        | _ -> false
        ) in
    Lwt_list.iter_s (print vg) to_print
    >>= fun () ->
    if to_print = [] then failwith "Failed to find any matching logical volumes";
    Lwt.return () in
  Lwt_main.run t

let lvdisplay_cmd =
  let doc = "report information about logical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "lvdisplay produces formatted output about logical volumes";
  ] in
  let colon_arg =
    let doc = "Print in terse colon-separated form" in
    Arg.(value & flag & info ["colon";"c"] ~doc) in
  Term.(pure lvdisplay $ Xenvm_common.copts_t $ colon_arg $ Xenvm_common.name_arg),
  Term.info "lvdisplay" ~sdocs:"COMMON OPTIONS" ~doc ~man
