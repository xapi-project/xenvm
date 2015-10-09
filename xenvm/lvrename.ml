(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let retry fn =
  let rec inner n =
    match n with
    | 0 -> return (fn ())
    | n ->
      try
        return (fn ())
      with e ->
        Printf.fprintf stderr "Caught exception: %s. Retrying.\n%!" (Printexc.to_string e);
        Lwt_unix.sleep 1.0 >>=
        fun () ->
        inner (n-1)
  in
  inner 5

let lvrename copts (vg_name,lv_opt) newname physical_device =
  let module Devmapper = (val !Xenvm_common.dm : S.RETRYMAPPER) in
  let lv_name = match lv_opt with | Some l -> l | None -> failwith "Need an LV name" in
  (* It seems you can say "vg/lv" or "lv" *)
  let newname = match newname with
  | _, Some lv -> lv
  | lv, _ -> lv in
  let open Xenvm_common in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let local_device = match (info,physical_device) with
      | _, Some d -> d (* cmdline overrides default for the VG *)
      | Some info, None -> info.local_device (* If we've got a default, use that *)
      | None, None -> failwith "Need to know the local device!"
    in
    Client.get_lv ~name:lv_name >>= fun (vg, lv) ->                                                                              
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid VG name";
    Client.rename ~oldname:lv_name ~newname:newname
    >>= fun () ->
    (* Delete the old device node *)
    Lwt.catch (fun () -> Lwt_unix.unlink (Printf.sprintf "/dev/%s/%s" vg_name lv_name)) (fun _ -> Lwt.return ()) >>= fun () ->
    Devmapper.ls () >>= fun all ->
    let old_name = Mapper.name_of vg.Lvm.Vg.name lv.Lvm.Lv.name in
    if List.mem old_name all then begin
      Devmapper.remove old_name
      >>= fun () ->
      Mapper.read [ local_device ]
      >>= fun devices ->
      let targets = Mapper.to_targets devices vg lv in
      let new_name = Mapper.name_of vg.Lvm.Vg.name newname in
      Devmapper.create new_name targets >>= fun () -> 
      Devmapper.mknod new_name (Printf.sprintf "/dev/%s/%s" vg_name newname) 0x0600
    end else return ()
  )

let new_name_arg =
  let doc = "New name for the LV" in
  let n = Arg.(required & pos 1 (some string) None & info [] ~docv:"NEWNAME" ~doc) in
  Term.(pure Xenvm_common.parse_name $ n)

let lvrename_cmd =
  let doc = "Rename a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvrename renames an existing logical volume with a new name. The contents of the logical volume are unchanged."
  ] in
  Term.(pure lvrename $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ new_name_arg $ Xenvm_common.physical_device_arg),
  Term.info "lvrename" ~sdocs:"COMMON OPTIONS" ~doc ~man

  
