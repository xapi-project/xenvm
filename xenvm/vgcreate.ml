open Cmdliner
open Lwt
open Xenvm_common
open Errors

let vgcreate _ vg_name devices =
  let open Lvm in
  let t =
    let module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock) in
    let open Xenvm_interface in
    (* 4 MiB per volume *)
    let size = Int64.(mul 4L (mul 1024L 1024L)) in
    Lwt_list.map_s
      (fun filename ->
        Block.connect filename
        >>= function
        | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" filename))
        | `Ok x -> return x
      ) devices
    >>= fun blocks ->
    let pvs = List.mapi (fun i block ->
        let name = match Pv.Name.of_string (Printf.sprintf "pv%d" i) with
          | `Ok x -> x
          | `Error (`Msg x) -> failwith x in
        (name,block)
      ) blocks in
    Vg_IO.format vg_name ~magic:`Journalled pvs >>|= fun () ->
    Vg_IO.connect (List.map snd pvs) `RW
    >>|= fun vg ->
    (return (Vg.create (Vg_IO.metadata_of vg) _journal_name size))
    >>|= fun (_, op) ->
    Vg_IO.update vg [ op ]
    >>|= fun () ->
    let module Eraser = EraseBlock.Make(Vg_IO.Volume) in
    let open Lwt in
    ( match Vg_IO.find vg _journal_name with
    | None -> Lwt.return (`Error (`Msg "Failed to find the xenvmd journal LV to erase it"))
    | Some lv ->
      Vg_IO.Volume.connect lv
      >>= function
      | `Ok disk ->
         let open IO in
         Eraser.erase ~pattern:"Block erased because this is the xenvmd journal" disk
      | `Error _ -> Lwt.return (`Error (`Msg "Failed to open the xenvmd journal to erase it"))
    ) >>|= fun () ->
    return () in
  Lwt_main.run t


let vg_name_arg =
  let doc = "Specify the volume group in which to create the logical volume." in
  Arg.(required & pos 0 (some string) None & info [] ~docv:"VOLUMEGROUP" ~doc)

let devices_arg =
  let doc = "The block devices on which the VG will be created" in
  Arg.(non_empty & pos_right 0 file [] & info [] ~doc ~docv:"FILES")

let vgcreate_cmd =
  let doc = "Create a volume group" in
  let man = [
    `S "DESCRIPTION";
    `P "vgcreate creates a volume group on the specified physical block devices";
  ] in
  Term.(pure vgcreate $ Xenvm_common.copts_t $ vg_name_arg $ devices_arg),
  Term.info "vgcreate" ~sdocs:copts_sect ~doc ~man
