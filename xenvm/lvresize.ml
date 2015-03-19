(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let lvresize copts (vg_name,lv_opt) real_size percent_size =
  let lv_name = match lv_opt with | Some l -> l | None -> failwith "Need an LV name" in
  let open Xenvm_common in
  let size = match parse_size real_size percent_size with
  | `IncreaseBy x -> `IncreaseBy x
  | `Absolute x -> `Absolute x
  | `DecreaseBy _ -> failwith "Shrinking volumes not supported" in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.get_lv ~name:lv_name >>= fun (vg, lv) ->
    if vg.Lvm.Vg.name <> vg_name then failwith "Invalid VG name";

    let resize_remotely () = match size with
    | `Absolute size -> Client.resize lv_name size
    | `IncreaseBy delta -> Client.resize lv_name Int64.(add delta (mul (mul 512L vg.Lvm.Vg.extent_size) (Lvm.Lv.size_in_extents lv))) in

    let resize_locally allocator =
      let name = Mapper.name_of vg lv in
      let s = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
      Lwt_unix.connect s (Unix.ADDR_UNIX allocator)
      >>= fun () ->
      let oc = Lwt_io.of_fd ~mode:Lwt_io.output s in
      let r = { ResizeRequest.local_dm_name = name; action = size } in
      Lwt_io.write_line oc (Sexplib.Sexp.to_string (ResizeRequest.sexp_of_t r))
      >>= fun () ->
      Lwt_io.close oc in

    match info with
    | Some { Xenvm_common.local_allocator_path = Some allocator } ->
      let name = Mapper.name_of vg lv in
      let all = Devmapper.ls () in
      (* An active device with local allocator running must be resized locally *)
      if List.mem name all
      then resize_locally allocator
      else resize_remotely ()
    | _ ->
      (* safe to allocate remotely *)
      resize_remotely ()
  )

let lvresize_cmd =
  let doc = "Resize a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvresize will resize an existing logical volume.";
  ] in
  Term.(pure lvresize $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.real_size_arg $ Xenvm_common.percent_size_arg),
  Term.info "lvresize" ~sdocs:"COMMON OPTIONS" ~doc ~man

let lvextend_cmd =
  let doc = "Resize a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvextend will resize an existing logical volume.";
  ] in
  Term.(pure lvresize $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.real_size_arg $ Xenvm_common.percent_size_arg),
  Term.info "lvextend" ~sdocs:"COMMON OPTIONS" ~doc ~man
