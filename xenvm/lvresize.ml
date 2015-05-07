(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt

let lvresize copts live (vg_name,lv_opt) real_size percent_size =
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
    let local_device = match info with
    | Some info -> info.local_device (* If we've got a default, use that *)
    | None -> failwith "Need to know the local device!" in

    let existing_size = Int64.(mul (mul 512L vg.Lvm.Vg.extent_size) (Lvm.Lv.size_in_extents lv)) in

    let device_is_active =
      let name = Mapper.name_of vg lv in
      let all = Devmapper.ls () in
      List.mem name all in

    let resize_remotely () =
      ( if device_is_active
        then Lvchange.deactivate vg lv
        else return () )
      >>= fun () ->
      ( match size with
        | `Absolute size -> Client.resize lv_name size
        | `IncreaseBy delta -> Client.resize lv_name Int64.(add delta (mul (mul 512L vg.Lvm.Vg.extent_size) (Lvm.Lv.size_in_extents lv))) )
      >>= fun () ->
      ( if device_is_active then begin
          Client.get_lv ~name:lv_name >>= fun (vg, lv) ->
          Lvchange.activate vg lv local_device
        end else return () ) in

    let resize_locally allocator =
      let name = Mapper.name_of vg lv in
      let s = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
      Lwt_unix.connect s (Unix.ADDR_UNIX allocator)
      >>= fun () ->
      let oc = Lwt_io.of_fd ~mode:Lwt_io.output s in
      let r = { ResizeRequest.local_dm_name = name; action = size } in
      Lwt_io.write_line oc (Sexplib.Sexp.to_string (ResizeRequest.sexp_of_t r))
      >>= fun () ->
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input ~close:return s in
      Lwt_io.read_line ic
      >>= fun txt ->
      let resp = ResizeResponse.t_of_sexp (Sexplib.Sexp.of_string txt) in
      Lwt_io.close oc
      >>= fun () ->
      match resp with
      | ResizeResponse.Device_mapper_device_does_not_exist name ->
        Printf.fprintf stderr "Device mapper device does not exist: %s\n%!" name;
        exit 1
      | ResizeResponse.Request_for_no_segments nr ->
        Printf.fprintf stderr "Request for an illegal number of segments: %Ld\n%!" nr;
        exit 2
      | ResizeResponse.Success ->
        return () in
    match live, info with
    | true, Some { Xenvm_common.local_allocator_path = Some allocator } ->
      if device_is_active then begin
        match size with
        | `Absolute size ->
          (* The local allocator can only allocate. When in this state we cannot shrink:
             deactivate the device first. *)
          if size < existing_size
          then failwith (Printf.sprintf "Existing size is %Ld: cannot decrease to %Ld" existing_size size);
          if size = existing_size
          then return ()
          else resize_locally allocator
        | _ -> resize_locally allocator
      end else resize_remotely ()
    | _, _ ->
      (* safe to allocate remotely *)
      resize_remotely ()
  )
let live_arg =
  let doc = "Resize a live device using the local allocator" in
  Arg.(value & flag & info ["live"] ~doc)

let lvresize_cmd =
  let doc = "Resize a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvresize will resize an existing logical volume.";
  ] in
  Term.(pure lvresize $ Xenvm_common.copts_t $ live_arg $ Xenvm_common.name_arg $ Xenvm_common.real_size_arg $ Xenvm_common.percent_size_arg),
  Term.info "lvresize" ~sdocs:"COMMON OPTIONS" ~doc ~man

let lvextend_cmd =
  let doc = "Resize a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvextend will resize an existing logical volume.";
  ] in
  Term.(pure lvresize $ Xenvm_common.copts_t $ live_arg $ Xenvm_common.name_arg $ Xenvm_common.real_size_arg $ Xenvm_common.percent_size_arg),
  Term.info "lvextend" ~sdocs:"COMMON OPTIONS" ~doc ~man
