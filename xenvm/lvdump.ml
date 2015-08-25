(* LVM compatible bits and pieces *)

open Cmdliner
open Lwt
open Xenvm_common
open Errors

let lvdump copts (vg_name, lv_name_opt) physical_device : unit =
  let open Xenvm_common in
  let lv_name = match lv_name_opt with Some l -> l | None -> failwith "Need LV name" in
  Lwt_main.run (
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let local_device : string = match (info,physical_device) with
      | _, Some d -> d (* cmdline overrides default for the VG *)
      | Some info, None -> info.local_device (* If we've got a default, use that *)
      | None, None -> failwith "Need to know the local device!" in
    let module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock) in
    with_block local_device
      (fun x ->
        Vg_IO.connect [ x ] `RO >>|= fun vg ->
        match Vg_IO.find vg lv_name with
        | None -> failwith (Printf.sprintf "Failed to find LV %s" lv_name)
        | Some vol ->
        Vg_IO.Volume.connect vol
        >>= function
        | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" lv_name))
        | `Ok disk ->
          Vg_IO.Volume.get_info disk
          >>= fun info ->
          let buffer = Io_page.(to_cstruct (get 1024)) in
          let nsectors = Cstruct.len buffer / info.Vg_IO.Volume.sector_size in
          let rec loop = function
            | n when n = info.Vg_IO.Volume.size_sectors -> return ()
            | n ->
              let remaining = Int64.sub info.Vg_IO.Volume.size_sectors n in
              let toread = min (Int64.to_int remaining) nsectors in
              let buffer' = Cstruct.sub buffer 0 (toread * info.Vg_IO.Volume.sector_size) in
              Vg_IO.Volume.read disk n [ buffer' ]
              >>= function
              | `Ok () ->
                Lwt_io.write Lwt_io.stdout (Cstruct.to_string buffer')
                >>= fun () ->
                loop (Int64.(add n (of_int toread)))
              | _ -> failwith (Printf.sprintf "Failed to read sector %Ld" n) in
          loop 0L
      )
    )

let lvdump_cmd =
  let doc = "Dump the physical contents of a logical volume" in
  let man = [
    `S "DESCRIPTION";
    `P "lvdump allows you to dump the physical contents of a logical volume to stdout."
  ] in
  Term.(pure lvdump $ Xenvm_common.copts_t $ Xenvm_common.name_arg $ Xenvm_common.physical_device_arg),
  Term.info "lvdump" ~sdocs:"COMMON OPTIONS" ~doc ~man
