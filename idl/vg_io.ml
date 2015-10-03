open Lwt
open Errors
open Log

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end

module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock)

let sector_size, sector_size_u = Lwt.task ()
let myvg, myvg_u = Lwt.task ()
let lock = Lwt_mutex.create ()

let vgopen ~devices =
  Lwt_list.map_s
    (fun filename ->
       Printf.printf "filename: %s\n%!" filename;
       Block.connect filename >>= function
      | `Error _ -> fatal_error_t ("open " ^ filename)
      | `Ok x -> return x
    ) devices
  >>= fun devices' ->
  let module Label_IO = Lvm.Label.Make(Block) in
  Lwt_list.iter_s
    (fun (filename, device) ->
      Label_IO.read device >>= function
      | `Error (`Msg m) ->
        error "Failed to read PV label from device: %s" m;
        fail (Failure "Failed to read PV label from device")
      | `Ok label ->
        info "opened %s: %s" filename (Lvm.Label.to_string label);
        begin match Lvm.Label.Label_header.magic_of label.Lvm.Label.label_header with
        | Some `Lvm ->
          error "Device has normal LVM PV label. I will only open devices with the new PV label.";
          fail (Failure "Device has wrong LVM PV label")
        | Some `Journalled ->
          return ()
        | _ ->
          error "Device has an unrecognised LVM PV label. I will only open devices with the new PV label.";
          fail (Failure "Device has wrong PV label")
        end
    ) (List.combine devices devices')
  >>= fun () ->
  Vg_IO.connect ~flush_interval:5. devices' `RW >>|= fun vg ->
  Lwt.wakeup_later myvg_u vg;
  Device.read_sector_size devices
  >>= fun sector_size ->
  Lwt.wakeup_later sector_size_u sector_size;
  return ()

let read fn =
  Lwt_mutex.with_lock lock (fun () ->
    myvg >>= fun myvg ->
    fn (Vg_IO.metadata_of myvg)
  )

let write fn =
  Lwt_mutex.with_lock lock (fun () ->
    myvg >>= fun myvg ->
    fn (Vg_IO.metadata_of myvg)
    >>*= fun (_, op) ->
    Vg_IO.update myvg [ op ]
    >>|= fun () ->
    Lwt.return ()
  )

let maybe_write fn =
  Lwt_mutex.with_lock lock (fun () ->
      myvg >>= fun myvg ->
      fn (Vg_IO.metadata_of myvg)
      >>*= (function
      | Some ops ->
        Vg_IO.update myvg ops
      | None ->
        Lwt.return (`Ok ()))
      >>|= fun () ->
      Lwt.return ()
    )

let sync () =
  Lwt_mutex.with_lock lock (fun () ->
    myvg >>= fun myvg ->
    Vg_IO.sync myvg
    >>|= fun () ->
    Lwt.return ()
  )

