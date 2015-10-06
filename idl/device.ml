open Lwt
open Log

let read_sector_size devices =
  Lwt_list.map_s
    (fun device ->
      Block.connect device
      >>= function
      | `Ok x ->
        Block.get_info x
        >>= fun info ->
        Block.disconnect x
        >>= fun () ->
        return info.Block.sector_size
      | _ ->
        error "Failed to read sector size of %s" device
        >>= fun () ->
        fail (Failure (Printf.sprintf "Failed to read sector size of %s" device))
    ) devices
  >>= function
  | [] ->
    error "I require at least one device"
    >>= fun () ->
    fail (Invalid_argument "You must configure at least one block device for the VG")
  | (x :: _) as sizes ->
    (* They need to be the same size *)
    let biggest = List.fold_left max x sizes in
    let smallest = List.fold_left min x sizes in
    if biggest <> smallest then begin
      error "Not all devices have the same sector size:"
      >>= fun () ->
      Lwt_list.iter_s (fun (device, size) ->
          error "Device %s has sector size %d" device size
        ) (List.combine devices sizes)
      >>= fun () ->
      fail (Failure "The sector size on each block device must be the same")
    end else return biggest
