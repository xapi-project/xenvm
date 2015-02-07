open Lwt
open Log

let read_sector_size device =
  Block.connect device
  >>= function
  | `Ok x ->
    Block.get_info x
    >>= fun info ->
    Block.disconnect x
    >>= fun () ->
    return info.Block.sector_size
  | _ ->
    error "Failed to read sector size of %s" device;
    fail (Failure (Printf.sprintf "Failed to read sector size of %s" device))
