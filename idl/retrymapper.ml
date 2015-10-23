(* Like devmapper, but retries in the face of exceptions *)

open Lwt
open Log

let retry ~dbg ?(retries=3) ?(interval=1.) f =
  let rec aux n =
    if n <= 0 then Lwt.return (f ())
    else
      try Lwt.return (f ())
      with exn ->
        warn "warning: 'RetryMapper.%s' failed with '%s'; will retry %d more time%s..."
          dbg (Printexc.to_string exn) n (if n = 1 then "" else "s")
        >>= fun () ->
        Lwt_unix.sleep interval
        >>= fun () ->
        aux (n - 1) in
  aux retries

module Make(DM : Devmapper.S.DEVMAPPER) = struct
  include DM

  let create device targets =
    retry ~dbg:"create" (fun () -> DM.create device targets)

  let remove device =
    retry ~dbg:"remove" (fun () -> DM.remove device)

  let reload device targets =
    retry ~dbg:"reload" (fun () -> DM.reload device targets)

  let suspend device =
    retry ~dbg:"suspend" (fun () -> DM.suspend device)

  let resume device =
    retry ~dbg:"resume" (fun () -> DM.resume device)

  let mknod device path mode =
    retry ~dbg:"mknod" (fun () -> DM.mknod device path mode)
  
  let stat device =
    retry ~dbg:"stat" (fun () -> DM.stat device)

  let ls () =
    retry ~dbg:"ls" (fun () -> DM.ls ())
end
