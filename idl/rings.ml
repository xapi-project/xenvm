open Errors
open Lwt

module Ring(Op:S.CSTRUCTABLE) = struct
  module R = Shared_block.Ring.Make(Log)(Vg_io.Volume)(Op)

  type item = Op.t

  type consumer = R.Consumer.t
  type cposition = R.Consumer.position

  type producer = R.Producer.t
  type pposition = R.Producer.position

  let prefix msg =
    Printf.sprintf "%s: %s" Op.name msg
  
  let create ~disk () =
    fatal_error (prefix "creating queue") (R.Producer.create ~disk ())
  let attach_as_consumer ~name ~disk () =
    fatal_error (prefix "attaching to queue") (R.Consumer.attach ~queue:(prefix "Consumer") ~client:"xenvmd" ~disk ())
  let attach_as_producer ~name ~disk () =
    let initial_state = ref `Running in
    let rec loop () = R.Producer.attach ~queue:(prefix "Producer") ~client:"xenvmd" ~disk () >>= function
      | `Error `Suspended ->
        Vg_io.Time.sleep 5.
        >>= fun () ->
        initial_state := `Suspended;
        loop ()
      | x -> fatal_error (prefix "attach") (return x) in
    loop ()
    >>= fun x ->
    return (!initial_state, x)
  let c_state t =
    fatal_error (prefix "querying state") (R.Consumer.state t)
  let p_state t = fatal_error (prefix "state") (R.Producer.state t)
  let c_debug_info t =
    fatal_error (prefix "querying debug_info") (R.Consumer.debug_info t)
  let p_debug_info t =
    fatal_error (prefix "querying debug_info") (R.Producer.debug_info t)
  let rec suspend t =
    retry_forever (fun () -> R.Consumer.suspend t)
    >>= fun r ->
    fatal_error (prefix "suspend") (suspended_is_ok r)
    >>= fun () ->
    wait_for (fun () -> R.Consumer.state t) `Suspended
    >>= function
    | `Ok _ -> Lwt.return ()
    |  _ -> fatal_error_t (prefix "suspend")
  let rec resume t =
    retry_forever (fun () -> R.Consumer.resume t)
    >>= fun r ->
    fatal_error (prefix "resume") (return r)
    >>= fun () ->
    wait_for (fun () -> R.Consumer.state t) `Running
    >>= function
    | `Ok _ -> Lwt.return ()
    |  _ -> fatal_error_t (prefix "resume")
  let rec push t item = R.Producer.push ~t ~item () >>= function
  | `Error (`Msg x) -> fatal_error_t (prefix (Printf.sprintf "Error pushing to the queue: %s" x))
  | `Error `Retry ->
    Vg_io.Time.sleep 5.
    >>= fun () ->
    push t item
  | `Error `Suspended ->
    Vg_io.Time.sleep 5.
    >>= fun () ->
    push t item
  | `Ok x -> return x
  let rec pop t =
    fatal_error (prefix "pop")
      (R.Consumer.fold ~f:(fun item acc -> item :: acc) ~t ~init:[] ())
    >>= fun (position, rev_items) ->
      let items = List.rev rev_items in
      return (position, items)
  let c_advance t position =
    fatal_error (prefix "advance") (R.Consumer.advance ~t ~position ())
  let p_advance t position =
    fatal_error (prefix "advance") (R.Producer.advance ~t ~position ())
end

module FromLVM = Ring(FreeAllocation)
module ToLVM = Ring(ExpandVolume)

