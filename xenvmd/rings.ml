open Errors
open Lwt
open Vg_io

module ToLVM = struct
  module R = Shared_block.Ring.Make(Log)(Vg_IO.Volume)(ExpandVolume)
  type consumer = R.Consumer.t
  type item = ExpandVolume.t
  type cposition = R.Consumer.position

  let create ~disk () =
    fatal_error "creating ToLVM queue" (R.Producer.create ~disk ())
  let attach_as_consumer ~name ~disk () =
    fatal_error "attaching to ToLVM queue" (R.Consumer.attach ~queue:(name ^ " ToLVM Consumer") ~client:"xenvmd" ~disk ())
  let c_state t =
    fatal_error "querying ToLVM state" (R.Consumer.state t)
  let c_debug_info t =
    fatal_error "querying ToLVM debug_info" (R.Consumer.debug_info t)
  let rec suspend t =
    retry_forever (fun () -> R.Consumer.suspend t)
    >>= fun r ->
    fatal_error "ToLVM.suspend" (suspended_is_ok r)
    >>= fun () ->
    wait_for (fun () -> R.Consumer.state t) `Suspended
    >>= function
    | `Ok _ -> Lwt.return ()
    |  _ -> fatal_error_t "ToLVM.suspend"
  let rec resume t =
    retry_forever (fun () -> R.Consumer.resume t)
    >>= fun r ->
    fatal_error "ToLVM.resume" (return r)
    >>= fun () ->
    wait_for (fun () -> R.Consumer.state t) `Running
    >>= function
    | `Ok _ -> Lwt.return ()
    |  _ -> fatal_error_t "ToLVM.resume"
  let rec pop t =
    fatal_error "ToLVM.pop"
      (R.Consumer.fold ~f:(fun item acc -> item :: acc) ~t ~init:[] ())
    >>= fun (position, rev_items) ->
      let items = List.rev rev_items in
      return (position, items)
  let c_advance t position =
    fatal_error "toLVM.advance" (R.Consumer.advance ~t ~position ())
end

module FromLVM = struct
  module R = Shared_block.Ring.Make(Log)(Vg_IO.Volume)(FreeAllocation)
  type producer = R.Producer.t
  type item = FreeAllocation.t
  type pposition = R.Producer.position
  let create ~disk () =
    fatal_error "FromLVM.create" (R.Producer.create ~disk ())
  let attach_as_producer ~name ~disk () =
    let initial_state = ref `Running in
    let rec loop () = R.Producer.attach ~queue:(name ^ " FromLVM Producer") ~client:"xenvmd" ~disk () >>= function
      | `Error `Suspended ->
        Time.sleep 5.
        >>= fun () ->
        initial_state := `Suspended;
        loop ()
      | x -> fatal_error "FromLVM.attach" (return x) in
    loop ()
    >>= fun x ->
    return (!initial_state, x)
  let p_state t = fatal_error "FromLVM.state" (R.Producer.state t)
  let p_debug_info t =
    fatal_error "querying FromLVM debug_info" (R.Producer.debug_info t)
  let rec push t item = R.Producer.push ~t ~item () >>= function
  | `Error (`Msg x) -> fatal_error_t (Printf.sprintf "Error pushing to the FromLVM queue: %s" x)
  | `Error `Retry ->
    Time.sleep 5.
    >>= fun () ->
    push t item
  | `Error `Suspended ->
    Time.sleep 5.
    >>= fun () ->
    push t item
  | `Ok x -> return x
  let p_advance t position =
    fatal_error "FromLVM.advance" (R.Producer.advance ~t ~position ())
end

