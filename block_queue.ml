open Lwt
open Sexplib.Std
open Block_ring_unix
open Log

module type CSTRUCTABLE = sig
  type t
  val to_cstruct: t -> Cstruct.t
  val of_cstruct: Cstruct.t -> t
end

module Pusher(Item: CSTRUCTABLE) = struct
  type t = {
    p: Producer.t;
  }

  let start filename =
    Producer.attach ~disk:filename ()
    >>= function
    | `Error msg ->
      info "Failed to attach to existing queue; creating a fresh one: %s" msg;
      ( Producer.create ~disk:filename ()
        >>= function
        | `Error msg ->
          error "Failed to create a fresh queue: %s" msg;
          fail (Failure msg)
        | `Ok () ->
          return ()
      ) >>= fun () ->
      ( Producer.attach ~disk:filename ()
        >>= function
        | `Error msg ->
          error "Failed to attach to a queue that I just created: %s" msg;
          fail (Failure msg)
        | `Ok p ->
          return { p }
      )
    | `Ok p ->
      return { p }

  let push t item =
    let item = Item.to_cstruct item in
    let rec push () =
      Producer.push ~t:t.p ~item ()
      >>= function
      | `Retry ->
         info "queue is temporarily full; sleeping 5s";
         Lwt_unix.sleep 5.
         >>= fun () ->
         push ()
      | `TooBig ->
         error "queue is too small to receive item of size %d bytes" (Cstruct.len item);
         fail (Failure "journal too small")
      | `Error msg ->
         error "Failed to write item queue: %s" msg;
         fail (Failure msg)
      | `Ok position ->
         return position in
    push ()

  let advance t position =
    Producer.advance ~t:t.p ~position ()
    >>= function
    | `Error msg ->
      error "Failed to advance queue producer pointer: %s" msg;
      fail (Failure msg)
    | `Ok () ->
      return () 
end

module Popper(Item: CSTRUCTABLE) = struct
  type t = {
    c: Consumer.t;
  }

  let start filename =
    Consumer.attach ~disk:filename ()
    >>= function
    | `Error msg ->
      info "Failed to attach to existing queue: %s" msg;
      fail (Failure msg)
    | `Ok c ->
      return { c }

  let rec pop t =
    Consumer.fold ~f:(fun buf acc ->
      let item = Item.of_cstruct buf in
      item :: acc
    ) ~t:t.c ~init:[] ()
    >>= function
    | `Error msg ->
      error "Failed to read from the queue: %s" msg;
      fail (Failure msg)
    | `Ok (position, rev_items) ->
      let items = List.rev rev_items in
      return (position, items)

  let advance t position =
    Consumer.advance ~t:t.c ~position ()
    >>= function
    | `Error msg ->
      error "Failed to advance queue consumer pointer: %s" msg;
      fail (Failure msg)
    | `Ok () ->
      return () 
end
