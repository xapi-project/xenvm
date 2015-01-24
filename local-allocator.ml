open Lwt
open Sexplib.Std
open Block_ring_unix

let debug fmt = Printf.ksprintf (fun s -> print_endline s) fmt
let info  fmt = Printf.ksprintf (fun s -> print_endline s) fmt
let error fmt = Printf.ksprintf (fun s -> print_endline s) fmt

module Op = struct
  type t =
    | Print of string
  with sexp

  let perform t =
    sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
    return ()
  let of_cstruct x =
    Cstruct.to_string x |> Sexplib.Sexp.of_string |> t_of_sexp
  let to_cstruct t =
    let s = sexp_of_t t |> Sexplib.Sexp.to_string in
    let c = Cstruct.create (String.length s) in
    Cstruct.blit_from_string s 0 c 0 (Cstruct.len c);
    c
end

module Journal = struct

  type t = {
    p: Producer.t;
    c: Consumer.t;
    filename: string;
    cvar: unit Lwt_condition.t;
    mutable please_shutdown: bool;
    mutable shutdown_complete: bool;
  }
  let replay t =
    Consumer.fold ~f:(fun x y -> x :: y) ~t:t.c ~init:[] ()
    >>= function
    | `Error msg ->
       error "Error replaying the journal, cannot continue: %s" msg;
       Consumer.detach t.c
       >>= fun () ->
       fail (Failure msg)
    | `Ok (position, items) ->
       info "There are %d items in the journal to replay" (List.length items);
       Lwt_list.iter_p
         (fun item ->
           Op.(perform (of_cstruct item))
         ) items
       >>= fun () ->
       ( Consumer.advance ~t:t.c ~position ()
         >>= function
         | `Error msg ->
           error "In replay, failed to advance consumer: %s" msg;
           fail (Failure msg)
         | `Ok () ->
           return () )
  let start filename =
    ( Consumer.attach ~disk:filename ()
      >>= function
      | `Error msg ->
        info "There is no journal on %s: no need to replay" filename;
        ( Producer.create ~disk:filename ()
          >>= function
          | `Error msg ->
            error "Failed to create empty journal on %s" filename;
            fail (Failure msg)
          | `Ok () ->
            ( Consumer.attach ~disk:filename ()
              >>= function
              | `Error msg ->
                error "Creating an empty journal failed on %s: %s" filename msg;
                fail (Failure msg)
              | `Ok c ->
                return c )
        )
      | `Ok x ->
        return x
    ) >>= fun c ->
    ( Producer.attach ~disk:filename ()
      >>= function
      | `Error msg ->
        error "Failed to open journal on %s: %s" filename msg;
        fail (Failure msg)
      | `Ok p ->
        return p
    ) >>= fun p ->
    let please_shutdown = false in
    let shutdown_complete = false in
    let cvar = Lwt_condition.create () in
    let t = { p; c; filename; please_shutdown; shutdown_complete; cvar } in
    replay t
    >>= fun () ->
    (* Run a background thread processing items from the journal *)
    let (_: unit Lwt.t) =
      let rec forever () =
        Lwt_condition.wait t.cvar
        >>= fun () ->
        if t.please_shutdown then begin
          t.shutdown_complete <- true;
          Lwt_condition.broadcast t.cvar ();
          return ()
        end else begin
          replay t
          >>= fun () ->
          forever ()
        end in
      forever () in
    return t

  let shutdown t =
    t.please_shutdown <- true;
    let rec loop () =
      if t.shutdown_complete
      then return ()
      else
        Lwt_condition.wait t.cvar
        >>= fun () ->
        loop () in
    loop ()
    >>= fun () ->
    Consumer.detach t.c 

  let rec push t op =
    if t.please_shutdown
    then fail (Failure "journal shutdown in progress")
    else begin
      let item = Op.to_cstruct op in
      Producer.push ~t:t.p ~item ()
      >>= function
      | `Retry ->
         info "journal is full; sleeping 5s";
         Lwt_unix.sleep 5.
         >>= fun () ->
         push t op
      | `TooBig ->
         error "journal is too small to receive item of size %d bytes" (Cstruct.len item);
         fail (Failure "journal too small")
      | `Error msg ->
         error "Failed to write item to journal: %s" msg;
         fail (Failure msg)
      | `Ok position ->
         ( Producer.advance ~t:t.p ~position ()
           >>= function
           | `Error msg ->
             error "Failed to advance producer pointer: %s" msg;
             fail (Failure msg)
           | `Ok () ->
             Lwt_condition.broadcast t.cvar ();
             return () )
    end
end

let main socket journal freePool fromLVM toLVM =
  let t =
    Journal.start journal
    >>= fun j ->
    let rec loop () =
      Lwt_io.read_line Lwt_io.stdin
      >>= fun line ->
      Journal.push j (Op.Print line)
      >>= fun () ->
      loop () in
    loop () in
  try
    `Ok (Lwt_main.run t)
  with Failure msg ->
    error "%s" msg;
    `Error(false, msg)

open Cmdliner
let info =
  let doc =
    "Local block allocator" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "local-allocator" ~version:"0.1-alpha" ~doc ~man

let socket =
  let doc = "Path of Unix domain socket to listen on" in
  Arg.(value & opt (some string) None & info [ "socket" ] ~docv:"SOCKET" ~doc)

let journal =
  let doc = "Path of the host local journal" in
  Arg.(value & opt file "journal" & info [ "journal" ] ~docv:"JOURNAL" ~doc)

let freePool =
  let doc = "Path to the device mapper device containing the free blocks" in
  Arg.(value & opt (some string) None & info [ "freePool" ] ~docv:"FREEPOOL" ~doc)

let toLVM =
  let doc = "Path to the device or file to contain the pending LVM metadata updates" in
  Arg.(value & opt (some string) None & info [ "fromLVM" ] ~docv:"TOLVM" ~doc)

let fromLVM =
  let doc = "Path to the device or file which contains new free blocks from LVM" in
  Arg.(value & opt (some string) None & info [ "freeLVM" ] ~docv:"FROMLVM" ~doc)

let () =
  let t = Term.(pure main $ socket $ journal $ freePool $ fromLVM $ toLVM) in
  match Term.eval (t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0
