open Lwt
open Sexplib.Std
open Block_ring_unix
open Log

module ToLVM = struct
  type t = {
    p: Producer.t;
  }

  let start filename =
    Producer.attach ~disk:filename ()
    >>= function
    | `Error msg ->
      info "Failed to attach to existing ToLVM queue; creating a fresh one: %s" msg;
      ( Producer.create ~disk:filename ()
        >>= function
        | `Error msg ->
          error "Failed to create a fresh ToLVM queue: %s" msg;
          fail (Failure msg)
        | `Ok () ->
          return ()
      ) >>= fun () ->
      ( Producer.attach ~disk:filename ()
        >>= function
        | `Error msg ->
          error "Failed to attach to a ToLVM queue that I just created: %s" msg;
          fail (Failure msg)
        | `Ok p ->
          return { p }
      )
    | `Ok p ->
      return { p }

  let rec push t bu =
    let item = BlockUpdate.to_cstruct bu in
    Producer.push ~t:t.p ~item ()
    >>= function
    | `Retry ->
       info "journal is full; sleeping 5s";
       Lwt_unix.sleep 5.
       >>= fun () ->
       push t bu
    | `TooBig ->
       error "journal is too small to receive item of size %d bytes" (Cstruct.len item);
       fail (Failure "journal too small")
    | `Error msg ->
       error "Failed to write item toLVM queue: %s" msg;
       fail (Failure msg)
    | `Ok position ->
       return position

  let advance t position =
    Producer.advance ~t:t.p ~position ()
    >>= function
    | `Error msg ->
      error "Failed to advance toLVM producer pointer: %s" msg;
      fail (Failure msg)
    | `Ok () ->
      return () 
end

module Op = struct
  type t =
    | Print of string
    | LocalAllocation of BlockUpdate.t
  with sexp

  let of_cstruct x =
    Cstruct.to_string x |> Sexplib.Sexp.of_string |> t_of_sexp
  let to_cstruct t =
    let s = sexp_of_t t |> Sexplib.Sexp.to_string in
    let c = Cstruct.create (String.length s) in
    Cstruct.blit_from_string s 0 c 0 (Cstruct.len c);
    c
end

let main socket config =
  let t =
    fail (Failure "unimplemented") in
  try
    `Ok (Lwt_main.run t)
  with Failure msg ->
    error "%s" msg;
    `Error(false, msg)

open Cmdliner
let info =
  let doc =
    "Remote block allocator" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "local-allocator" ~version:"0.1-alpha" ~doc ~man

let socket =
  let doc = "Path of Unix domain socket to listen on" in
  Arg.(value & opt (some string) None & info [ "socket" ] ~docv:"SOCKET" ~doc)

let config =
  let doc = "Path to the config file" in
  Arg.(value & opt file "config" & info [ "config" ] ~docv:"CONFIG" ~doc)

let () =
  let t = Term.(pure main $ socket $ config) in
  match Term.eval (t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0
