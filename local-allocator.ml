open Lwt
open Block_ring_unix

let debug fmt = Printf.ksprintf (fun s -> print_endline s) fmt
let info  fmt = Printf.ksprintf (fun s -> print_endline s) fmt
let error fmt = Printf.ksprintf (fun s -> print_endline s) fmt

module Journal = struct
  let replay filename =
    Consumer.attach ~disk:filename ()
    >>= function
    | `Error msg ->
      info "There is no journal on %s: no need to replay" filename;
      return ()
    | `Ok c ->
      ( Consumer.fold ~f:(fun x y -> x :: y) ~t:c ~init:[] ()
        >>= function
        | `Error msg ->
          error "Error replaying the journal, cannot continue: %s" msg;
          Consumer.detach c
          >>= fun () ->
          fail (Failure msg)
        | `Ok (pos, items) ->
          info "There are %d items in the journal to replay" (List.length items);
          (* -- replay them here *)
          Consumer.detach c
      )
end

let main socket journal freePool fromLVM toLVM =
  `Error(false, "not implemented")

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
  Arg.(value & opt (some string) None & info [ "journal" ] ~docv:"JOURNAL" ~doc)

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
