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

exception Retry (* out of space *)

(* Return the physical (location, length) pairs which we're going
   to use to extend the volume with *)
let first_exn volume size =
  let open Devmapper in
  let rec loop targets size = match targets, size with
  | _, 0L -> []
  | [], _ -> raise Retry
  | t :: ts, n ->
    let available = min t.Target.size n in
    let still_needed = Int64.sub n available in
    let location = match t.Target.kind with
    | Target.Linear l -> l
    | Target.Unknown _ -> failwith "unknown is not implemented"
    | Target.Striped _ -> failwith "striping is not implemented" in
    (location, available) :: loop ts still_needed in
  loop volume.targets size

(* Return the virtual size of the volume *)
let sizeof volume =
  let open Devmapper in
  let ends = List.map (fun t -> Int64.add t.Target.start t.Target.size) volume.targets in
  List.fold_left max 0L ends

(* Return the targets you would need to extend the volume with the given extents *)
let new_targets volume extents =
  let open Devmapper in
  let size = sizeof volume in
  List.map
    (fun (location, available) ->
      let sectors = Int64.div available 512L in
      { Target.start = size; size = sectors; kind = Target.Linear location }
    ) extents

let main socket journal freePool fromLVM toLVM =
  let t =
    ToLVM.start toLVM
    >>= fun tolvm ->
    let perform t =
      let open Op in
      sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
      match t with
      | Print _ -> return ()
      | LocalAllocation b ->
        ToLVM.push tolvm b
        >>= fun position ->
        print_endline "Suspend local dm devices";
        print_endline "Move target from one to the other (make idempotent)";
        ToLVM.advance tolvm position
        >>= fun () ->
        print_endline "Resume local dm devices";
        return () in

    let module J = Journal.Make(Op) in
    J.start journal perform
    >>= fun j ->

    let rec loop () =
      Lwt_io.read_line Lwt_io.stdin
      >>= fun line ->
      ( match Devmapper.stat line with
      | None ->
        J.push j (Op.Print ("Couldn't find device mapper device: " ^ line))
      | Some data_volume ->
        ( match Devmapper.stat "free" with
          | None ->
            failwith "Couldn't find free volume"
          | Some free_volume ->
            (* choose the blocks we're going to transfer *)
            let rec loop () =
              try
                let physical_blocks = first_exn free_volume 1048578L in
                (* append these onto the data_volume *)
                let new_targets = new_targets data_volume physical_blocks in
                return (BlockUpdate.({ fromLV = "free"; toLV = line; targets = new_targets }))
              with Retry ->
                Lwt_unix.sleep 5.
                >>= fun () ->
                loop () in
            loop ()
            >>= fun bu ->
            J.push j (Op.LocalAllocation bu)
        )
      ) >>= fun () ->
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
  Arg.(value & opt file "toLVM" & info [ "toLVM" ] ~docv:"TOLVM" ~doc)

let fromLVM =
  let doc = "Path to the device or file which contains new free blocks from LVM" in
  Arg.(value & opt file "fromLVM" & info [ "fromLVM" ] ~docv:"FROMLVM" ~doc)

let () =
  let t = Term.(pure main $ socket $ journal $ freePool $ fromLVM $ toLVM) in
  match Term.eval (t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0
