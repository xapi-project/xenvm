open Lwt
open Sexplib.Std
open Block_ring_unix
open Log

module Config = struct
  type host = {
    to_lvm: string; (* where to read changes to be written to LVM from *)
    from_lvm: string; (* where to write changes from LVM to *)
    free: string; (* name of the free block LV *)
  } with sexp

  type t = {
    host_allocation_quantum: int64; (* amount of allocate each host at a time *)
    hosts: (string * host) list; (* host id -> rings *)
    master_journal: string; (* path to the SRmaster journal *)
  } with sexp
end

module ToLVM = Block_queue.Popper(BlockUpdate)

module Op = struct
  type t =
    | Print of string
    | BatchOfAllocations of BlockUpdate.t list
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
  let config = Config.t_of_sexp (Sexplib.Sexp.load_sexp config) in
  debug "Loaded configuration: %s" (Sexplib.Sexp.to_string_hum (Config.sexp_of_t config));
  let t =
    let to_LVMs = List.map (fun (_, { Config.to_lvm }) ->
      ToLVM.start to_lvm
    ) config.Config.hosts in

    let perform t =
      let open Op in
      sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
      match t with
      | Print _ -> return ()
      | BatchOfAllocations _ -> return () in

    let module J = Shared_block.Journal.Make(Block_ring_unix.Producer)(Block_ring_unix.Consumer)(Op) in
    J.start config.Config.master_journal perform
    >>= fun j ->

    let rec main_loop () =
      (* 1. Do any of the host free LVs need topping up? *)

      (* 2. Are there any pending LVM updates from hosts? *)
      Lwt_list.map_p
        (fun t ->
          t >>= fun to_lvm ->
          ToLVM.pop to_lvm
          >>= fun (pos, item) ->
          return (to_lvm, pos, item)
        ) to_LVMs
      >>= fun work ->
      let items = List.concat (List.map (fun (_, _, bu) -> bu) work) in
      if items = [] then begin
        debug "sleeping for 5s";
        Lwt_unix.sleep 5.
        >>= fun () ->
        main_loop ()
      end else begin
        J.push j (Op.BatchOfAllocations (List.concat (List.map (fun (_, _, bu) -> bu) work)))
        >>= fun () -> 
        (* write the work to a journal *)
        Lwt_list.iter_p
          (fun (t, pos, _) ->
            ToLVM.advance t pos
          ) work
        >>= fun () ->
        main_loop ()
      end in
    main_loop () in
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
  Arg.(value & opt file "remoteConfig" & info [ "config" ] ~docv:"CONFIG" ~doc)

let () =
  let t = Term.(pure main $ socket $ config) in
  match Term.eval (t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0
