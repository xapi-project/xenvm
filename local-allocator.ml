open Lwt
open Sexplib.Std
open Block_ring_unix
open Log

module Config = struct
  type t = {
    socket: string; (* listen on this socket *)
    localJournal: string; (* path to the host local journal *)
    freePool: string; (* name of the device mapper device with free blocks *)
    toLVM: string; (* pending updates for LVM *)
    fromLVM: string; (* received updates from LVM *)
  } with sexp
end

module ToLVM = Block_queue.Pusher(LocalAllocation)
module FromLVM = Block_queue.Popper(LocalAllocation)

module Op = struct
  type t =
    | Print of string
    | LocalAllocation of LocalAllocation.t
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

module Allocator = Lvm.Allocator.Make(struct
  open Devmapper
  type t = Location.device with sexp
  let compare (a: t) (b: t) = compare a b
  let to_string t = Sexplib.Sexp.to_string (sexp_of_t t)
end)

(* Remove the physical blocks [targets] from [from], and then create a new
   virtual LV containing the remainder *)
let remove_physical from targets =
  let open Devmapper in
  let area_of_target target = match target.Target.kind with
  | Target.Linear location -> location.Location.device, (location.Location.offset, target.size)
  | Target.Unknown _ -> failwith "unknown target not implemented"
  | Target.Striped _ -> failwith "striping is not implemented" in
  let existing = List.map area_of_target from in
  let to_remove = List.map area_of_target targets in
  let remaining = Allocator.sub existing to_remove in
  let _, targets =
    List.fold_left
      (fun (next_voffset, acc) (device, (poffset, size)) ->
        Int64.add next_voffset size,
        { Target.start = next_voffset;
          size = size;
          kind = Target.Linear { Location.device; offset = poffset } } :: acc
      ) (0L, []) remaining in
  List.rev targets

let rec try_forever f =
  f ()
  >>= function
  | `Ok x -> return x
  | _ ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    try_forever f

let stat x =
  match Devmapper.stat x with
  | Some x -> return (`Ok x)
  | None ->
    error "The device mapper device %s has disappeared." x;
    return `Retry

let main config socket journal freePool fromLVM toLVM =
  let config = Config.t_of_sexp (Sexplib.Sexp.load_sexp config) in
  let config = {
    Config.socket = (match socket with None -> config.Config.socket | Some x -> x);
    localJournal = (match journal with None -> config.Config.localJournal | Some x -> x);
    freePool = (match freePool with None -> config.Config.freePool | Some x -> x);
    toLVM = (match toLVM with None -> config.Config.toLVM | Some x -> x);
    fromLVM = (match fromLVM with None -> config.Config.fromLVM | Some x -> x);
  } in
  debug "Loaded configuration: %s" (Sexplib.Sexp.to_string_hum (Config.sexp_of_t config));

  (* Perform some prechecks *)
  let freePool =
    begin match Devmapper.stat config.Config.freePool with
      | None ->
        let ls = Devmapper.ls () in
        failwith (Printf.sprintf "Failed to find freePool %s. Available volumes are: %s" config.Config.freePool (String.concat ", " ls))
      | Some _ ->
        ()
    end;
    freePool in

  let t =
    ToLVM.start config.Config.toLVM
    >>= fun tolvm ->

    let receive_free_blocks_forever () =
      debug "Receiving free blocks from the SRmaster forever";
      FromLVM.start config.Config.fromLVM
      >>= fun from_lvm -> (* blocks for a while *)
      let rec loop_forever () =
        FromLVM.pop from_lvm
        >>= fun (pos, ts) ->
        let open LocalAllocation in
        Lwt_list.iter_s
          (fun t ->
            sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
            assert (t.fromLV = "");
            Devmapper.suspend config.Config.freePool;
            try_forever (fun () -> stat config.Config.freePool)
            >>= fun to_volume ->
            let to_targets = to_volume.Devmapper.targets @ t.targets in
            Devmapper.reload config.Config.freePool to_targets;
            Devmapper.resume config.Config.freePool;
            return ()
          ) ts
        >>= fun () ->
        FromLVM.advance from_lvm pos
        >>= fun () ->
        loop_forever () in
      loop_forever () in
    let (_: unit Lwt.t) = receive_free_blocks_forever () in

    let perform t =
      let open Op in
      sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
      match t with
      | Print _ -> return ()
      | LocalAllocation ({ LocalAllocation.fromLV; toLV; targets } as b) ->
        ToLVM.push tolvm b
        >>= fun position ->
        try_forever (fun () -> stat fromLV)
        >>= fun from_volume ->
        try_forever (fun () -> stat toLV)
        >>= fun to_volume ->
        (* Remove the physical blocks from fromLV *)
        let from_targets = remove_physical from_volume.Devmapper.targets targets in
        (* Append the physical blocks to toLV *)
        let to_targets = to_volume.Devmapper.targets @ targets in
        Devmapper.suspend fromLV;
        Devmapper.suspend toLV;
        print_endline "Suspend local dm devices";
        Printf.printf "reload %s with\n%s\n%!" fromLV (Sexplib.Sexp.to_string_hum (LocalAllocation.sexp_of_targets from_targets));
        Devmapper.reload fromLV from_targets;
        Printf.printf "reload %s with\n%S\n%!" toLV (Sexplib.Sexp.to_string_hum (LocalAllocation.sexp_of_targets to_targets));
        Devmapper.reload toLV to_targets;
        print_endline "Move target from one to the other (make idempotent)";
        ToLVM.advance tolvm position
        >>= fun () ->
        Devmapper.resume fromLV;
        Devmapper.resume toLV;
        print_endline "Resume local dm devices";
        return () in

    let module J = Shared_block.Journal.Make(Block_ring_unix.Producer)(Block_ring_unix.Consumer)(Op) in
    J.start config.Config.localJournal perform
    >>= fun j ->

    let ls = Devmapper.ls () in
    debug "Visible device mapper devices: [ %s ]\n%!" (String.concat "; " ls);

    let rec loop () =
      Lwt_io.read_line Lwt_io.stdin
      >>= fun line ->
      ( match Devmapper.stat line with
      | None ->
        (* Log this kind of error. This tapdisk may block but at least
           others will keep going *)
        J.push j (Op.Print ("Couldn't find device mapper device: " ^ line))
      | Some data_volume ->
        try_forever (fun () -> stat config.Config.freePool)
        >>= fun free_volume ->
        (* choose the blocks we're going to transfer *)
        try_forever (fun () ->
          try
            let physical_blocks = first_exn free_volume 1024L in
            (* append these onto the data_volume *)
            let new_targets = new_targets data_volume physical_blocks in
            return (`Ok (LocalAllocation.({ fromLV = config.Config.freePool; toLV = line; targets = new_targets })))
          with Retry ->
            info "There aren't enough free blocks, waiting.";
            return `Retry
        )
        >>= fun bu ->
        J.push j (Op.LocalAllocation bu)
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

let config =
  let doc = "Path to the config file" in
  Arg.(value & opt file "localConfig" & info [ "config" ] ~docv:"CONFIG" ~doc)

let socket =
  let doc = "Path of Unix domain socket to listen on" in
  Arg.(value & opt (some string) None & info [ "socket" ] ~docv:"SOCKET" ~doc)

let journal =
  let doc = "Path of the host local journal" in
  Arg.(value & opt (some file) None & info [ "journal" ] ~docv:"JOURNAL" ~doc)

let freePool =
  let doc = "Name of the device mapper device containing the free blocks" in
  Arg.(value & opt (some string) None & info [ "freePool" ] ~docv:"FREEPOOL" ~doc)

let toLVM =
  let doc = "Path to the device or file to contain the pending LVM metadata updates" in
  Arg.(value & opt (some file) None & info [ "toLVM" ] ~docv:"TOLVM" ~doc)

let fromLVM =
  let doc = "Path to the device or file which contains new free blocks from LVM" in
  Arg.(value & opt (some file) None & info [ "fromLVM" ] ~docv:"FROMLVM" ~doc)

let () =
  let t = Term.(pure main $ config $ socket $ journal $ freePool $ fromLVM $ toLVM) in
  match Term.eval (t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0
