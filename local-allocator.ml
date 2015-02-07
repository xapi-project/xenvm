open Lwt
open Sexplib.Std
open Block_ring_unix
open Log

module Config = struct
  type t = {
    socket: string; (* listen on this socket *)
    allocation_quantum: int64; (* amount of allocate each device at a time (MiB) *)
    localJournal: string; (* path to the host local journal *)
    devices: string list; (* devices containing the PVs *)
    freePool: string; (* path where we will store free block information *)
    toLVM: string; (* pending updates for LVM *)
    fromLVM: string; (* received updates from LVM *)
  } with sexp
end

type pv = {
  pe_start: int64;
  device: string;
} with sexp
(* The minimum amount of info about a PV we need ot know *)

type vg = {
  extent_size: int64;
  pvs: (string * pv) list;
}
(* The minimum amount of info about a VG we need ot know *)

let query_lvm config =
  let module Disk = Disk_mirage.Make(Block)(Io_page) in
  let module Vg_IO = Lvm.Vg.Make(Disk) in
  match config.Config.devices with
  | [] ->
    fail (Failure "I require at least one physical device")
  | device :: _ ->
    Vg_IO.read [ device ]
    >>= function
    | `Error e ->
      error "Fatal error reading LVM metadata: %s" e;
      fail (Failure (Printf.sprintf "Failed to read LVM metadata from %s" device))
    | `Ok x ->
      let extent_size = x.Lvm.Vg.extent_size in
      let pvs = List.map (fun pv ->
        pv.Lvm.Pv.name, { pe_start = pv.Lvm.Pv.pe_start; device = pv.Lvm.Pv.real_device }
      ) x.Lvm.Vg.pvs in
      return { extent_size; pvs }

module ToLVM = Block_queue.Pusher(LocalAllocation)
module FromLVM = Block_queue.Popper(FreeAllocation)

module FreePool = struct
  (** Record the free block list in a file *)

  let m = Lwt_mutex.create ()
  let free = ref []

  let add extents =
    Lwt_mutex.with_lock m
      (fun () ->
        free := Lvm.Pv.Allocator.merge !free extents;
        return ()
      )

  let find nr_extents =
    Lwt_mutex.with_lock m
      (fun () ->
        return (Lvm.Pv.Allocator.find !free nr_extents)
      )

  let remove extents =
    Lwt_mutex.with_lock m
      (fun () ->
        free := Lvm.Pv.Allocator.sub !free extents;
        return ()
      )
end

module Op = struct
  module T = struct
    type t =
      | Print of string
      | LocalAllocation of LocalAllocation.t
    with sexp
  end
  include SexpToCstruct.Make(T)
  include T
end

exception Retry (* out of space *)

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

(* Compute the new device mapper targets if [extents] are appended onto [lv] *)
let extend_volume vg lv extents =
  let to_sector pv segment = Int64.(add pv.pe_start (mul segment vg.extent_size)) in
  (* We will extend the LV, so find the next 'virtual segment' *)
  let next_sector = sizeof lv in
  let _, targets =
    let open Devmapper in
    List.fold_left
      (fun (next_sector, acc) (pvname, (psegment, size)) ->
        try
          let pv = List.assoc pvname vg.pvs in
          let device = Location.Path pv.device in
          Int64.(add next_sector (mul size vg.extent_size)),
          { Target.start = next_sector;
            size = Int64.mul size vg.extent_size;
            kind = Target.Linear { Location.device; offset = to_sector pv psegment } } :: acc
        with Not_found ->
          error "PV with name %s not found in volume group; where did this allocation come from?" pvname;
          next_sector, acc
      ) (next_sector, []) extents in
   targets

module Allocator = Lvm.Allocator.Make(struct
  open Devmapper
  type t = Location.device with sexp
  let compare (a: t) (b: t) = compare a b
  let to_string t = Sexplib.Sexp.to_string (sexp_of_t t)
end)

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
  let config = { config with
    Config.socket = (match socket with None -> config.Config.socket | Some x -> x);
    localJournal = (match journal with None -> config.Config.localJournal | Some x -> x);
    freePool = (match freePool with None -> config.Config.freePool | Some x -> x);
    toLVM = (match toLVM with None -> config.Config.toLVM | Some x -> x);
    fromLVM = (match fromLVM with None -> config.Config.fromLVM | Some x -> x);
  } in
  debug "Loaded configuration: %s" (Sexplib.Sexp.to_string_hum (Config.sexp_of_t config));

  let t =
    Device.read_sector_size (List.hd config.Config.devices)
    >>= fun sector_size ->

    query_lvm config
    >>= fun vg ->

    ToLVM.start config.Config.toLVM
    >>= fun tolvm ->

    let extent_size = vg.extent_size in (* in sectors *)
    let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in

    info "Device %s has %d byte sectors" (List.hd config.Config.devices) sector_size;
    info "The Volume Group has %Ld sector (%Ld MiB) extents" extent_size extent_size_mib;

    let receive_free_blocks_forever () =
      debug "Receiving free blocks from the SRmaster forever";
      FromLVM.start config.Config.fromLVM
      >>= fun from_lvm -> (* blocks for a while *)
      let rec loop_forever () =
        FromLVM.pop from_lvm
        >>= fun (pos, ts) ->
        let open FreeAllocation in
        Lwt_list.iter_s
          (fun t ->
            sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
            FreePool.add t
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
      | LocalAllocation ({ LocalAllocation.extents; toLV; targets } as b) ->
        ToLVM.push tolvm b
        >>= fun position ->
        try_forever (fun () -> stat toLV)
        >>= fun to_volume ->
        FreePool.remove extents
        >>= fun () ->
        (* Append the physical blocks to toLV *)
        let to_targets = to_volume.Devmapper.targets @ targets in
        Devmapper.suspend toLV;
        print_endline "Suspend local dm device";
        Printf.printf "reload %s with\n%S\n%!" toLV (Sexplib.Sexp.to_string_hum (LocalAllocation.sexp_of_targets to_targets));
        Devmapper.reload toLV to_targets;
        print_endline "Move target from one to the other (make idempotent)";
        ToLVM.advance tolvm position
        >>= fun () ->
        Devmapper.resume toLV;
        print_endline "Resume local dm device";
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
        try_forever (fun () ->
          FreePool.find Int64.(div config.Config.allocation_quantum extent_size_mib)
        ) >>= fun extents ->
        let targets = extend_volume vg data_volume extents in
        J.push j (Op.LocalAllocation(LocalAllocation.({ extents; toLV = line; targets })))
        (* XXX: need ot wait to prevent double-allocation *) 
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
