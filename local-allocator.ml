open Lwt
open Sexplib.Std
open Block_ring_unix
open Log

module Config = struct
  type t = {
    socket: string; (* listen on this socket *)
    port: int; (* listen on this port *)
    remoteHost: string; (* IP address of the remote *)
    remotePort: int; (* port of the remote server *)
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
  pvs: (Lvm.Pv.Name.t * pv) list;
}
(* The minimum amount of info about a VG we need ot know *)

let with_block filename f =
  let open Lwt in
  Block.connect filename
  >>= function
  | `Error _ -> fail (Failure (Printf.sprintf "Unable to read %s" filename))
  | `Ok x ->
    Lwt.catch (fun () -> f x) (fun e -> Block.disconnect x >>= fun () -> fail e)

let query_lvm config =
  let module Vg_IO = Lvm.Vg.Make(Block) in
  match config.Config.devices with
  | [] ->
    fail (Failure "I require at least one physical device")
  | device :: _ ->
    with_block device
      (fun x ->
        Vg_IO.read [ x ]
        >>= function
        | `Error e ->
          error "Fatal error reading LVM metadata: %s" e;
          fail (Failure (Printf.sprintf "Failed to read LVM metadata from %s" device))
        | `Ok x ->
          let x = Vg_IO.metadata_of x in
          let extent_size = x.Lvm.Vg.extent_size in
          let pvs = List.map (fun pv ->
            pv.Lvm.Pv.name, { pe_start = pv.Lvm.Pv.pe_start; device }
          ) x.Lvm.Vg.pvs in
          return { extent_size; pvs }
      )

module ToLVM = Block_queue.Pusher(ExpandVolume)
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
        debug "FreePool.find %Ld extents from %s" nr_extents
          (Sexplib.Sexp.to_string_hum (Lvm.Pv.Allocator.sexp_of_t !free)); 
        return (Lvm.Pv.Allocator.find !free nr_extents)
      )

  let remove extents =
    Lwt_mutex.with_lock m
      (fun () ->
        debug "FreePool.remove %s"
          (Sexplib.Sexp.to_string_hum (Lvm.Pv.Allocator.sexp_of_t extents));
        free := Lvm.Pv.Allocator.sub !free extents;
        return ()
      )
end

module Op = struct
  module T = struct
    type t = {
      volume: ExpandVolume.t;
      device: ExpandDevice.t;
    } with sexp 
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

(* Compute the new segments and device mapper targets if [extents] are appended onto [lv] *)
let extend_volume vg lv extents =
  let to_sector pv segment = Int64.(add pv.pe_start (mul segment vg.extent_size)) in
  (* We will extend the LV, so find the next 'virtual segment' *)
  let next_sector = sizeof lv in
  let _, segments, targets =
    let open Devmapper in
    List.fold_left
      (fun (next_sector, segments, targets) (pvname, (psegment, size)) ->
        try
          let next_segment = Int64.div next_sector vg.extent_size in
          let pv = List.assoc pvname vg.pvs in
          let device = Location.Path pv.device in
          let segment =
            { Lvm.Lv.Segment.start_extent = next_segment;
              extent_count = size;
              cls = Lvm.Lv.Segment.Linear { Lvm.Lv.Linear.name = pvname; start_extent = psegment } } in
          let target =
            { Target.start = next_sector;
              size = Int64.mul size vg.extent_size;
              kind = Target.Linear { Location.device; offset = to_sector pv psegment } } in

          Int64.(add next_sector (mul size vg.extent_size)),
          segment :: segments,
          target :: targets
        with Not_found ->
          error "PV with name %s not found in volume group; where did this allocation come from?"
            (Lvm.Pv.Name.to_string pvname);
          next_sector, segments, targets
      ) (next_sector, [], []) extents in
   segments, targets

let rec try_forever msg f =
  f ()
  >>= function
  | `Ok x -> return x
  | _ ->
    debug "%s: retrying after 5s" msg;
    Lwt_unix.sleep 5.
    >>= fun () ->
    try_forever msg f

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
    Device.read_sector_size config.Config.devices
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
        ( if ts = [] then begin
            debug "No free blocks, sleeping for 5s";
            Lwt_unix.sleep 5.
          end else return ()
        ) >>= fun () ->
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

    (* We can either cache the free blocks locally, or simply ask the remote
       to tell us on startup: *)
    let open Xenvm_client in
    Rpc.uri := Printf.sprintf "http://%s:%d/" config.Config.remoteHost config.Config.remotePort;
    Client.get_lv ~name:config.Config.freePool
    >>= fun (_, lv) ->
    FreePool.add (Lvm.Lv.to_allocation lv);
    let (_: unit Lwt.t) = receive_free_blocks_forever () in

    (* This is the idempotent part which will be done at-least-once *)
    let perform ops =
      let open Op in
      Lwt_list.iter_s (fun t ->
        sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
        ToLVM.push tolvm t.volume
        >>= fun position ->
        try_forever
          (Printf.sprintf "Querying dm device %s" t.device.ExpandDevice.device)
          (fun () -> stat t.device.ExpandDevice.device)
        >>= fun to_device ->
        FreePool.remove t.device.ExpandDevice.extents
        >>= fun () ->
        (* Append the physical blocks to toLV *)
        let to_targets = to_device.Devmapper.targets @ t.device.ExpandDevice.targets in
        Devmapper.suspend t.device.ExpandDevice.device;
        print_endline "Suspend local dm device";
        Devmapper.reload t.device.ExpandDevice.device to_targets;
        ToLVM.advance tolvm position
        >>= fun () ->
        Devmapper.resume t.device.ExpandDevice.device;
        print_endline "Resume local dm device";
        return ()
      ) ops in

    let module J = Shared_block.Journal.Make(Log)(Block)(Op) in
    ( Block.connect config.Config.localJournal
      >>= function
      | `Ok x -> return x
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open localJournal device: %s" config.Config.localJournal))
    ) >>= fun device ->
    J.start device perform
    >>= fun j ->

    (* Called to extend a single device. This function decides what needs to be
       done, pushes the operation to the journal and waits for it to complete.
       The idempotent perform function is exected at least once. We need to 
       make the whole thing a critical section to avoid double-allocating the same
       blocks to 2 different LVs. *)
    let handler =
      let m = Lwt_mutex.create () in
      fun device ->
        Lwt_mutex.with_lock m
          (fun () ->
            ( match Devmapper.stat device with
              | None ->
                (* Log this kind of error. This tapdisk may block but at least
                   others will keep going *)
                error "Couldn't find device mapper device: %s" device;
                return ()
              | Some data_volume ->
                try_forever
                  (Printf.sprintf "Waiting for %Ld MiB free space" config.Config.allocation_quantum)
                  (fun () -> FreePool.find Int64.(div config.Config.allocation_quantum extent_size_mib))
                >>= fun extents ->
                let segments, targets = extend_volume vg data_volume extents in
                let _, volume = Mapper.vg_lv_of_name device in
                let volume = { ExpandVolume.volume; segments } in
                let device = { ExpandDevice.extents; device; targets } in
                J.push j { Op.volume; device }
                >>= fun wait ->
                (* The operation is now in the journal *)
                wait ()
                (* The operation is now complete *)
            )
          ) in

    let ls = Devmapper.ls () in
    debug "Visible device mapper devices: [ %s ]\n%!" (String.concat "; " ls);

    let rec stdin () =
      Lwt_io.read_line Lwt_io.stdin
      >>= fun device ->
      handler device
      >>= fun () ->
      stdin () in
    let read_stdin = stdin () in

    let s = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_STREAM 0 in
    Unix.setsockopt (Lwt_unix.unix_file_descr s) Unix.SO_REUSEADDR true;
    Lwt_unix.bind s (Lwt_unix.ADDR_INET(Unix.inet_addr_of_string "0.0.0.0", config.Config.port));
    Lwt_unix.listen s 5;
    let rec tcp () =
      Lwt_unix.accept s
      >>= fun (fd, _) ->
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd in
      let rec per_connection () =
        Lwt_io.read_line ic
        >>= function
        | "" -> Lwt_io.close ic
        | device ->
          handler device
          >>= fun () ->
          per_connection () in
      let (_: unit Lwt.t) = per_connection () in
      tcp () in
    let listen_tcp = tcp () in
    
    Lwt.join [ read_stdin; listen_tcp ] in
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
