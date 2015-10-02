open Lwt
open Sexplib.Std
open Log
open Errors

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end

let dm = ref (module Devmapper.Linux : Devmapper.S.DEVMAPPER)

let journal_size = Int64.(mul 4L (mul 1024L 1024L))

let retry ~dbg ~retries ~interval f =
  debug "Attempting to '%s': will try at most %d times with a %ds interval."
    dbg retries interval;
  let rec aux n =
    if n <= 0 then f ()
    else
      try f ()
      with exn ->
        warn "warning: '%s' failed with '%s'; will retry %d more time%s..."
          dbg (Printexc.to_string exn) retries (if retries = 1 then "" else "s");
        Unix.sleep interval;
        aux (retries - 1) in
  aux retries

(* This error must cause the system to stop for manual maintenance.
   Perhaps we could scope this later and take down only a single connection? *)
let fatal_error_t msg =
  error "%s" msg;
  fail (Failure msg)

let fatal_error msg m = m >>= function
  | `Error _ -> fatal_error_t msg
  | `Ok x -> return x

let with_block filename f =
  let open Lwt in
  Block.connect filename
  >>= function
  | `Error _ -> fail (Failure (Printf.sprintf "Unable to read %s" filename))
  | `Ok x ->
    Lwt.catch (fun () -> f x) (fun e -> Block.disconnect x >>= fun () -> fail e)

module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock)

let get_device config = match config.Config.Local_allocator.devices with
  | [ x ] -> return x
  | _ ->
    fail (Failure "I require exactly one physical device")

let query_lvm config =
  get_device config
  >>= fun device ->
  with_block device
    (fun x ->
      Vg_IO.connect [ x ] `RO
      >>|= fun x ->
      return x
    )

module FromLVM = struct
  module R = Shared_block.Ring.Make(Log)(Vg_IO.Volume)(FreeAllocation)
  let rec attach ~disk () =
    fatal_error "attaching to FromLVM queue" (R.Consumer.attach ~queue:"FromLVM Consumer" ~client:"xenvm-local-allocator" ~disk ()) 
  let state t =
    fatal_error "querying FromLVM state" (R.Consumer.state t)
  let rec suspend t =
    retry_forever (fun () -> R.Consumer.suspend t)
    >>= fun x ->
    fatal_error "FromLVM.suspend" (return x)
    >>= fun () ->
      let rec wait () =
        fatal_error "reading state of FromLVM" (R.Consumer.state t)
        >>= function
        | `Suspended -> return ()
        | `Running ->
          Lwt_unix.sleep 5.
          >>= fun () ->
          wait () in
      wait ()
  let rec resume t =
    retry_forever (fun () -> R.Consumer.resume t)
    >>= fun x ->
    fatal_error "FromLVM.suspend" (return x)
    >>= fun () ->
      let rec wait () =
        fatal_error "reading state of FromLVM" (R.Consumer.state t)
        >>= function
        | `Suspended ->
          Lwt_unix.sleep 5.
          >>= fun () ->
          wait ()
        | `Running -> return () in
      wait ()
  let rec pop t =
    R.Consumer.fold ~f:(fun item acc -> item :: acc) ~t ~init:[] ()
    >>= fun x ->
    fatal_error "FromLVM.pop" (return x)
    >>= fun (position, rev_items) ->
    let items = List.rev rev_items in
    return (position, items)
  let advance t position =
    fatal_error "advancing the FromLVM consumer pointer" (R.Consumer.advance ~t ~position ())
end
module ToLVM = struct
  module R = Shared_block.Ring.Make(Log)(Vg_IO.Volume)(ExpandVolume)
  let rec attach ~disk () =
    R.Producer.attach ~queue:"ToLVM Producer" ~client:"xenvm-local-allocator" ~disk ()
    >>= function
    | `Ok x -> return x
    | _ ->
      Lwt_unix.sleep 5.
      >>= fun () ->
      attach ~disk ()
  let state t =
    fatal_error "querying ToLVM state" (R.Producer.state t)
  let rec push t item = R.Producer.push ~t ~item () >>= function
  | `Error (`Retry | `Suspended) ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    push t item
  | `Error (`Msg x) -> fatal_error_t (Printf.sprintf "Error pushing to the ToLVM queue: %s" x)
  | `Ok x -> return x
  let advance t position =
    fatal_error "advancing ToLVM pointer" (R.Producer.advance ~t ~position ())
end


module FreePool = struct
  let m = Lwt_mutex.create ()
  let c = Lwt_condition.create ()
  let free = ref []

  let add extents =
    Lwt_mutex.with_lock m
      (fun () ->
        free := Lvm.Pv.Allocator.merge !free extents;
        Lwt_condition.broadcast c ();
        return ()
      )

  (* Allocate up to [nr_extents]. Blocks if there is no space free. Can return
     a partial allocation. *)
  let remove nr_extents =
    Lwt_mutex.with_lock m
      (fun () ->
        debug "FreePool.extents %Ld extents from %s" nr_extents
          (Sexplib.Sexp.to_string_hum (Lvm.Pv.Allocator.sexp_of_t !free));
        let rec wait () =
          match Lvm.Pv.Allocator.find !free nr_extents with
          | `Ok x ->
            free := Lvm.Pv.Allocator.sub !free x;
            return x
          | `Error (`OnlyThisMuchFree (_, 0L)) ->
            Lwt_condition.wait ~mutex:m c
            >>= fun () ->
            wait ()
          | `Error (`OnlyThisMuchFree (_, n)) ->
            begin match Lvm.Pv.Allocator.find !free n with
            | `Ok x ->
              free := Lvm.Pv.Allocator.sub !free x;
              return x
            | _ -> assert false
            end in
        wait ()
      )

    let start config vg =
      debug "Initialising the FreePool";
      ( match Vg_IO.find vg config.Config.Local_allocator.fromLVM with
        | Some x -> return x
        | None -> assert false ) >>= fun v ->
      Vg_IO.Volume.connect v
      >>= function
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" config.Config.Local_allocator.fromLVM))
      | `Ok disk ->
      FromLVM.attach ~disk ()
      >>= fun from_lvm ->
      FromLVM.state from_lvm
      >>= fun state ->
      debug "FromLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended");

      (* Suspend and resume the queue: the producer will resend us all
         the free blocks on resume. *)
      debug "Suspending FromLVM queue";
      FromLVM.suspend from_lvm
      >>= fun () ->
      (* Drop all queue entries to free space. They'll be duplicates
         of the large request we expect on resume. *)
      debug "Dropping FromLVM allocations";
      FromLVM.pop from_lvm
      >>= fun (pos, _) ->
      FromLVM.advance from_lvm pos
      >>= fun () ->
      debug "Resuming FromLVM queue";
      (* Resume the queue and we're good to go! *)
      FromLVM.resume from_lvm
      >>= fun () ->

      let rec loop_forever () =
        FromLVM.pop from_lvm
        >>= fun (pos, ts) ->
        let open FreeAllocation in
        ( if ts = [] then begin
            Lwt_unix.sleep 5.
          end else return ()
        ) >>= fun () ->
        Lwt_list.iter_s
          (fun t ->
            sexp_of_t t |> Sexplib.Sexp.to_string_hum |> debug "FreePool: received new allocation: %s";
            add t
          ) ts
        >>= fun () ->
        FromLVM.advance from_lvm pos
        >>= fun () ->
        loop_forever () in
      return loop_forever
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

(* Return the virtual size of the volume *)
let sizeof dm_targets =
  List.map (fun t -> t.Devmapper.Target.size) dm_targets
  |> List.fold_left Int64.add 0L

(* Compute the new segments and device mapper targets if [extents] are appended onto [lv] *)
let extend_volume device vg existing_targets extents =
  let open Lvm in
  let to_sector pv segment = Int64.(add pv.Pv.pe_start (mul segment vg.Vg.extent_size)) in
  (* We will extend the LV, so find the next 'virtual segment' *)
  let next_sector = sizeof existing_targets in
  let _, segments, targets =
    let open Devmapper in
    List.fold_left
      (fun (next_sector, segments, targets) (pvname, (psegment, size)) ->
        try
          let next_segment = Int64.div next_sector vg.Vg.extent_size in
          let pv = List.find (fun pv -> pv.Pv.name = pvname) vg.Vg.pvs in
          let device = Location.Path device in
          let segment =
            { Lvm.Lv.Segment.start_extent = next_segment;
              extent_count = size;
              cls = Lvm.Lv.Segment.Linear { Lvm.Lv.Linear.name = pvname; start_extent = psegment } } in
          let target =
            { Target.start = next_sector;
              size = Int64.mul size vg.Vg.extent_size;
              kind = Target.Linear { Location.device; offset = to_sector pv psegment } } in

          Int64.(add next_sector (mul size vg.Vg.extent_size)),
          segment :: segments,
          target :: targets
        with Not_found ->
          error "PV with name %s not found in volume group; where did this allocation come from?"
            (Lvm.Pv.Name.to_string pvname);
          next_sector, segments, targets
      ) (next_sector, [], []) extents in
   List.rev segments, List.rev targets

let targets_of x =
  let module D = (val !dm: Devmapper.S.DEVMAPPER) in
  match D.stat x with
  | Some x -> return (`Ok x.D.targets)
  | None ->
    error "The device mapper device %s has disappeared." x;
    return (`Error `Retry)

let main use_mock config daemon socket journal fromLVM toLVM =
  let open Config.Local_allocator in
  let config = t_of_sexp (Sexplib.Sexp.load_sexp config) in
  let config = { config with
    socket = (match socket with None -> config.socket | Some x -> x);
    localJournal = (match journal with None -> config.localJournal | Some x -> x);
    toLVM = (match toLVM with None -> config.toLVM | Some x -> x);
    fromLVM = (match fromLVM with None -> config.fromLVM | Some x -> x);
  } in
  debug "Loaded configuration: %s" (Sexplib.Sexp.to_string_hum (sexp_of_t config));
  dm := if use_mock then (module Devmapper.Mock: Devmapper.S.DEVMAPPER) else (module Devmapper.Linux: Devmapper.S.DEVMAPPER);

  let module D = (val !dm: Devmapper.S.DEVMAPPER) in

  if daemon then Lwt_daemon.daemonize ();

  Pidfile.write_pid (config.socket ^ ".lock");

  let t =
    Device.read_sector_size config.devices
    >>= fun sector_size ->

    get_device config
    >>= fun vg_device ->

    query_lvm config
    >>= fun vg ->
    let metadata = Vg_IO.metadata_of vg in

    ( match Vg_IO.find vg config.toLVM with
      | Some x -> return x
      | None -> assert false ) >>= fun v ->
    Vg_IO.Volume.connect v
    >>= function
    | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" config.toLVM))
    | `Ok disk ->
    ToLVM.attach ~disk ()
    >>= fun tolvm ->
    ToLVM.state tolvm
    >>= fun state ->
    debug "ToLVM queue is currently %s" (match state with `Running -> "Running" | `Suspended -> "Suspended");

    let extent_size = metadata.Lvm.Vg.extent_size in (* in sectors *)
    let extent_size_mib = Int64.(div (mul extent_size (of_int sector_size)) (mul 1024L 1024L)) in

    info "Device %s has %d byte sectors" (List.hd config.devices) sector_size;
    info "The Volume Group has %Ld sector (%Ld MiB) extents" extent_size extent_size_mib;

    let rec wait_for_shutdown_forever () =
      ToLVM.state tolvm
      >>= function
      | `Suspended ->
        info "The ToLVM queue has been suspended. We will acknowledge and exit";
        exit 0
      | `Running ->
        debug "The ToLVM queue is still running";
        Lwt_unix.sleep 5.
        >>= fun () ->
        wait_for_shutdown_forever () in

    (* This is the idempotent part which will be done at-least-once *)
    let perform ops =
      let open Op in
      Lwt_list.iter_s (fun t ->
        sexp_of_t t |> Sexplib.Sexp.to_string_hum |> print_endline;
        ToLVM.push tolvm t.volume
        >>= fun position ->
        let msg = Printf.sprintf "Querying dm device %s" t.device.ExpandDevice.device in
        retry_forever (fun () -> targets_of t.device.ExpandDevice.device)
        >>= fun x ->
        fatal_error msg (return x)
        >>= fun to_device_targets ->
        (* Append the physical blocks to toLV *)
        let to_targets = to_device_targets @ t.device.ExpandDevice.targets in
        retry ~dbg:"Suspend local dm device" ~retries:3 ~interval:1 (fun () ->
          D.suspend t.device.ExpandDevice.device);
        retry ~dbg:"Reload local dm device" ~retries:3 ~interval:1 (fun () ->
          D.reload t.device.ExpandDevice.device to_targets);
        ToLVM.advance tolvm position
        >>= fun () ->
        retry ~dbg:"Resume local dm device" ~retries:3 ~interval:1 (fun () ->
          D.resume t.device.ExpandDevice.device);
        return ()
      ) ops
      >>= fun () ->
      return (`Ok ()) in

    let module J = Shared_block.Journal.Make(Log)(Block)(Time)(Clock)(Op) in
    ( if not (Sys.file_exists config.localJournal) then begin
        info "Creating an empty journal: %s" config.localJournal;
        Lwt_unix.openfile config.localJournal [ Lwt_unix.O_CREAT; Lwt_unix.O_WRONLY ] 0o0666 >>= fun fd ->
        Lwt_unix.LargeFile.lseek fd Int64.(sub journal_size 1L) Lwt_unix.SEEK_CUR >>= fun _ ->
        Lwt_unix.write_string fd "\000" 0 1 >>= fun _ ->
        Lwt_unix.close fd
      end else return () ) >>= fun () ->
    ( Block.connect config.localJournal
      >>= function
      | `Ok x -> return x
      | `Error _ -> fail (Failure (Printf.sprintf "Failed to open localJournal device: %s" config.localJournal))
    ) >>= fun device ->

    (* We must replay the journal before resynchronising free blocks *)
    J.start ~client:"xenvm-local-allocator" ~name:"local allocator journal" device perform
    >>|= fun j ->

    FreePool.start config vg
    >>= fun forever_fun ->
    let (_: unit Lwt.t) = forever_fun () in
    let (_: unit Lwt.t) = wait_for_shutdown_forever () in

    (* Called to extend a single device. This function decides what needs to be
       done, pushes the operation to the journal and waits for it to complete.
       The idempotent perform function is exected at least once. We need to 
       make the whole thing a critical section to avoid double-allocating the same
       blocks to 2 different LVs. *)
    let handler =
      let m = Lwt_mutex.create () in
      fun { ResizeRequest.local_dm_name = device; action } ->
        Lwt_mutex.with_lock m
          (fun () ->
            (* We may need to enlarge in multiple chunks if the free pool is depleted *)
            let rec expand action = match D.stat device with
              | None ->
                (* Log this kind of error. This tapdisk may block but at least
                   others will keep going *)
                error "Couldn't find device mapper device: %s" device;
                return (ResizeResponse.Device_mapper_device_does_not_exist device)
              | Some data_volume ->
                let sector_size = Int64.of_int sector_size in
                let current = Int64.mul sector_size (sizeof data_volume.D.targets) in
                let extent_b = Int64.mul sector_size extent_size in
                (* NB: make sure we round up to the next extent *)
                let nr_extents = match action with
                | `Absolute x ->
                  Int64.(div (add (sub x current) (sub extent_b 1L)) extent_b)
                | `IncreaseBy x ->
                  Int64.(div (add x extent_b) extent_b) in
                if nr_extents <= 0L then begin
                  error "Request for %Ld (<= 0) segments" nr_extents;
                  return (ResizeResponse.Request_for_no_segments nr_extents)
                end else begin
                  FreePool.remove nr_extents
                  >>= fun extents ->
                  (* This may have allocated short *)
                  let nr_extents' = Lvm.Pv.Allocator.size extents in
                  let segments, targets = extend_volume vg_device metadata data_volume.D.targets extents in
                  let _, volume = Mapper.vg_lv_of_name device in
                  let volume = { ExpandVolume.volume; segments } in
                  let device = { ExpandDevice.extents; device; targets } in
                  J.push j { Op.volume; device }
                  >>|= fun wait ->
                  (* The operation is now in the journal *)
                  wait.J.sync ()
                  (* The operation is now complete *)
                  >>= fun () ->
                  let action = match action with
                  | `Absolute x -> `Absolute x
                  | `IncreaseBy x -> `IncreaseBy Int64.(sub x (mul nr_extents' (mul sector_size extent_size))) in
                  if nr_extents = nr_extents'
                  then return ResizeResponse.Success
                  else expand action
                end in
            expand action
          ) in

    let ls = D.ls () in
    debug "Visible device mapper devices: [ %s ]\n%!" (String.concat "; " ls);

    let rec stdin () =
      Lwt_io.read_line Lwt_io.stdin
      >>= fun device ->
      let r = { ResizeRequest.local_dm_name = device; action = `IncreaseBy 1L } in
      handler r
      >>= fun resp ->
      Lwt_io.write_line Lwt_io.stdout (Sexplib.Sexp.to_string_hum (ResizeResponse.sexp_of_t resp))
      >>= fun () ->
      stdin () in
    debug "Creating Unix domain socket %s" config.socket;
    let s = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
    Unix.setsockopt (Lwt_unix.unix_file_descr s) Unix.SO_REUSEADDR true;
    Lwt.catch (fun () -> Lwt_unix.unlink config.socket) (fun _ -> return ())
    >>= fun () ->
    debug "Binding and listening on the socket";
    Lwt_unix.bind s (Lwt_unix.ADDR_UNIX(config.socket));
    Lwt_unix.listen s 5;
    let conn_handler fd () =
      let ic = Lwt_io.of_fd ~mode:Lwt_io.input fd in
      let oc = Lwt_io.of_fd ~mode:Lwt_io.output ~close:return fd in
      (* read one line *)
      Lwt_io.read_line ic
      >>= fun message ->
      let r = ResizeRequest.t_of_sexp (Sexplib.Sexp.of_string message) in
      handler r
      >>= fun resp ->
      Lwt_io.write_line oc (Sexplib.Sexp.to_string (ResizeResponse.sexp_of_t resp))
      >>= fun () ->
      Lwt_io.flush oc
      >>= fun () ->
      Lwt_io.close ic
    in
    let rec unix () =
      debug "Calling accept on the socket";
      Lwt_unix.accept s
      >>= fun (fd, _) ->
      async (conn_handler fd);
      unix () in
    let listen_unix = unix () in
    debug "Waiting forever for requests";
    Lwt.join (listen_unix :: (if daemon then [] else [ stdin () ]))
    >>= fun () ->
    debug "Stopped listening";
    return () in
  try
    `Ok (Lwt_main.run t)
  with Failure msg ->
    error "%s" msg;
    `Error(false, msg)

open Cmdliner
let info =
  let doc =
    "Xenvm: local block allocator" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "xenvm-local-allocator" ~version:"0.1-alpha" ~doc ~man

let config =
  let doc = "Path to the config file" in
  Arg.(value & opt file "localConfig" & info [ "config" ] ~docv:"CONFIG" ~doc)

let daemon =
  let doc = "Detach from the terminal and run as a daemon" in
  Arg.(value & flag & info ["daemon"] ~docv:"DAEMON" ~doc)

let socket =
  let doc = "Path of Unix domain socket to listen on" in
  Arg.(value & opt (some string) None & info [ "socket" ] ~docv:"SOCKET" ~doc)

let journal =
  let doc = "Path of the host local journal" in
  Arg.(value & opt (some file) None & info [ "journal" ] ~docv:"JOURNAL" ~doc)

let toLVM =
  let doc = "Path to the device or file to contain the pending LVM metadata updates" in
  Arg.(value & opt (some file) None & info [ "toLVM" ] ~docv:"TOLVM" ~doc)

let fromLVM =
  let doc = "Path to the device or file which contains new free blocks from LVM" in
  Arg.(value & opt (some file) None & info [ "fromLVM" ] ~docv:"FROMLVM" ~doc)

let mock_dm_arg =
  let doc = "Enable mock interfaces on device mapper." in
  Arg.(value & flag & info ["mock-devmapper"] ~doc)

let () =
  let t = Term.(pure main $ mock_dm_arg $ config $ daemon $ socket $ journal $ fromLVM $ toLVM) in
  match Term.eval (t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0
