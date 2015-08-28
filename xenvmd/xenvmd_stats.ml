(*
 * Copyright (C) 2015 Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)
open Rrdd_plugin
open Threadext
open Lwt
open Log
module D = Debug.Make(struct let name = "xenvmd_stats" end)

let phys_util () =
  let open Lvm in
  let size_of_lv_in_extents lv =
    List.map Lv.Segment.to_allocation lv.Lv.segments
    |> List.fold_left Pv.Allocator.merge []
    |> Pv.Allocator.size in
  let t =
    VolumeManager.read (fun x -> return x) >>= fun vg ->
    let bytes_of_extents = Int64.(mul (mul vg.Vg.extent_size 512L)) in
    Vg.LVs.bindings vg.Vg.lvs
    |> List.map (fun (_, lv) -> size_of_lv_in_extents lv)
    |> List.map bytes_of_extents
    |> List.fold_left Int64.add 0L
    |> return in
  Lwt_main.run t

let generate_stats owner =
  let phys_util_ds =
    Rrd.SR owner,
    Ds.ds_make
      ~name:"physical_utilisation"
      ~description:(Printf.sprintf "Physical uitilisation of SR %s" owner)
      ~value:(Rrd.VT_Int64 (phys_util ()))
      ~ty:Rrd.Gauge
      ~default:true
      ~min:0.0
      ~units:"B"
      ()
  in
  [phys_util_ds]

let reporter_cache : Reporter.t option ref = ref None
let reporter_m = Lwt_mutex.create ()
let stop_signal_t, stop_signal_u = Lwt.wait ()

(* xenvmd currently exports just 1 datasource; a single page will suffice *)
let shared_page_count = 1

let start owner =
  Lwt.async (fun () ->
    let rec loop () =
      Lwt_mutex.with_lock reporter_m (fun () ->
        match !reporter_cache with
        | Some r -> return r
        | None ->
          let reporter =
            info "Starting RRDD reporter";
            Reporter.start_async
              (module D : Debug.DEBUG)
              ~uid:(Printf.sprintf "xenvmd-%d-stats" (Unix.getpid ()))
              ~neg_shift:0.5
              ~target:(Reporter.Local shared_page_count)
              ~protocol:Rrd_interface.V2
              ~dss_f:(fun () -> generate_stats owner) in
          reporter_cache := (Some reporter);
          return reporter
      ) >>= fun reporter ->
      stop_signal_t >>= return <?> Lwt_unix.sleep 0.1 >>= fun () ->
      let open Reporter in
      match get_state reporter with
      | Running ->
        debug "RRDD reporter currently running; will check again in 60s...";
        stop_signal_t >>= return <?> Lwt_unix.sleep 60. >>= loop
      | Stopped `New ->
        debug "RRDD reporter not yet started; will check again in 5s...";
        stop_signal_t >>= return <?> Lwt_unix.sleep 5. >>= loop
      | Stopped (`Failed exn) ->
        debug "RRDD reporter has failed with exception: %s; restarting in 60s..."
          (Printexc.to_string exn);
        reporter_cache := None;
        stop_signal_t >>= return <?> Lwt_unix.sleep 60. >>= loop
      | Stopped `Cancelled | Cancelled ->
        debug "RRDD reporter was explictly stopped; not restarting.";
        return ()
    in
    loop ()
  )

let stop () =
  Lwt_mutex.with_lock reporter_m (fun () ->
    match !reporter_cache with
    | None -> return ()
    | Some reporter ->
        info "Stopping RRDD reporter";
        Reporter.cancel reporter;
        reporter_cache := None;
        Lwt.wakeup_later stop_signal_u ();
        return ()
  )
