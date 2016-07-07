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
open Stdext.Threadext
open Lwt
open Log
open Lvm
module D = Debug.Make(struct let name = "xenvmd_stats" end)

let generate_stats owner vg =
  (* Dereference vg once and shadow so that all stats see the same world *)
  let vg = !vg in

  let size_of_lv_in_extents lv =
    List.map Lv.Segment.to_allocation lv.Lv.segments
    |> List.fold_left Pv.Allocator.merge []
    |> Pv.Allocator.size in

  let bytes_of_extents = Int64.(mul (mul vg.Vg.extent_size 512L)) in

  let size =
    vg.Vg.pvs
    |> List.map (fun pv -> bytes_of_extents pv.Pv.pe_count)
    |> List.fold_left Int64.add 0L in

  let phys_util =
    Vg.LVs.bindings vg.Vg.lvs
    |> List.map (fun (_, lv) -> size_of_lv_in_extents lv)
    |> List.map bytes_of_extents
    |> List.fold_left Int64.add 0L in

  let size_ds =
    Rrd.SR owner,
    Ds.ds_make
      ~name:"size"
      ~description:(Printf.sprintf "Size of SR %s" owner)
      ~value:(Rrd.VT_Int64 size)
      ~ty:Rrd.Gauge
      ~default:true
      ~min:0.0
      ~units:"B"
      () in
  let phys_util_ds =
    Rrd.SR owner,
    Ds.ds_make
      ~name:"physical_utilisation"
      ~description:(Printf.sprintf "Physical utilisation of SR %s" owner)
      ~value:(Rrd.VT_Int64 phys_util)
      ~ty:Rrd.Gauge
      ~default:true
      ~min:0.0
      ~units:"B"
      () in

  [size_ds; phys_util_ds]

let reporter_cache : Reporter.t option ref = ref None
let reporter_m = Lwt_mutex.create ()
let stop_signal_t, stop_signal_u = Lwt.wait ()

(* xenvmd currently exports just 1 datasource; a single page will suffice *)
let shared_page_count = 1

let start owner vg =
  let rec loop () =
    Lwt_mutex.with_lock reporter_m (fun () ->
      match !reporter_cache with
      | Some r -> return r
      | None ->
        info "Starting RRDD reporter" >>= fun () ->
        let reporter =
          Reporter.start_async
            (module D : Debug.DEBUG)
            ~uid:(Printf.sprintf "xenvmd-%d-stats" (Unix.getpid ()))
            ~neg_shift:0.5
            ~target:(Reporter.Local shared_page_count)
            ~protocol:Rrd_interface.V2
            ~dss_f:(fun () -> generate_stats owner vg) in
        reporter_cache := (Some reporter);
        return reporter
    ) >>= fun reporter ->
    let open Reporter in
    match get_state reporter with
    | Running ->
      debug "RRDD reporter currently running; will check again in 60s..."
      >>= fun () ->
      Lwt_unix.sleep 60. >>= loop
    | Stopped `New ->
      debug "RRDD reporter not yet started; will check again in 5s..."
      >>= fun () ->
      Lwt_unix.sleep 5. >>= loop
    | Stopped (`Failed exn) ->
      debug "RRDD reporter has failed with exception: %s; restarting in 60s..."
        (Printexc.to_string exn)
      >>= fun () ->
      reporter_cache := None;
      Lwt_unix.sleep 60. >>= loop
    | Stopped `Cancelled | Cancelled ->
      debug "RRDD reporter was explictly stopped; not restarting."
      >>= fun () ->
      return ()
  in
  Lwt.async (fun () -> Lwt.pick [stop_signal_t; loop ()])

let stop () =
  info "Stopping RRDD reporter"
  >>= fun () ->
  Lwt.wakeup_later stop_signal_u ();
  Lwt_mutex.with_lock reporter_m (fun () ->
    match !reporter_cache with
    | None -> return ()
    | Some reporter ->
        Reporter.cancel reporter;
        reporter_cache := None;
        return ()
  )
