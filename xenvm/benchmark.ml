open Lwt
open Xenvm_common
open Cmdliner

let benchmark copts (vg_name,_) volumes threads =
  let t =
    let creation_host = Unix.gethostname () in
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let mib = Int64.mul 1048576L 4L in

    let start = ref (Unix.gettimeofday ()) in
    let n_pending = ref volumes in
    let m = Lwt_mutex.create () in
    let n_complete = ref 0 in
    let times = ref [] in
    let on_complete () =
      incr n_complete;
      let n = !n_complete in
      times := (n, Unix.gettimeofday () -. !start) :: !times;
      if ((n * 100) / volumes) <> (((n + 1) * 100) / volumes)
      then stderr "%d %% complete" ((n * 100) / volumes)
      else return () in
    let rec worker f =
      Lwt_mutex.with_lock m (fun () ->
        if !n_pending > 0 then begin
          decr n_pending;
          return (Some (volumes - !n_pending))
        end else return None
      )
      >>= function
      | Some n ->
        f n
        >>= fun () ->
        on_complete ()
        >>= fun () ->
        worker f
      | None ->
        return () in

    let create n =
      Client.create ~name:(Printf.sprintf "test-lv-%d" n) ~size:mib ~creation_host ~creation_time:(Unix.gettimeofday () |> Int64.of_float) ~tags:[] in
    let destroy n =
      Client.remove ~name:(Printf.sprintf "test-lv-%d" n) in

    let rec mkints = function
      | 0 -> []
      | n -> n :: (mkints (n - 1)) in
    let creators = List.map (fun _ -> worker create) (mkints threads) in
    Lwt.join creators
    >>= fun () ->

    let oc = open_out "benchmark.dat" in
    let time = Unix.gettimeofday () -. !start in
    List.iter (fun (n, t) -> Printf.fprintf oc "%d %f\n" n t) (List.rev !times);
    Printf.fprintf oc "# %d creates in %.1f s\n" volumes time;
    Printf.fprintf oc "# Average %.1f /sec\n" (float_of_int volumes /. time);

    start := Unix.gettimeofday ();
    n_pending := volumes;
    n_complete := 0;
    times := [];

    let destroyers = List.map (fun _ -> worker destroy) (mkints threads) in
    Lwt.join destroyers
    >>= fun () ->
    let time = Unix.gettimeofday () -. !start in
    List.iter (fun (n, t) -> Printf.fprintf oc "%d %f\n" (volumes + n) t) (List.rev !times);
    Printf.fprintf oc "# %d destroys in %.1f s\n" volumes time;
    Printf.fprintf oc "# Average %.1f /sec\n" (float_of_int volumes /. time);
    close_out oc;
    let oc = open_out "benchmark.gp" in
    Printf.fprintf oc "set xlabel \"LV number\"\n";
    Printf.fprintf oc "set ylabel \"Time/seconds\"\n";
    Printf.fprintf oc "set title \"Creating and then destroying %d LVs\"\n" volumes;
    Printf.fprintf oc "plot \"benchmark.dat\" with points\n";
    close_out oc;
    return () in
  Lwt_main.run t


let benchmark_cmd =
  let doc = "Perform some microbenchmarks" in
  let man = [
    `S "DESCRIPTION";
    `P "Perform some microbenchmarks and print the results.";
  ] in
  let volumes_arg =
    let doc = "The number of logical volumes which should be created then destroyed." in
    Arg.(value & opt int 10000 & info [ "volumes"; "v" ] ~docv:"VOLUMES" ~doc) in
  let threads_arg =
    let doc = "The number of concurrent worker threads which should create then destroy the volumes." in
    Arg.(value & opt int 1 & info [ "threads"; "t" ] ~docv:"THREADS" ~doc) in
  Term.(pure benchmark $ copts_t $ name_arg $ volumes_arg $ threads_arg),
  Term.info "benchmark" ~sdocs:copts_sect ~doc ~man

