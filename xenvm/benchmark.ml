open Lwt
open Xenvm_common
open Cmdliner

let ( *** ) a b = Int64.mul a b

let time_this f x =
  let start_time = Unix.gettimeofday () in
  f x >>= fun () ->
  let end_time = Unix.gettimeofday () in
  return (end_time -. start_time)

let write_gnuplot ~title ~x_axis ~y_axis (points: (int * float) list) =
  let file_prefix = String.(Stringext.split title ' ' |> concat "-" |> lowercase) in
  let total_ops = List.length points in
  let total_time = List.map snd points |> List.fold_left (+.) 0. in
  let average_time = total_time /. (float_of_int total_ops) in

  let dat_file = (file_prefix ^ ".dat") in
  let oc_dat = open_out dat_file in
  Printf.fprintf oc_dat "# Entries: %d; Sum(y): %.2f; Average: %.2f" total_ops total_time average_time;
  Printf.fprintf oc_dat "# other-existing-lvs time-taken-for-op\n";
  List.iter (fun (x, y) -> Printf.fprintf oc_dat "%d %f\n" x y) points;
  close_out oc_dat;

  let oc_gp = open_out (file_prefix ^ ".gp") in
  Printf.fprintf oc_gp "set xlabel \"%s\"\n" x_axis;
  Printf.fprintf oc_gp "set ylabel \"%s\"\n" y_axis;
  Printf.fprintf oc_gp "set title \"%s\"\n" title;
  Printf.fprintf oc_gp "plot \"%s\" with points\n" dat_file;
  close_out oc_gp

let benchmark copts (vg_name,_) volumes =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;

    let creation_host = Unix.gethostname () in
    let creation_time = Unix.gettimeofday () |> Int64.of_float in
    let size = Int64.mul 1024L 1024L in

    let name = Printf.sprintf "test-lv-%d" in
    let create name = Client.create ~name ~size ~creation_host ~creation_time ~tags:[] in
    let remove name = Client.remove ~name in
    let resize name = Client.resize ~name ~size:(60L *** 1024L *** 1024L *** 1024L) in

    let rec loop create_times resize_times remove_times i =
      if i >= volumes then return (create_times, resize_times, remove_times)
      else
        time_this create (name i)
        >>= fun create_time ->
        time_this resize (name i)
        >>= fun resize_time ->
        time_this remove (name i)
        >>= fun remove_time ->
        begin
          if i * 100 / volumes <> (i + 1) * 100 / volumes
          then stdout "%d %% complete" (i * 100 / volumes)
          else return_unit
        end
        >>= fun () ->
        create (name i) (* recreate LV so there is +1 LV next loop iteration *)
        >>= fun () ->
        loop
          ((i, create_time)::create_times)
          ((i, resize_time)::resize_times)
          ((i, remove_time)::remove_times)
          (succ i) in
    loop [] [] [] 0 >>= fun (create_times, resize_times, remove_times) ->

    write_gnuplot ~title:(Printf.sprintf "Creating %d LVs" volumes)
      ~x_axis:"Existing LVs" ~y_axis:"Create time/s" create_times;
    write_gnuplot ~title:(Printf.sprintf "Resizing %d LVs to 60G" volumes)
      ~x_axis:"Existing LVs" ~y_axis:"Resize time/s" resize_times;
    write_gnuplot ~title:(Printf.sprintf "Remove %d LVs" volumes)
      ~x_axis:"Existing LVs" ~y_axis:"Remove time/s" remove_times;

    Lwt_list.iter_s (fun (i, _) -> remove (name i)) create_times

    >>= return in
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
  Term.(pure benchmark $ copts_t $ name_arg $ volumes_arg),
  Term.info "benchmark" ~sdocs:copts_sect ~doc ~man
