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

open OUnit
open Lvm
open Vg
open Common
open Lwt

let vg = "myvg"

let get_hostname host =
  Printf.sprintf "host%d" host

let get_local_allocator_sockpath host =
  Printf.sprintf "/tmp/host%d-socket" host

let vgcreate =
  "vgcreate <vg name> <device>: check that we can create a volume group and re-read the metadata." >::
  fun () ->
  with_temp_file (fun filename ->
    xenvm [ "vgcreate"; vg; filename ] |> ignore_string;
    let t =
      with_block filename
        (fun block ->
           Vg_IO.connect [ block ] `RO >>= fun t ->
           let vg' = Result.get_ok t in
           assert_equal ~printer:(fun x -> x) vg (Vg_IO.metadata_of vg').Vg.name;
           return ()
        ) in
    Lwt_main.run t 
  )

let vgs_offline =
  "vgs <device>: check that we can read metadata without xenvmd." >::
  (fun () ->
    with_temp_file (fun filename ->
      xenvm [ "vgcreate"; vg; filename ] |> ignore_string;
      mkdir_rec "/tmp/xenvm.d" 0o0755;
      xenvm [ "set-vg-info"; "--pvpath"; filename; "-S"; "/tmp/xenvmd"; vg; "--local-allocator-path"; (get_local_allocator_sockpath 1); "--uri"; "file://local/services/xenvmd/"^vg ] |> ignore_string;
      xenvm [ "vgs"; vg ] |> ignore_string
    )
  )

let set_vg_info pvpath vg host =
  xenvm ~host [ "set-vg-info";
          "--pvpath"; pvpath;
          "-S"; "/tmp/xenvmd";
          vg;
          "--local-allocator-path"; get_local_allocator_sockpath host;
          "--uri"; "file://local/services/xenvmd/"^vg ]
  |> ignore_string

let with_xenvmd ?existing_vg ?(cleanup_vg=true) (f : string -> string -> 'a) =
  let host = 1 in
  let with_xenvmd_running loop =
    set_vg_info loop vg host;
    let config = {
        Config.Xenvmd.listenPort = None;
        listenPath = Some "/tmp/xenvmd";
        host_allocation_quantum = 128L;
        host_low_water_mark = 8L;
        vg;
        devices = [loop];
        rrd_ds_owner = Some (Printf.sprintf "xenvmd-%d-stats" (Unix.getpid ()));
      } in
      Sexplib.Sexp.to_string_hum (Config.Xenvmd.sexp_of_t config)
      |> file_of_string "test.xenvmd.conf";
      xenvmd [ "--config"; "./test.xenvmd.conf"; "--daemon" ] |> ignore_string;
      Xenvm_client.Rpc.uri := "file://local/services/xenvmd/" ^ vg;
      Xenvm_client.unix_domain_socket_path := "/tmp/xenvmd";
      finally
        (fun () -> f vg loop)
        (fun () -> xenvm [ "shutdown"; "/dev/"^vg ] |> ignore_string ) in
  match existing_vg with
  | Some path -> with_xenvmd_running path
  | None ->
    with_temp_file ~delete:cleanup_vg (fun filename ->
      with_loop_device filename (fun loop ->
        xenvm [ "vgcreate"; vg; loop ] |> ignore_string;
        with_xenvmd_running loop
      )
    )

let start_local_allocator host devices =
  let hostname = get_hostname host in
  ignore(xenvm [ "host-create"; vg; hostname]);
  let config_file = Printf.sprintf "local_allocator.%s.conf" hostname in
  let config = {
    Config.Local_allocator.socket = get_local_allocator_sockpath host;
    localJournal = Printf.sprintf "%s-localJournal" hostname;
    devices = devices;
    toLVM = Printf.sprintf "%s-toLVM" hostname;
    fromLVM = Printf.sprintf "%s-fromLVM" hostname;
  } in
  Sexplib.Sexp.to_string_hum (Config.Local_allocator.sexp_of_t config)
  |> file_of_string config_file;
  ignore(xenvm [ "host-connect"; vg; hostname]);
  ignore(local_allocator [ "--config"; config_file; "--daemon"])

let lvchange_offline =
  "lvchange vg/lv --offline: check that we can activate volumes offline" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  with_xenvmd ~cleanup_vg:false (fun vg _ ->
    xenvm [ "lvcreate"; "-n"; name; "-L"; "3"; vg ] |> ignore_string;
    xenvm [ "flush"; vg ^ "/" ^ name ] |> ignore_string;
    xenvm [ "lvchange"; "-ay"; vg ^ "/" ^ name; "--offline" ] |> ignore_string
  )

let pvremove =
  "pvremove <device>: check that we can make a PV unrecognisable" >::
  (fun () ->
    with_temp_file (fun filename ->
      xenvm [ "vgcreate"; vg; filename ] |> ignore_string;
      xenvm [ "vgs"; vg ] |> ignore_string;
      xenvm [ "pvremove"; filename ] |> ignore_string;
      try
        xenvm [ "vgs"; vg ] |> ignore_string;
        failwith "pvremove failed to hide a VG from vgs"
      with Bad_exit(1, _, _, _, _) -> ()
    )
  )

let upgrade =
  "Check that we can upgrade an LVM volume to XenVM" >:: fun () ->
  with_temp_file (fun filename ->
    let t =
      with_block filename (fun block ->
        Result.get_ok (Pv.Name.of_string "pv0") |> fun pv ->
        Vg_IO.format ~magic:`Lvm "vg" [ pv, block ] >>|= fun () ->
        Vg_IO.connect ~flush_interval:0. [ block ] `RW >>|= fun vg ->
        (* there should be no LVs on this (not even a redo log) *)
        assert_equal ~msg:"Formated volume (LVM magic) has non-zero LV count"
          ~printer:string_of_int 0 (LVs.cardinal (Vg_IO.metadata_of vg).Vg.lvs);
        (* create a couple of LVs *)
        let lv0 = (Uuid.(to_string (create ())), Int64.(10L * mib)) in
        let lv1 = (Uuid.(to_string (create ())), Int64.(20L * mib)) in
        Lwt_list.iter_p (fun (name, size) ->
          Vg.create (Vg_IO.metadata_of vg) name size >>*= fun (_, op) ->
          Vg_IO.update vg [ op ] >>|= fun () ->
          Vg_IO.sync vg >>|= fun () ->
          return ()
        ) [ lv0; lv1 ] >>= fun () ->
        (* check that these LVs are there *)
        assert_equal ~msg:"Unexpected number of LVs on LVM volume"
          ~printer:string_of_int 2 (LVs.cardinal (Vg_IO.metadata_of vg).Vg.lvs);

        (* Upgrade the volume to journalled *)
        xenvm [ "upgrade"; "--pvpath"; "vg"; "vg" ] |> ignore_string;
        (* Check it's idempotent *)
        xenvm [ "upgrade"; "--pvpath"; "vg"; "vg" ] |> ignore_string;

        (* check the changing of the magic persisted *)
        let module Label_IO = Label.Make(Block) in
        let (>>:=) m f = m >>= function `Ok x -> f x | `Error (`Msg e) -> fail (Failure e) in
        Label_IO.read block >>:= fun label ->
        assert_equal ~msg:"PV label was not as expected after upgrade"
          ~printer:(function None -> "" | Some m -> Sexplib.Sexp.to_string_hum (Magic.sexp_of_t m))
          (Some `Journalled) Label.(Label_header.magic_of label.label_header);
        (* Check we now have 1 more volume than before (the redo log) *)
        Vg_IO.connect ~flush_interval:0. [ block ] `RO >>|= fun vg ->
        assert_equal ~msg:"Unexpected number of LVs on LVM after upgrade"
          ~printer:string_of_int 3 (LVs.cardinal (Vg_IO.metadata_of vg).Vg.lvs);
        (* Check xenvmd is happy to connect *)
        with_xenvmd ~existing_vg:filename (fun vg _ ->
          let lv_count =
            xenvm [ "vgs"; "/dev/" ^ vg; "--noheadings"; "-o"; "lv_count" ]
            |> String.trim |> int_of_string in
          assert_equal ~msg:"Xenvmd reported wrong number of LVs"
            ~printer:string_of_int 3 lv_count;
          xenvm [ "lvs"; "/dev/" ^ vg ] |> ignore_string;
        );

        (* Downgrade the volume to lvm2 *)
        xenvm [ "downgrade"; "--pvpath"; "vg"; "vg" ] |> ignore_string;
        (* Check it's idempotent *)
        xenvm [ "downgrade"; "--pvpath"; "vg"; "vg" ] |> ignore_string;

        (* check the changing of the magic persisted *)
        Label_IO.read block >>:= fun label ->
        assert_equal ~msg:"PV label was not as expected after downgrade"
          ~printer:(function None -> "" | Some m -> Sexplib.Sexp.to_string_hum (Magic.sexp_of_t m))
          (Some `Lvm) Label.(Label_header.magic_of label.label_header);
        (* Check we now have 1 more volume than before (the redo log) *)
        Vg_IO.connect ~flush_interval:0. [ block ] `RO >>|= fun vg ->
        assert_equal ~msg:"Unexpected number of LVs on LVM after downgrade"
          ~printer:string_of_int 2 (LVs.cardinal (Vg_IO.metadata_of vg).Vg.lvs);
        return ()
      ) in
    Lwt_main.run t
  )

let no_xenvmd_suite = "Commands which should work without xenvmd" >::: [
  vgcreate;
  vgs_offline;
  lvchange_offline;
  pvremove;
  upgrade;
]

let assert_lv_exists ?expected_size_in_extents name =
  let t =
    Lwt.catch
      (fun () ->
        Client.get_lv name
        >>= fun (_, lv) ->
        match expected_size_in_extents with
        | None -> return ()
        | Some size -> assert_equal ~printer:Int64.to_string size (Lvm.Lv.size_in_extents lv); return ()
      ) (fun e ->
        Printf.fprintf stderr "xenvmd claims the LV %s doesn't exist: %s\n%!" name (Printexc.to_string e);
        failwith (Printf.sprintf "LV %s doesn't exist" name)
      ) in
  Lwt_main.run t

let free_extents () =
  let t =
    Client.get ()
    >>= fun vg ->
    return (Lvm.Pv.Allocator.size (vg.Lvm.Vg.free_space)) in
  Lwt_main.run t

let lvcreate_L =
  "lvcreate -n <name> -L <mib> <vg>: check that we can create an LV with a size in MiB" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-L"; "16"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:4L name;
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let lvcreate_l =
  "lvcreate -n <name> -l <extents> <vg>: check that we can create an LV with a size in extents" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-l"; "2"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:2L name;
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let lvcreate_percent =
  "lvcreate -n <name> -l 100%F <vg>: check that we can fill all free space in the VG" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-l"; "100%F"; vg ] |> ignore_string;
  let free = free_extents () in
  assert_equal ~printer:Int64.to_string 0L free;
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let kib = 1024L
let mib = Int64.mul kib 1024L
let gib = Int64.mul mib 1024L
let tib = Int64.mul gib 1024L
let pib = Int64.mul tib 1024L

let contains s1 s2 =
    let re = Str.regexp_string s2 in
    try 
       ignore (Str.search_forward re s1 0); 
       true
    with Not_found -> false

let lvcreate_toobig =
  "lvcreate -n <name> -l <too many>: check that we fail nicely" >::
  fun () ->
  Lwt_main.run (
    Lwt.catch
      (fun () -> Client.create "toobig" tib "unknown" 0L [])
      (function Xenvm_interface.Insufficient_free_space(needed, available) -> return ()
       | e -> failwith (Printf.sprintf "Did not get Insufficient_free_space: %s" (Printexc.to_string e)))
  );
  try
    let name = Uuid.(to_string (create ())) in
    xenvm [ "lvcreate"; "-n"; name; "-l"; Int64.to_string tib; vg ] |> ignore_string;
    failwith "Did not get Insufficient_free_space"
  with
    | Bad_exit(5, _, _, stdout, stderr) ->
      let expected = "insufficient free space" in
      if not (contains stderr expected)
      then failwith (Printf.sprintf "stderr [%s] did not have expected string [%s]" stderr expected)
    | e ->
      failwith (Printf.sprintf "Expected exit code 5; got exception: %s" (Printexc.to_string e))

let lvextend_toobig =
  "lvextend packer-virtualbox-iso-vg/swap_1 -L 1T: check that the failure is nice" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-l"; "100%F"; vg ] |> ignore_string;
  begin
    Lwt_main.run (
      Lwt.catch
        (fun () -> Client.resize name tib)
        (function Xenvm_interface.Insufficient_free_space(needed, available) -> return ()
         | e -> failwith (Printf.sprintf "Did not get Insufficient_free_space: %s" (Printexc.to_string e)))
    );
    try
      xenvm [ "lvextend"; vg ^ "/" ^ name; "-L"; Int64.to_string tib ] |> ignore_string;
      failwith "Did not get Insufficient_free_space"
    with
      | Bad_exit(5, _, _, stdout, stderr) ->
        let expected = "Insufficient free space" in
        if not (contains stderr expected)
        then failwith (Printf.sprintf "stderr [%s] did not have expected string [%s]" stderr expected)
      | e ->
        failwith (Printf.sprintf "Expected exit code 5: %s" (Printexc.to_string e))
  end;
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let file_exists filename =
  try
    Unix.LargeFile.stat filename |> ignore;
    true
  with Unix.Unix_error(Unix.ENOENT, _, _) -> false

let dm_exists name = match Devmapper.Linux.stat name with
  | None -> false
  | Some _ -> true

let dev_path_of name = "/dev/" ^ vg ^ "/" ^ name
let mapper_path_of name = "/dev/mapper/" ^ (Mapper.name_of vg name)

let lvchange_addtag =
  "lvchange vg/lv [--addtag|--removetag]: check that we can add tags" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-L"; "3"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:1L name;
  xenvm [ "lvchange"; vg ^ "/" ^ name; "--addtag"; "hidden" ] |> ignore_string;
  let vg_metadata, lv_metadata = Lwt_main.run (Client.get_lv name) in
  let tags = List.map Lvm.Name.Tag.to_string lv_metadata.Lvm.Lv.tags in
  if not(List.mem "hidden" tags)
  then failwith "Failed to add 'hidden' tag";
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let lvchange_deltag =
  "lvchange vg/lv [--addtag|--deltag]: check that we can add and remove tags" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-L"; "3"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:1L name;
  xenvm [ "lvchange"; vg ^ "/" ^ name; "--addtag"; "hidden" ] |> ignore_string;
  xenvm [ "lvchange"; vg ^ "/" ^ name; "--deltag"; "hidden" ] |> ignore_string;
  let vg_metadata, lv_metadata = Lwt_main.run (Client.get_lv name) in
  let tags = List.map Lvm.Name.Tag.to_string lv_metadata.Lvm.Lv.tags in
  if List.mem "hidden" tags
  then failwith "Failed to remove 'hidden' tag";
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let lvchange_n =
  "lvchange -an <device>: check that we can deactivate a volume" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-L"; "3"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:1L name;
  let vg_metadata, lv_metadata = Lwt_main.run (Client.get_lv name) in
  let map_name = Mapper.name_of vg_metadata.name lv_metadata.Lv.name in
  xenvm [ "lvchange"; "-ay"; "/dev/" ^ vg ^ "/" ^ name ] |> ignore_string;
  run "udevadm" [ "settle" ] |> ignore_string;
  if not !Common.use_mock then begin (* FIXME: #99 *)
  Printf.printf "Checking dev path does not exist...\n%!";
  assert_equal ~printer:string_of_bool true (file_exists (dev_path_of name));
  Printf.printf "Checking mapper path does not exist... (mapper_path_of_name=%s)\n%!" (mapper_path_of name);
  assert_equal ~printer:string_of_bool true (file_exists (mapper_path_of name));
  Printf.printf "Checking map name does not exist\n%!";
  assert_equal ~printer:string_of_bool true (dm_exists map_name);
  end;
  xenvm [ "lvchange"; "-an"; "/dev/" ^ vg ^ "/" ^ name ] |> ignore_string;
  run "udevadm" [ "settle" ] |> ignore_string;
  if not !Common.use_mock then begin (* FIXME: #99 *)
  Printf.printf "Checking dev path does not exist...\n%!";
  assert_equal ~printer:string_of_bool false (file_exists (dev_path_of name));
  Printf.printf "Checking mapper path does not exist...\n%!";
  assert_equal ~printer:string_of_bool false (file_exists (mapper_path_of name));
  Printf.printf "Checking map name does not exist\n%!";
  assert_equal ~printer:string_of_bool false (dm_exists map_name);
  
  end;
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string

let lvremove_deactivates =
  "lvremove: check that lvremove deactivates the LV" >::
  fun () ->
  let name = Uuid.(to_string (create ())) in
  xenvm [ "lvcreate"; "-n"; name; "-L"; "3"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:1L name;
  let vg_metadata, lv_metadata = Lwt_main.run (Client.get_lv name) in
  let map_name = Mapper.name_of vg_metadata.name lv_metadata.Lv.name in
  xenvm [ "lvchange"; "-ay"; "/dev/" ^ vg ^ "/" ^ name ] |> ignore_string;
  run "udevadm" [ "settle" ] |> ignore_string;
  if not !Common.use_mock then begin (* FIXME: #99 *)
  Printf.printf "Checking dev path does not exist...\n%!";
  assert_equal ~printer:string_of_bool true (file_exists (dev_path_of name));
  Printf.printf "Checking mapper path does not exist... (mapper_path_of_name=%s)\n%!" (mapper_path_of name);
  assert_equal ~printer:string_of_bool true (file_exists (mapper_path_of name));
  Printf.printf "Checking map name does not exist\n%!";
  assert_equal ~printer:string_of_bool true (dm_exists map_name);
  end;
  xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string;
  run "udevadm" [ "settle" ] |> ignore_string;
  if not !Common.use_mock then begin (* FIXME: #99 *)
  Printf.printf "Checking dev path does not exist...\n%!";
  assert_equal ~printer:string_of_bool false (file_exists (dev_path_of name));
  Printf.printf "Checking mapper path does not exist...\n%!";
  assert_equal ~printer:string_of_bool false (file_exists (mapper_path_of name));
  Printf.printf "Checking map name does not exist\n%!";
  assert_equal ~printer:string_of_bool false (dm_exists map_name);
  end


let parse_int x =
  int_of_string (String.trim x)

let vgs_online =
  "vgs <device>: check that we can read fresh metadata with xenvmd." >::
  (fun () ->
    let metadata = Lwt_main.run (Client.get ()) in
    let count = xenvm [ "vgs"; "/dev/" ^ vg; "--noheadings"; "-o"; "lv_count" ] |> parse_int in
    let expected = Lvm.Vg.LVs.cardinal metadata.Lvm.Vg.lvs in
    assert_equal ~printer:string_of_int expected count;
    (* The new LV will be cached: *)
    let name = Uuid.(to_string (create ())) in
    xenvm [ "lvcreate"; "-n"; name; "-L"; "3"; vg ] |> ignore_string;
    (* This should use the network, not the on-disk metadata: *)
    let count = xenvm [ "vgs"; "/dev/" ^ vg; "--noheadings"; "-o"; "lv_count" ] |> parse_int in
    (* Did we see the new volume? *)
    assert_equal ~printer:string_of_int (expected+1) count;
    xenvm [ "lvremove"; vg ^ "/" ^ name ] |> ignore_string
  )

let benchmark =
  "benchmark <device>: check the timing characteristics of xenvm commands at scale" >::
  fun () ->
  let should_skip = not (try Sys.getenv "TEST_BENCHMARK" <> "" with _ -> false) in
  skip_if should_skip "Skipping benchmarks (export $TEST_BENCHMARK) to enable";
  xenvm [ "benchmark"; vg ] |> print_endline

let xenvmd_suite = "Commands which require xenvmd" >::: [
  lvcreate_L;
  lvcreate_l;
  lvcreate_percent;
  lvcreate_toobig;
  lvchange_addtag;
  lvchange_deltag;
  lvchange_n;
  lvremove_deactivates;
  lvextend_toobig;
  vgs_online;
  benchmark;
]

exception Timeout

let la_is_running host =
  let pidfile = Printf.sprintf "%s.lock" (get_local_allocator_sockpath host) in
  match Pidfile.write_pid pidfile with
  | `Ok fd ->
    (* It clearly wasn't running, as we've got the lock *)
    Unix.close fd;
    Unix.unlink pidfile;
    false
  | `Error _ ->
    (* It's still running *)
    true

let rec wait_la_stop host =
  if la_is_running host then
    Lwt_unix.sleep 0.1 >>= fun () -> wait_la_stop host
  else Lwt.return ()

let la_start device =
  "Start and shutdown the local allocator" >::
  (fun () ->
     ignore(Lwt_main.run (
         let rec n_times n =
           if n=0 then Lwt.return () else begin
             start_local_allocator 1 [device];
             ignore(xenvm ["host-disconnect"; vg; "host1"]);
             Lwt.choose [wait_la_stop 1; (Lwt_unix.sleep 30.0 >>= fun () -> Lwt.fail Timeout)]
             >>= fun () ->
             n_times (n-1)
           end
         in
         n_times 1))
  )

let la_extend device =
  "Extend an LV with the local allocator" >::
  (fun () ->
     ignore(Lwt_main.run (
         let lvname = "test" in
         start_local_allocator 1 [device];
         ignore(xenvm ["lvcreate"; "-n"; lvname; "-L"; "4"; vg]);
         ignore(xenvm ["lvextend"; "-L"; "132"; "--live"; Printf.sprintf "%s/%s" vg lvname]);
         Lwt_unix.sleep 11.0 >>= fun () ->
         Client.get_lv lvname >>= fun (myvg, lv) ->
         ignore(myvg,lv);
         let size = Lvm.Lv.size_in_extents lv in
         Printf.printf "final size=%Ld\n%!" size;
         assert_equal ~msg:"Unexpected final size"
           ~printer:Int64.to_string 31L size;
         ignore(xenvm ["host-disconnect"; vg; "host1"]);
         Lwt.choose [wait_la_stop 1; (Lwt_unix.sleep 30.0 >>= fun () -> Lwt.fail Timeout)])))

let inparallel fns =
  Lwt.join (
    List.map (fun fn ->
        Lwt_preemptive.detach (fun () -> ignore(fn ())) ()) fns)

let la_extend_multi device =
  "Extend an LV with the local allocator" >::
  (fun () ->
     ignore(Lwt_main.run (
         let lvname = "test2" in
         let lvname2 = "test3" in
         start_local_allocator 1 [device];
         start_local_allocator 2 [device];
         set_vg_info device vg 2;
         inparallel [(fun () -> xenvm ["lvcreate"; "-n"; lvname; "-L"; "4"; vg]);
                     (fun () -> xenvm ["lvcreate"; "-n"; lvname2; "-L"; "4"; vg])]
         >>= fun () ->
         inparallel [(fun () -> xenvm ["lvextend"; "-L"; "132"; "--live"; Printf.sprintf "%s/%s" vg lvname]);
                     (fun () -> xenvm ~host:2 ["lvextend"; "-L"; "132"; "--live"; Printf.sprintf "%s/%s" vg lvname2])]
         >>= fun () ->
         inparallel [(fun () -> xenvm ["host-disconnect"; vg; "host1"] |> ignore);
                     (fun () -> xenvm ["host-disconnect"; vg; "host2"] |> ignore)]
         >>= fun () ->
         Client.get_lv lvname >>= fun (myvg, lv) ->
         Client.get_lv lvname2 >>= fun (_, lv2) ->
         ignore(myvg,lv,lv2);
         let size = Lvm.Lv.size_in_extents lv in
         let size2 = Lvm.Lv.size_in_extents lv2 in
         ignore(xenvm ["lvchange"; "-an"; Printf.sprintf "%s/%s" vg lvname]);
         ignore(xenvm ["lvchange"; "-an"; Printf.sprintf "%s/%s" vg lvname2]);
         Printf.printf "Sanity checking VG\n%!";
         Client.get () >>= fun myvg -> 
         Common.sanity_check myvg;
         Printf.printf "final size=%Ld final_size2=%Ld\n%!" size size2;
         assert_equal ~msg:"Unexpected final size"
           ~printer:Int64.to_string 33L size;
                  assert_equal ~msg:"Unexpected final size"
           ~printer:Int64.to_string 33L size2;

         let la_dead = wait_la_stop 1 >>= fun _ -> wait_la_stop 2 in
         Lwt.choose [la_dead; (Lwt_unix.sleep 30.0 >>= fun () -> Lwt.fail Timeout)]
       )))

let local_allocator_suite device = "Commands which require the local allocator" >::: [
(*    la_start device;
      la_extend device;*)
    la_extend_multi device;
]

let _ =
  Random.self_init ();
  List.iter (fun host -> let configdir = Common.get_configdir host in mkdir_rec configdir 0o0755) [1;2;3;4] in
  let check_results_with_exit_code results =
    if List.exists (function RFailure _ | RError _ -> true | _ -> false) results
    then exit 1 in
  run_test_tt_main no_xenvmd_suite |> check_results_with_exit_code;
  with_xenvmd (fun _ _ -> run_test_tt_main xenvmd_suite |> check_results_with_exit_code);
  if not !Common.use_mock then begin (* FIXME: #99 *)
    with_xenvmd (fun _ device -> run_test_tt_main (local_allocator_suite device) |> check_results_with_exit_code)
  end;
  dump_stats ()
