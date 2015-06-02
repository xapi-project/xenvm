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
      xenvm [ "set-vg-info"; "--pvpath"; filename; "-S"; "/tmp/xenvmd"; vg; "--local-allocator-path"; "/tmp/xenvm-local-allocator"; "--uri"; "file://local/services/xenvmd/"^vg ] |> ignore_string;
      xenvm [ "vgs"; vg ] |> ignore_string
    )
  )

let no_xenvmd_suite = "Commands which should work without xenvmd" >::: [
  vgcreate;
  vgs_offline;
]

let assert_lv_exists ?expected_size_in_extents name =
  let t =
    Lwt.catch
      (fun () ->
        Client.get_lv "test"
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
  xenvm [ "lvcreate"; "-n"; "test"; "-L"; "16"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:4L "test";
  xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string

let lvcreate_l =
  "lvcreate -n <name> -l <extents> <vg>: check that we can create an LV with a size in extents" >::
  fun () ->
  xenvm [ "lvcreate"; "-n"; "test"; "-l"; "2"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:2L "test";
  xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string

let lvcreate_percent =
  "lvcreate -n <name> -l 100%F <vg>: check that we can fill all free space in the VG" >::
  fun () ->
  xenvm [ "lvcreate"; "-n"; "test"; "-l"; "100%F"; vg ] |> ignore_string;
  let free = free_extents () in
  assert_equal ~printer:Int64.to_string 0L free;
  xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string

let kib = 1024L
let mib = Int64.mul kib 1024L
let gib = Int64.mul mib 1024L
let tib = Int64.mul mib 1024L
let xib = Int64.mul tib 1024L

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
      (fun () -> Client.create "toobig" xib "unknown" 0L [])
      (function Xenvm_interface.Insufficient_free_space(needed, available) -> return ()
       | e -> failwith (Printf.sprintf "Did not get Insufficient_free_space: %s" (Printexc.to_string e)))
  );
  try
    xenvm [ "lvcreate"; "-n"; "test"; "-l"; Int64.to_string xib; vg ] |> ignore_string;
    failwith "Did not get Insufficient_free_space"
  with
    | Bad_exit(5, _, _, stdout, stderr) ->
      let expected = "insufficient free space" in
      if not (contains stderr expected)
      then failwith (Printf.sprintf "stderr [%s] did not have expected string [%s]" stderr expected)
    | _ ->
      failwith "Expected exit code 5"

let file_exists filename =
  try
    Unix.LargeFile.stat filename |> ignore;
    true
  with Unix.Unix_error(Unix.ENOENT, _, _) -> false

let dm_exists name = match Devmapper.Linux.stat name with
  | None -> false
  | Some _ -> true

let dev_path_of name = "/dev/" ^ vg ^ "/" ^ name
let mapper_path_of name = "/dev/mapper/" ^ vg ^ "-" ^ name

let lvchange_n =
  "lvchange -an <device>: check that we can deactivate a volume" >::
  fun () ->
  xenvm [ "lvcreate"; "-n"; "test"; "-L"; "3"; vg ] |> ignore_string;
  assert_lv_exists ~expected_size_in_extents:1L "test";
  let vg_metadata, lv_metadata = Lwt_main.run (Client.get_lv "test") in
  let name = Mapper.name_of vg_metadata lv_metadata in
  xenvm [ "lvchange"; "-ay"; "/dev/" ^ vg ^ "/test" ] |> ignore_string;
  if not !Common.use_mock then begin (* FIXME: #99 *)
  assert_equal ~printer:string_of_bool true (file_exists (dev_path_of "test"));
  assert_equal ~printer:string_of_bool true (file_exists (mapper_path_of "test"));
  assert_equal ~printer:string_of_bool true (dm_exists name);
  end;
  xenvm [ "lvchange"; "-an"; "/dev/" ^ vg ^ "/test" ] |> ignore_string;
  if not !Common.use_mock then begin (* FIXME: #99 *)
  assert_equal ~printer:string_of_bool false (file_exists (dev_path_of"test"));
  assert_equal ~printer:string_of_bool false (file_exists (mapper_path_of"test"));
  assert_equal ~printer:string_of_bool false (dm_exists name);
  end;
  xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string

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
    xenvm [ "lvcreate"; "-n"; "test"; "-L"; "3"; vg ] |> ignore_string;
    (* This should use the network, not the on-disk metadata: *)
    let count = xenvm [ "vgs"; "/dev/" ^ vg; "--noheadings"; "-o"; "lv_count" ] |> parse_int in
    (* Did we see the new volume? *)
    assert_equal ~printer:string_of_int (expected+1) count;
    xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string
  )

let xenvmd_suite = "Commands which require xenvmd" >::: [
  lvcreate_L;
  lvcreate_l;
  lvcreate_percent;
  lvcreate_toobig;
  lvchange_n;
  vgs_online;
]

let _ =
  mkdir_rec "/tmp/xenvm.d" 0o0755;
  run_test_tt_main no_xenvmd_suite |> ignore;
  with_temp_file (fun filename' ->
    with_loop_device filename' (fun loop ->
      xenvm [ "vgcreate"; vg; loop ] |> ignore_string;
      xenvm [ "set-vg-info"; "--pvpath"; loop; "-S"; "/tmp/xenvmd"; vg; "--local-allocator-path"; "/tmp/xenvm-local-allocator"; "--uri"; "file://local/services/xenvmd/"^vg ] |> ignore_string;
      file_of_string "test.xenvmd.conf" ("( (listenPort ()) (listenPath (Some \"/tmp/xenvmd\")) (host_allocation_quantum 128) (host_low_water_mark 8) (vg "^vg^") (devices ("^loop^")))");
      xenvmd [ "--config"; "./test.xenvmd.conf"; "--daemon" ] |> ignore_string;
      Xenvm_client.Rpc.uri := "file://local/services/xenvmd/" ^ vg;
      Xenvm_client.unix_domain_socket_path := "/tmp/xenvmd";
      finally
        (fun () ->
          run_test_tt_main xenvmd_suite |> ignore;
        ) (fun () ->
          xenvm [ "shutdown"; "/dev/"^vg ] |> ignore_string
        )
    )
  )
