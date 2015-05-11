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

let no_xenvmd_suite = "Commands which should work without xenvmd" >::: [
  vgcreate;
]

let assert_lv_exists name =
  let t =
    Lwt.catch
      (fun () ->
        Client.get_lv "test"
        >>= fun (_lv, _vg) ->
        return ()
      ) (fun e ->
        Printf.fprintf stderr "xenvmd claims the LV %s doesn't exist: %s\n%!" name (Printexc.to_string e);
        failwith (Printf.sprintf "LV %s doesn't exist" name)
      ) in
  Lwt_main.run t

let lvcreate_L =
  "lvcreate -n <name> -L <mib> <vg>: check that we can create an LV with a size in MiB" >::
  fun () ->
  xenvm [ "lvcreate"; "-n"; "test"; "-L"; "4"; vg ] |> ignore_string;
  assert_lv_exists "test";
  xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string

let lvcreate_l =
  "lvcreate -n <name> -l <extents> <vg>: check that we can create an LV with a size in extents" >::
  fun () ->
  xenvm [ "lvcreate"; "-n"; "test"; "-l"; "1"; vg ] |> ignore_string;
  assert_lv_exists "test";
  xenvm [ "lvremove"; vg ^ "/test" ] |> ignore_string

let xenvmd_suite = "Commands which require xenvmd" >::: [
  lvcreate_L;
  lvcreate_l;
]

let _ =
  run_test_tt_main no_xenvmd_suite |> ignore;
  with_temp_file (fun filename' ->
    with_loop_device filename' (fun loop ->
      xenvm [ "vgcreate"; vg; loop ] |> ignore_string;
      mkdir_rec "/etc/xenvm.d" 0o0644;
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
