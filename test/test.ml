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

module Log = struct
  let debug fmt = Printf.ksprintf (fun s -> print_endline s) fmt
  let info  fmt = Printf.ksprintf (fun s -> print_endline s) fmt
  let error fmt = Printf.ksprintf (fun s -> print_endline s) fmt
end

module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end

module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock)

let (>>*=) m f = match Lvm.Vg.error_to_msg m with
  | `Error (`Msg e) -> fail (Failure e)
  | `Ok x -> f x
let (>>|=) m f = m >>= fun x -> x >>*= f

let with_dummy fn =
  let filename = "/tmp/vg" in
  let f = Unix.openfile filename [Unix.O_CREAT; Unix.O_RDWR; Unix.O_TRUNC] 0o644 in
  (* approximately 10000 4MiB extents for volumes, 100MiB for metadata and
     overhead *)
  let _ = Unix.lseek f (1024*1024*4*10100 - 1) Unix.SEEK_SET in
  ignore(Unix.write f "\000" 0 1);
  Unix.close f;
  let result = fn filename in
  Unix.unlink filename;
  result
  (* NB we leak the file on error, but this is quite useful *)

let with_block filename f =
  let open Lwt in
  Block.connect (Printf.sprintf "buffered:%s" filename)
  >>= function
  | `Error x ->
    fail (Failure (Printf.sprintf "Unable to read %s" filename))
  | `Ok x ->
    f x (* no point catching errors here *)

let pv = Result.get_ok (Pv.Name.of_string "pv0")
let small = Int64.(mul (mul 1024L 1024L) 4L)
let small_extents = 1L

let xenvm = "./xenvm.native"

let vgcreate () =
  with_dummy (fun filename ->
    Common.run xenvm [ "vgcreate"; "myvg"; filename ] |> ignore_string;
    let t =
      with_block filename
        (fun block ->
           Vg_IO.connect [ block ] `RO >>= fun t ->
           let vg = Result.get_ok t in
           assert_equal ~printer:(fun x -> x) "myvg" (Vg_IO.metadata_of vg).Vg.name;
           return ()
        ) in
    Lwt_main.run t 
  )

let suite = "LVM-style interface" >::: [
  "vgcreate <vg name> <device>" >:: vgcreate;
]
 
let _ =
  run_test_tt suite
