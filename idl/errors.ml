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
open Lwt

let (>>*=) m f = match m with
  | `Error (`Msg e) -> fail (Failure e)
  | `Error (`DuplicateLV lv) -> fail (Failure (Printf.sprintf "An LV with name %s already exists" lv))
  | `Error (`OnlyThisMuchFree (needed, available)) -> fail (Xenvm_interface.Insufficient_free_space(needed, available))
  | `Error (`UnknownLV lv) -> fail (Failure (Printf.sprintf "The LV with name %s was not found" lv))
  | `Ok x -> f x
let (>>|=) m f = m >>= fun x -> x >>*= f

(* This error must cause the system to stop for manual maintenance.
 * Perhaps we could scope this later and take down only a single connection? *)
let fatal_error_t msg =
  Log.error "%s" msg >>= fun () ->
  fail (Failure msg)

let fatal_error msg m = m >>= function
  | `Error (`Msg x) -> fatal_error_t (msg ^ ": " ^ x)
  | `Error `Suspended -> fatal_error_t (msg ^ ": queue is suspended")
  | `Error `Retry -> fatal_error_t (msg ^ ": queue temporarily unavailable")
  | `Ok x -> return x

let rec retry_forever f =
  f ()
  >>= function
  | `Ok x -> return (`Ok x)
  | `Error `Retry ->
    Lwt_unix.sleep 5.
    >>= fun () ->
    retry_forever f
  | `Error x -> return (`Error x)

let wait_for f result =
  let new_f () =
    f () >>= function
    | `Error _ as x -> return x
    | `Ok s when s=result -> return (`Ok ())
    | `Ok _ -> return (`Error `Retry)
  in retry_forever new_f

let suspended_is_ok x =
  match x with
  | `Error `Suspended -> return (`Ok ())
  | y -> return y
