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
  | `Error (`OnlyThisMuchFree space) -> fail (Failure (Printf.sprintf "Only this much space is available: %Ld" space))
  | `Error (`UnknownLV lv) -> fail (Failure (Printf.sprintf "The LV with name %s was not found" lv))
  | `Ok x -> f x
let (>>|=) m f = m >>= fun x -> x >>*= f
