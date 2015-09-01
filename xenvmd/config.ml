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
open Sexplib.Std

type t = {
  listenPort: int option; (* TCP port number to listen on *)
  listenPath: string option; (* path of a unix-domain socket to listen on *)
  host_allocation_quantum: int64; (* amount of allocate each host at a time (MiB) *)
  host_low_water_mark: int64; (* when the free memory drops below, we allocate (MiB) *)
  vg: string; (* name of the volume group *)
  devices: string list; (* physical device containing the volume group *)
} with sexp
