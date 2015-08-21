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
open Core.Std

exception Could_not_obtain_lock of string

let with_flock path f =
  Unix.mkdir_p ~perm:0o700 (Filename.dirname path);
  let fd = Unix.(openfile path ~mode:[O_WRONLY; O_CREAT] ~perm:0o644) in
  let rec try_lock tries =
    if tries > 0 then
      if Unix.(flock fd Flock_command.lock_exclusive) then ()
      else begin Unix.sleep 1; try_lock (tries - 1) end
    else raise (Could_not_obtain_lock path)
  in
  try_lock 5;
  protect ~f ~finally:(fun () -> Unix.(ignore (flock fd Flock_command.unlock)))
