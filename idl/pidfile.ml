(* We wish to ensure at-most-one copy of the program is started *)

let write_pid pidfile =
  let txt = string_of_int (Unix.getpid ()) in
  try
    let fd = Unix.openfile pidfile [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC ] 0o0644 in
    Unix.lockf fd Unix.F_TLOCK (String.length txt);
    let (_: int) = Unix.write fd txt 0 (String.length txt) in
    `Ok ()
  with e ->
    `Error (`Msg (Printexc.to_string e))

