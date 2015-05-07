(* We wish to ensure at-most-one copy of the program is started *)

let write_pid pidfile =
  let txt = string_of_int (Unix.getpid ()) in
  try
    let fd = Unix.openfile pidfile [ Unix.O_WRONLY; Unix.O_CREAT ] 0o0644 in
    Unix.lockf fd Unix.F_TLOCK (String.length txt);
    let (_: int) = Unix.write fd txt 0 (String.length txt) in
    ()
  with e ->
    Printf.fprintf stderr "%s\n" (Printexc.to_string e);
    Printf.fprintf stderr "The pidfile %s is locked: you cannot start the program twice!\n" pidfile;
    Printf.fprintf stderr "If the process was shutdown cleanly then verify and remove the pidfile.\n%!";
    exit 1
