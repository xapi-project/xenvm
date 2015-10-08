(* We wish to ensure at-most-one copy of the program is started *)

let write_pid pidfile =
  let txt = string_of_int (Unix.getpid ()) in
  try
    begin
      let fd = Unix.openfile pidfile [ Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC ] 0o0644 in
      try
        Unix.lockf fd Unix.F_TLOCK (String.length txt);
        let (_: int) = Unix.write fd txt 0 (String.length txt) in
        `Ok fd
      with e ->
        Unix.close fd;
        `Error (`Msg (Printexc.to_string e))
    end
  with
  e -> `Error (`Msg (Printexc.to_string e))
