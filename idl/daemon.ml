(* This function is largely taken from the Lwt_daemon module, but extended
   such that the parent process will wait until the child confirms it has
   completely set up correctly *)

let daemonize () =
  Unix.chdir "/";

  (* The input end of the pipe is handled by the parent, which will
     read the exit code from it. the output end is given to the 
     child, which will write the exit code when it knows what it should
     be. *)
  let ic,oc = Lwt_io.pipe () in
  
  (* Exit the parent, and continue in the child: *)
  if Lwt_unix.fork () > 0 then begin
    (* Do not run exit hooks in the parent. *)
    Lwt_sequence.iter_node_l Lwt_sequence.remove Lwt_main.exit_hooks;
    let open Lwt.Infix in
    let t =
      Lwt_io.read_line ic >>= fun exitcode ->
      let exitcode = int_of_string exitcode in
      exit exitcode in
    Lwt_main.run t
  end;
    
  (* Redirection of standard IOs *)
  let dev_null = Unix.openfile "/dev/null" [Unix.O_RDWR] 0o666 in
  
  Unix.dup2 dev_null Unix.stdin;
  Unix.dup2 dev_null Unix.stdout;
  Unix.dup2 dev_null Unix.stderr;
  
  Unix.close dev_null;
  
  ignore (Unix.umask 0o022);
  
  ignore (Unix.setsid ());

  oc
    

let parent_should_exit oc n =
  match oc with
  | Some oc ->
    Lwt_io.write_line oc (Printf.sprintf "%d\n" n)
  | None ->
    Lwt.return ()
    
