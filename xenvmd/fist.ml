(* Fist points for testing *)

type t = string
exception FistPointHit of string

let dummy = "dummy"
let freepool_fail_point0 = "freepool_fail_point0"
let freepool_fail_point1 = "freepool_fail_point1"
let freepool_fail_point2 = "freepool_fail_point2"

let all = Hashtbl.create 10

let _ =
  Hashtbl.replace all dummy false;
  Hashtbl.replace all freepool_fail_point1 false
    

let get k = Hashtbl.find all k
let set k v = Hashtbl.replace all k v
let list () = Hashtbl.fold (fun k v acc -> (k,v)::acc) all []

let t_of_string str : t option =
  if Hashtbl.mem all str then Some str else None

let maybe_exn k = if get k then raise (FistPointHit k)
let maybe_lwt_fail k = if get k then Lwt.fail (FistPointHit k) else Lwt.return ()
