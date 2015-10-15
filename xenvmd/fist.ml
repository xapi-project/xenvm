(* Fist points for testing *)

type t = string

let dummy = "dummy"

let all = Hashtbl.create 10

let _ =
  Hashtbl.replace all dummy false

let get k = Hashtbl.find all k
let set k v = Hashtbl.replace all k v
let list () = Hashtbl.fold (fun k v acc -> (k,v)::acc) all []

let t_of_string str : t option =
  if Hashtbl.mem all str then Some str else None
