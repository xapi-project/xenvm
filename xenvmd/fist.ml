(* Fist points for testing *)

type t = Xenvm_interface.fist

exception FistPointHit of string

let string_of_t t = Rpc.to_string (Xenvm_interface.rpc_of_fist t)

let all : (t * bool) list ref = ref []

let get k = try List.assoc k !all with _ -> false
let set k v = all := (k,v) :: (List.filter (fun (k',_) -> k' <> k) !all)
let list () = !all

let maybe_exn k = if get k then raise (FistPointHit (string_of_t k))
let maybe_lwt_fail k =
  if get k then
    let str = string_of_t k in
    Lwt.(Log.error "Causing Lwt thread failure due to fist point: %s" str
         >>= fun () -> Lwt.fail (FistPointHit str))
  else Lwt.return ()
