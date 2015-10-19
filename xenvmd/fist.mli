type t = Xenvm_interface.fist

val get : t -> bool
val set : t -> bool -> unit
val list : unit -> (t * bool) list

val maybe_exn : t -> unit
val maybe_lwt_fail : t -> unit Lwt.t
