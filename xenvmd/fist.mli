type t

val dummy : t
val freepool_fail_point0 : t
val freepool_fail_point1 : t
val freepool_fail_point2 : t

val get : t -> bool
val set : t -> bool -> unit
val list : unit -> (string * bool) list
val t_of_string : string -> t option

val maybe_exn : t -> unit
val maybe_lwt_fail : t -> unit Lwt.t
