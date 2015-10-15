type t

val dummy : t

val get : t -> bool
val set : t -> bool -> unit
val list : unit -> (string * bool) list
val t_of_string : string -> t option
