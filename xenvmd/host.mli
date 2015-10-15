val create : string -> unit Lwt.t
val connect : string -> unit Lwt.t
val disconnect : cooperative:bool -> string -> unit Lwt.t
val destroy : string -> unit Lwt.t
val all : unit -> Xenvm_interface.host list Lwt.t
val reconnect_all : unit -> unit Lwt.t
val flush_all : unit -> unit Lwt.t
val shutdown : unit -> unit Lwt.t
