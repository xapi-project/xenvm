module Host : sig
  val create : string -> unit Lwt.t
  val connect : string -> unit Lwt.t
  val disconnect : cooperative:bool -> string -> unit Lwt.t
  val destroy : string -> unit Lwt.t
  val all : unit -> Xenvm_interface.host list Lwt.t
  val reconnect_all : unit -> unit Lwt.t
end

module FreePool : sig
  val start : string -> unit Lwt.t
  val shutdown : unit -> unit Lwt.t
  val resend_free_volumes : Config.xenvmd_config -> unit Lwt.t
  val top_up_free_volumes : Config.xenvmd_config -> unit Lwt.t
end

val vgopen : devices:string list -> unit Lwt.t
val read : (Lvm.Vg.metadata -> 'a Lwt.t) -> 'a Lwt.t
val write :
  (Lvm.Vg.metadata ->
   [< `Error of Lvm.Vg.error
    | `Ok of 'a * Lvm.Redo.Op.t ]) ->
  unit Lwt.t
val sync : unit -> unit Lwt.t
val flush_all : unit -> unit Lwt.t
val shutdown : unit -> unit Lwt.t
