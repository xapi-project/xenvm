val start : string -> unit Lwt.t
val shutdown : unit -> unit Lwt.t
val resend_free_volumes : unit -> unit Lwt.t
val top_up_free_volumes : Config.Xenvmd.t -> unit Lwt.t
