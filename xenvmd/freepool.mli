val start : string -> unit Lwt.t
val shutdown : unit -> unit Lwt.t
val top_up_host : Config.Xenvmd.t -> string -> Hostdb.connected_host -> bool Lwt.t
val send_allocation_to : Hostdb.connected_host -> unit Lwt.t
val resend_free_volume_to : Hostdb.connected_host -> unit Lwt.t
val resend_free_volumes : unit -> unit Lwt.t
val top_up_free_volumes : Config.Xenvmd.t -> unit Lwt.t
val create : string -> int64 -> Lvm.Vg.metadata Lwt.t
