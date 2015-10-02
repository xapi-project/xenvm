type connected_host = {
  mutable state : Xenvm_interface.connection_state;
  to_LVM : Rings.ToLVM.t;
  from_LVM : Rings.FromLVM.t;
  free_LV : string;
  free_LV_uuid : Lvm.Vg.LVs.key;
}
val get_connected_hosts : unit -> (string * connected_host) list
val get_connected_host : string -> connected_host option
val toLVM : string -> string
val fromLVM : string -> string
val freeLVM : string -> string
val create : string -> unit Lwt.t
val sexp_of_exn : exn -> Sexplib.Sexp.t
val connect : string -> unit Lwt.t
val flush_m : Lwt_mutex.t
val flush_already_locked : string -> unit Lwt.t
val disconnect : cooperative:bool -> string -> unit Lwt.t
val destroy : string -> unit Lwt.t
val all : unit -> Xenvm_interface.host list Lwt.t
val reconnect_all : unit -> unit Lwt.t
val flush_one : string -> unit Lwt.t
val flush_all : unit -> unit Lwt.t
val shutdown : unit -> unit Lwt.t
