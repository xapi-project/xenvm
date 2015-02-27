(* XenVM LVM type thing *)

let _redo_log_name = "xenvm_redo_log"
let _journal_name = "xenvm_journal"

external get_lv: name:string -> (Vg_wrapper.t * Lv_wrapper.t) = ""
(** [get_lv lv] returns all the information you need to be able to map
    the blocks of an LV: metadata for the VG (with an empty list of LVs)
    and metadata for the LV in question. *)

external get : unit -> Vg_wrapper.t = ""
external create : name:string -> size:int64 -> unit = ""
external rename : oldname:string -> newname:string -> unit = ""

external shutdown : unit -> unit = ""

module Host = struct

  external create: name:string -> unit = ""
  (** [create host] creates and initialises the metadata volumes
      for a host with name [host] *)

  external connect: name:string -> unit = ""
  (** [connect host] connects to existing metadata volumes and
      process them. *)

  external disconnect: name:string -> unit = ""
  (** [disconnect host] disconnects from metadata volumes and
      stops processing them. *)

  external destroy: name:string -> unit = ""
  (** [destroy host] removes the metadata volumes for a host with
      name [host] *)
end
