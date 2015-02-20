(* XenVM LVM type thing *)

exception Uninitialised
exception AlreadyOpen

external vgopen : devices:string list -> unit = ""

external get_lv: name:string -> (Vg_wrapper.t * Lv_wrapper.t) = ""
(** [get_lv lv] returns all the information you need to be able to map
    the blocks of an LV: metadata for the VG (with an empty list of LVs)
    and metadata for the LV in question. *)

external get : unit -> Vg_wrapper.t = ""
external create : name:string -> size:int64 -> unit = ""
external rename : oldname:string -> newname:string -> unit = ""

external set_redo_log : path:string -> unit = ""
(** [set_redo_log path] uses [path] as a redo log for the LVM metadata.
    Metadata changes will be appended to the log in O(1) time, rather than
    O(N) for a full write (where 'N' refers to the number of LVs) *)

external set_journal : path:string -> unit = ""
(** [set_journal path] uses [path] as an operation journal to ensure
    key operations (such as free block allocations) are performed
    at-least-once. *)

external shutdown : unit -> unit = ""

type host = {
  name: string;
  toLVM: string; (* LVM updates coming from a host *)
  fromLVM: string; (* LVM updates sent to a host *)
  freeLV: string; (* name of the host's free block LV *)
}
external register: host -> unit = ""
