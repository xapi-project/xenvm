(* XenVM LVM type thing *)

exception Uninitialised
exception AlreadyOpen

external format : name:string -> pvs:(string * string) list -> unit = ""
external vgopen : devices:string list -> unit = ""

external get_lv: name:string -> (Vg_wrapper.t * Lv_wrapper.t) = ""
(** [get_lv lv] returns all the information you need to be able to map
    the blocks of an LV: metadata for the VG (with an empty list of LVs)
    and metadata for the LV in question. *)

external get : unit -> Vg_wrapper.t = ""
external create : name:string -> size:int64 -> unit = ""
external rename : oldname:string -> newname:string -> unit = ""
external start_journal : path:string -> unit = ""
external shutdown : unit -> unit = ""
