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

type host = {
  name: string;
  toLVM: string; (* LVM updates coming from a host *)
  fromLVM: string; (* LVM updates sent to a host *)
  freeLV: string; (* name of the host's free block LV *)
}
external register: host -> unit = ""
