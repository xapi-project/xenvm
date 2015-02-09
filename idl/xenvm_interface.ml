(* XenVM LVM type thing *)

exception Uninitialised
exception AlreadyOpen

external format : name:string -> pvs:(string * string) list -> unit = ""
external vgopen : devices:string list -> unit = ""
external activate : name:string -> unit = ""
external get : unit -> Vg_wrapper.t = ""
external create : name:string -> size:int64 -> unit = ""
external rename : oldname:string -> newname:string -> unit = ""
external start_journal : path:string -> unit = ""

