(* XenVM LVM type thing *)

exception HostNotCreated
exception HostStillConnecting of string
exception UnknownFistPoint of string

let _journal_name = "xenvm_journal"

external get_lv: name:string -> (Vg_wrapper.t * Lv_wrapper.t) = ""
(** [get_lv lv] returns all the information you need to be able to map
    the blocks of an LV: metadata for the VG (with an empty list of LVs)
    and metadata for the LV in question. *)

external get : unit -> Vg_wrapper.t = ""
external create : name:string -> size:int64 -> creation_host:string -> creation_time:int64 -> tags:string list -> unit = ""
external rename : oldname:string -> newname:string -> unit = ""
external remove : name:string -> unit = ""

exception Insufficient_free_space of (int64 (* extents needed *) * int64 (* extents available *))
(** There's not enough space to create or resize the LV *)

external resize : name:string -> size:int64 -> unit = ""
external set_status : name:string -> readonly:bool -> unit = ""

external add_tag: name:string -> tag:string -> unit = ""
(** [add_tag name tag]: adds the tag to the named LV. If the tag already
    exists then this operation succeeds anyway. *)

external remove_tag: name:string -> tag:string -> unit = ""
(** [remove_tag name tag]: removes the tag from the named LV. If the LV
    hasn't got the tag then the operation succeeds anywya. *)

external flush : name:string -> unit = ""
(** [flush lv] processes all pending allocations for this LV, such that
    future calls to [get_lv] will return accurate metadata. *)

(** [shutdown ()] will cause xenvmd to exit shortly after returning.
    The returned value is the pid of the process to enable the caller
    to wait until the process has actually exitted. *)
external shutdown : unit -> int = ""

type queue = {
  lv: string;
  suspended: bool;
  debug_info: (string * string) list;
}

type connection_state =
  | Resuming_to_LVM
  | Resending_free_blocks
  | Connected
  | Failed of string

type host = {
  name: string;
  connection_state: connection_state option;
  fromLVM: queue;
  toLVM: queue;
  freeExtents: int64;
}

type fist =
  | FreePool0
  | FreePool1
  | FreePool2

module Host = struct

  external create: name:string -> unit = ""
  (** [create host] creates and initialises the metadata volumes
      for a host with name [host] *)

  external connect: name:string -> unit = ""
  (** [connect host] connects to existing metadata volumes and
      process them. *)

  external disconnect: cooperative:bool -> name:string -> unit = ""
  (** [disconnect host] disconnects from metadata volumes and
      stops processing them. *)

  external destroy: name:string -> unit = ""
  (** [destroy host] removes the metadata volumes for a host with
      name [host] *)

  external all: unit -> host list = ""
end

module Fist = struct
  external set : fist -> bool -> unit = ""
  external list : unit -> (fist * bool) list = ""
end
