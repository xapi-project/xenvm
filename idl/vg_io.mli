module Time : sig type 'a io = 'a Lwt.t val sleep : float -> unit Lwt.t end

type vg

val metadata_of : vg -> Lvm.Vg.metadata
val format :
  string ->
  ?creation_host:string ->
  ?creation_time:int64 ->
  ?magic:Lvm.Magic.t ->
  (Lvm.Pv.Name.t * Block.t) list -> unit Lvm.Vg.result Lwt.t
val connect :
  ?flush_interval:float ->
  Block.t list -> [ `RO | `RW ] -> vg Lvm.Vg.result Lwt.t
val update : vg -> Lvm.Redo.Op.t list -> unit Lvm.Vg.result Lwt.t
val sync : vg -> unit Lvm.Vg.result Lwt.t
module Volume : sig
  include V1_LWT.BLOCK
  val connect : id -> [ `Error of error | `Ok of t ] Lwt.t
  val metadata_of : id -> Lvm.Lv.t
  end
val find : vg -> string -> Volume.id option
val vgopen : devices:string list -> unit Lwt.t
val read : (Lvm.Vg.metadata -> 'a Lwt.t) -> 'a Lwt.t
val write : (Lvm.Vg.metadata -> ('a * Lvm.Redo.Op.t) Lvm.Vg.result) -> 'a Lwt.t
val sync : unit -> unit Lwt.t
val maybe_write : (Lvm.Vg.metadata -> Lvm.Redo.Op.t list Lvm.Vg.result) -> unit Lwt.t
val myvg : vg Lwt.t
val sector_size : int Lwt.t
