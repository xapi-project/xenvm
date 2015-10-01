module ToLVM :
sig
  type t
  type item = ExpandVolume.t
  type position
  val create : disk:Vg_io.Vg_IO.Volume.t -> unit -> unit Lwt.t
  val attach :
    name:string -> disk:Vg_io.Vg_IO.Volume.t -> unit -> t Lwt.t
  val state : t -> [ `Running | `Suspended ] Lwt.t
  val debug_info : t -> (string * string) list Lwt.t
  val suspend : t -> unit Lwt.t
  val resume : t -> unit Lwt.t
  val pop : t -> (position * item list) Lwt.t
  val advance : t -> position -> unit Lwt.t
end
module FromLVM :
sig
  type t
  type item = FreeAllocation.t
  type position
  val create : disk:Vg_io.Vg_IO.Volume.t -> unit -> unit Lwt.t
  val attach :
    name:string ->
    disk:Vg_io.Vg_IO.Volume.t ->
    unit -> ([> `Running | `Suspended ] * t) Lwt.t
  val state : t -> [ `Running | `Suspended ] Lwt.t
  val debug_info : t -> (string * string) list Lwt.t
  val push : t -> item -> position Lwt.t
  val advance : t -> position -> unit Lwt.t
end
