module ToLVM :
sig
  type consumer
  type item = ExpandVolume.t
  type cposition
  val create : disk:Vg_io.Vg_IO.Volume.t -> unit -> unit Lwt.t
  val attach :
    name:string -> disk:Vg_io.Vg_IO.Volume.t -> unit -> consumer Lwt.t
  val state : consumer -> [ `Running | `Suspended ] Lwt.t
  val debug_info : consumer -> (string * string) list Lwt.t
  val suspend : consumer -> unit Lwt.t
  val resume : consumer -> unit Lwt.t
  val pop : consumer -> (cposition * item list) Lwt.t
  val advance : consumer -> cposition -> unit Lwt.t
end
module FromLVM :
sig
  type producer
  type item = FreeAllocation.t
  type pposition
  val create : disk:Vg_io.Vg_IO.Volume.t -> unit -> unit Lwt.t
  val attach :
    name:string ->
    disk:Vg_io.Vg_IO.Volume.t ->
    unit -> ([> `Running | `Suspended ] * producer) Lwt.t
  val state : producer -> [ `Running | `Suspended ] Lwt.t
  val debug_info : producer -> (string * string) list Lwt.t
  val push : producer -> item -> pposition Lwt.t
  val advance : producer -> pposition -> unit Lwt.t
end
