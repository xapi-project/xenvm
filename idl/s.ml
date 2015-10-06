module type CSTRUCTABLE = sig
  type t
  (** Something that can be read from and written to a Cstruct.t *)

  val to_cstruct: t -> Cstruct.t
  val of_cstruct: Cstruct.t -> t option
  val name: string
end

module type XENVMRING =
sig
  type item
  type consumer
  type cposition
  type producer
  type pposition
  val create : disk:Vg_io.Volume.t -> unit -> unit Lwt.t
  val attach_as_producer :
    name:string ->
    disk:Vg_io.Volume.t ->
    unit -> ([ `Running | `Suspended ] * producer) Lwt.t
  val attach_as_consumer :
    name:string -> disk:Vg_io.Volume.t -> unit -> consumer Lwt.t
  val p_state : producer -> [ `Running | `Suspended ] Lwt.t
  val c_state : consumer -> [ `Running | `Suspended ] Lwt.t
  val c_debug_info : consumer -> (string * string) list Lwt.t
  val p_debug_info : producer -> (string * string) list Lwt.t
  val suspend : consumer -> unit Lwt.t
  val resume : consumer -> unit Lwt.t
  val push : producer -> item -> pposition Lwt.t
  val pop : consumer -> (cposition * item list) Lwt.t
  val p_advance : producer -> pposition -> unit Lwt.t
  val c_advance : consumer -> cposition -> unit Lwt.t
end

