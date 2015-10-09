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

module type RETRYMAPPER =
sig
  type device = string

  type info = {
    suspended: bool;
    live_table: int;
    inactive_table: int;
    open_count: int32;
    event_nr: int32;
    major: int32;
    minor: int32;
    read_only: bool;
    target_count: int32;
    deferred_remove: int;
    targets: Devmapper.Target.t list;
  } with sexp

  val create: device -> Devmapper.Target.t list -> unit Lwt.t
  (** [create device targets]: creates a device with name [device] and
   targets [targets]. This function returns before any generated udev events
   have been processed, but the client may use [mknod] to manually create
   a device node, which will be fully functional.

   It seems that the targets must be contiguous, leaving no unmapped gap
   in the source device. *)

  val remove: device -> unit Lwt.t
  (** [remove device]: remove the device mapper device with name [device] *)

  val reload: device -> Devmapper.Target.t list -> unit Lwt.t
  (** [reload device targets]: modifies the existing device [device] to
      have targets [targets]. The modifications will only take effect
      after the device is suspended and resumed.*)

  val suspend: device -> unit Lwt.t
  (** [suspend device]: suspends the device mapper device with name [device] *)

  val resume: device -> unit Lwt.t
  (** [resume device]: resumes the suspended device mapper device with
      name [device]. If the targets have been reloaded then the new values
      will take effect. *)

  val mknod: device -> string -> int -> unit Lwt.t
  (** [mknod device path mode]: creates a Unix device node for device
      [device] at [path] and with [mode] *)

  val stat: device -> info option Lwt.t
  (** [stat device] describes the device mapper [device], or returns None if
      the device doesn't exist. *)

  val ls: unit -> device list Lwt.t
  (** [ls ()] returns a list of all current devices *)

end
