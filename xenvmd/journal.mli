module Op :
sig
  type ef = {    
    host : string;
    old_allocation : Lvm.Pv.Allocator.t;
    extra_size : int64;
  }
  type t = ExpandFree of ef
  val sexp_of_t : t -> Sexplib.Sexp.t
end

module J :
sig
  type t
  type error = [ `Msg of string ]
  type 'a result = ('a, error) Shared_block.Result.t
  val start :
    ?name:string ->
    ?client:string ->
    ?flush_interval:float ->
    Vg_io.Volume.t ->
    (Op.t list -> unit result Lwt.t) -> t result Lwt.t
  val shutdown : t -> unit Lwt.t
  type waiter = {
    flush : unit -> unit;
    sync : unit -> unit Lwt.t;
  }
  val push : t -> Op.t -> waiter result Lwt.t
end
