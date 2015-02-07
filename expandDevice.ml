open Sexplib.Std

module T = struct

  type device = string with sexp
  type targets = Devmapper.Target.t list with sexp
  type t = {
    extents: Lvm.Pv.Allocator.t; (* The physical extents in the VG *)
    device: device;              (* The destination dm device *)
    targets: targets;            (* The additional dm targets *)
  } with sexp
  (** A local allocation to a user data dm device *)

end

include SexpToCstruct.Make(T)
include T
