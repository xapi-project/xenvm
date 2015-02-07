open Sexplib.Std

module T = struct

  type lv = string with sexp
  type targets = Devmapper.Target.t list with sexp
  type t = {
    extents: Lvm.Pv.Allocator.t; (* The physical extents in the VG *)
    toLV: lv;                    (* The destination LV *)
    targets: targets;            (* The resulting targets *)
  } with sexp
  (** A local allocation from a freeLV to a user data LV. *)

end

include SexpToCstruct.Make(T)
include T
