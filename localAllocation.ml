open Sexplib.Std

module T = struct

  type lv = string with sexp
  type targets = Devmapper.Target.t list with sexp
  type t = {
    fromLV: lv;
    toLV: lv;
    targets: targets; (* Remove from fromLV, add to toLV *)
  } with sexp
  (** A local allocation from a freeLV to a user data LV. *)

end

include SexpToCstruct.Make(T)
include T
