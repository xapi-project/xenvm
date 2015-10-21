open Sexplib.Std

module T = struct
  type t = {
    blocks : Lvm.Pv.Allocator.t;
    generation : int
  } with sexp
  (** Physical blocks which should be included in the free pool *)
end

include SexpToCstruct.Make(T)
include T

let name = "FromLVM"
