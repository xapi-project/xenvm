open Sexplib.Std

module T = struct
  type t = Lvm.Pv.Allocator.t with sexp
  (** Physical blocks which should be included in the free pool *)
end

include SexpToCstruct.Make(T)
include T
