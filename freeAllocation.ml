open Sexplib.Std

module Type = struct
  type t = Lvm.Pv.Allocator.t with sexp
  (** Physical blocks which should be included in the free pool *)
end

include SexpToCstruct.Make(Type)
include Type
