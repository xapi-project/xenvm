open Sexplib.Std

module T = struct
  type t = {
    local_dm_name: string;
    action: [ `IncreaseBy of int64 | `Absolute of int64 ];
  } with sexp
  (** Request from xenvm to xenvm-local-allocator *)
end

include SexpToCstruct.Make(T)
include T
