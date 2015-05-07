open Sexplib.Std

module T = struct
  type t =
    | Device_mapper_device_does_not_exist of string
    | Request_for_no_segments of int64
    | Success
  with sexp
  (** Response from xenvm-local-allocator to xenvm *)
end

include SexpToCstruct.Make(T)
include T
