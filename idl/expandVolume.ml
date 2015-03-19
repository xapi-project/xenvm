open Sexplib.Std

module T = struct
  type t = {
    volume: string;
    segments: Lvm.Lv.Segment.t list;
  } with sexp
  (** A local allocation to a user data LV. *)
end

include SexpToCstruct.Make(T)
include T
