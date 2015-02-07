open Sexplib.Std

module T = struct
  (* XXX: it should be this but LvExpand is not idempotent
  type t = Lvm.Redo.t with sexp
  *)
  type t = {
    volume: string;
    segments: Lvm.Lv.Segment.t list;
  } with sexp
  (** A local allocation to a user data LV. *)
end

include SexpToCstruct.Make(T)
include T
