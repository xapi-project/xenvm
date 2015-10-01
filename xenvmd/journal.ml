open Sexplib.Std
open Vg_io

module Op = struct
  module T = struct
    type host = string with sexp
    type ef = {
      host : string;
      old_allocation : Lvm.Pv.Allocator.t;
      extra_size : int64;
    } with sexp
    type t =
      | ExpandFree of ef
      (* Assign a block allocation to a host *)
    with sexp
  end
  
  include SexpToCstruct.Make(T)
  include T
end

module J = Shared_block.Journal.Make(Log)(Vg_IO.Volume)(Time)(Clock)(Op)
