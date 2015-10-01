open Sexplib.Std
open Vg_io

module Op = struct
  module T = struct
    type host = string with sexp
    type fa = {
      host : string;
      old_allocation : FreeAllocation.t;
      extra_size : int64;
    } with sexp
    type t =
      | FreeAllocation of fa
      (* Assign a block allocation to a host *)
    with sexp
  end
  
  include SexpToCstruct.Make(T)
  include T
end

module J = Shared_block.Journal.Make(Log)(Vg_IO.Volume)(Time)(Clock)(Op)
