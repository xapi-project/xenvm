open Sexplib.Std

type t = Lvm.Pv.Allocator.t with sexp
(** Physical blocks which should be included in the free pool *)

let of_cstruct x =
  Cstruct.to_string x |> Sexplib.Sexp.of_string |> t_of_sexp
let to_cstruct t =
  let s = sexp_of_t t |> Sexplib.Sexp.to_string in
  let c = Cstruct.create (String.length s) in
  Cstruct.blit_from_string s 0 c 0 (Cstruct.len c);
  c
