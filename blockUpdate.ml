open Sexplib.Std

type lv = string with sexp
type t = {
  fromLV: lv;
  toLV: lv;
  target: Devmapper.Target.t; (* Remove from fromLV, add to toLV *)
} with sexp

let of_cstruct x =
  Cstruct.to_string x |> Sexplib.Sexp.of_string |> t_of_sexp
let to_cstruct t =
  let s = sexp_of_t t |> Sexplib.Sexp.to_string in
  let c = Cstruct.create (String.length s) in
  Cstruct.blit_from_string s 0 c 0 (Cstruct.len c);
  c
