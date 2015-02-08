open Sexplib.Std

module type SEXPABLE = sig
  type t with sexp
end

module Make(S: SEXPABLE) = struct

  let of_cstruct x =
    try
      Some (Cstruct.to_string x |> Sexplib.Sexp.of_string |> S.t_of_sexp)
    with _ ->
      None

  let to_cstruct t =
    let s = S.sexp_of_t t |> Sexplib.Sexp.to_string in
    let c = Cstruct.create (String.length s) in
    Cstruct.blit_from_string s 0 c 0 (Cstruct.len c);
    c
end
