type t = Lvm.Lv.t

let rpc_of_t x = Rpc.rpc_of_string (Sexplib.Sexp.to_string (Lvm.Lv.sexp_of_t x))
let t_of_rpc rpc = Lvm.Lv.t_of_sexp (Sexplib.Sexp.of_string (Rpc.string_of_rpc rpc))
