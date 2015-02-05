type t = Lvm.Vg.t

let rpc_of_t vg = Rpc.rpc_of_string (Sexplib.Sexp.to_string (Lvm.Vg.sexp_of_t vg))
let t_of_rpc rpc = Lvm.Vg.t_of_sexp (Sexplib.Sexp.of_string (Rpc.string_of_rpc rpc))
