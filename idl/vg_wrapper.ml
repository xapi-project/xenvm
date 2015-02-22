type t = Lvm.Vg.metadata

let rpc_of_t vg = Rpc.rpc_of_string (Sexplib.Sexp.to_string (Lvm.Vg.sexp_of_metadata vg))
let t_of_rpc rpc = Lvm.Vg.metadata_of_sexp (Sexplib.Sexp.of_string (Rpc.string_of_rpc rpc))
