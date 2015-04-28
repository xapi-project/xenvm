(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt

open Lvm

let vgremove copts _ =
  () (* we don't destroy the VG *)

let vgremove_cmd =
  let doc = "remove a volume group" in
  let man = [
    `S "DESCRIPTION";
    `P "remove a volume group";
  ] in
  Term.(pure vgremove $ Xenvm_common.copts_t $ Xenvm_common.names_arg),
  Term.info "vgremove" ~sdocs:"COMMON OPTIONS" ~doc ~man
