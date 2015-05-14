(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt

open Lvm
module Pv_IO = Pv.Make(Block)

let pvcreate copts ff y metadatasize filenames =
  let open Xenvm_common in
  Lwt_main.run (
    stderr "NOTE: pvcreate is currently a no-op"
  )

let f =
  let doc = "Force the command and override safety-checks" in
  Arg.(value & opt (some string) None & info [ "f" ] ~doc)

let y =
  let doc = "Answer yes to all questions" in
  Arg.(value & flag & info [ "y" ] ~doc)

let metadatasize =
  let doc = "Size of the metadata area" in
  Arg.(value & opt (some string) None & info [ "metadatasize" ] ~doc)

let pvcreate_cmd =
  let doc = "create physical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "This command is a no-op for backwards compatibility with traditional LVM2. Please see vgcreate.";
  ] in
  Term.(pure pvcreate $ Xenvm_common.copts_t $ f $ y $ metadatasize $ Xenvm_common.devices_arg),
  Term.info "pvcreate" ~sdocs:"COMMON OPTIONS" ~doc ~man
