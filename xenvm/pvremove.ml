(* LVM compatible bits and pieces *)

open Cmdliner
open Xenvm_common
open Lwt

open Lvm
module Pv_IO = Pv.Make(Block)

let pvremove copts undo filenames =
  let open Xenvm_common in
  Lwt_main.run (
    IO.FromResult.all (
      Lwt_list.map_s
        (fun filename ->
          with_block filename
            (fun device ->
              (if undo then Pv_IO.unwipe else Pv_IO.wipe) device
            )
        ) filenames
    ) >>= function
    | `Error (`Msg m) -> failwith m
    | `Ok _ -> Lwt.return ()
  )

let undo =
  let doc = "Attempt to unwipe a previously-wiped volume" in
  Arg.(value & flag & info ["undo"] ~doc)

let pvremove_cmd =
  let doc = "destroy physical volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "pvremove wipes the data from physical volumes";
  ] in
  Term.(pure pvremove $ Xenvm_common.copts_t $ undo $ Xenvm_common.devices_arg),
  Term.info "pvremove" ~sdocs:"COMMON OPTIONS" ~doc ~man
