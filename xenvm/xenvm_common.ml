open Sexplib.Std
open Cmdliner
  
module Client = Xenvm_client.Client

type copts_t = {
  uri_override : string option; (* CLI set URI override *)
  config : string; 
}

let make_copts config uri_override = {uri_override; config}

let config =
  let doc = "Path to the config directory" in
  Arg.(value & opt dir "/etc/xenvm.d" & info [ "configdir" ] ~docv:"CONFIGDIR" ~doc)

let uri_arg =
  let doc = "Overrides the URI of the XenVM daemon in charge of the volume group." in
  Arg.(value & opt (some string) None & info ["u"; "uri"] ~docv:"URI" ~doc)

let copts_t =
  Term.(pure make_copts $ config $ uri_arg)




type vg_info_t = {
  uri : string;
  local_device : string;
} with sexp

let set_vg_info_t copts uri local_device vg_name =
  let info = {uri; local_device} in
  let filename = Filename.concat copts.config vg_name in
  let s = sexp_of_vg_info_t info |> Sexplib.Sexp.to_string in
  Lwt_io.with_file ~mode:Lwt_io.Output filename (fun f ->
    Lwt_io.fprintf f "%s" s)

let get_vg_info_t copts vg_name =
  let open Lwt in
  let lift f = fun x -> Lwt.return (f x) in
  let filename = Filename.concat copts.config vg_name in
  Lwt.catch (fun () ->
    Lwt_io.with_file ~mode:Lwt_io.Input filename Lwt_io.read >>=
    lift Sexplib.Sexp.of_string >>=
    lift vg_info_t_of_sexp >>=
    lift (fun s -> Some s))
    (fun e -> Lwt.return None)




let set_uri copts vg_info_opt =
  let uri = 
    match copts.uri_override with
    | Some uri -> uri
    | None ->
      match vg_info_opt with
      | Some info -> info.uri
      | None -> "http://127.0.0.1:4000/"
  in
  Xenvm_client.Rpc.uri := uri



