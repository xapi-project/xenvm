open Cohttp_lwt_unix

let unix_domain_socket_path = ref "/var/run/xenvm.sock"

let _ =
  let service svc =
    match svc with
    | "file" -> Lwt.return (Some {Resolver.name="file"; port=0; tls=false})
    | _ -> Resolver_lwt_unix.system_service svc
  in
  Resolver_lwt.set_service ~f:service Resolver_lwt_unix.system;
  Resolver_lwt.add_rewrite ~host:"local" ~f:(fun svc uri ->
      match svc.Resolver.name with
      | "file" -> Lwt.return (`Unix_domain_socket !unix_domain_socket_path : Conduit.endp)
      | _ -> Resolver_lwt_unix.system_resolver svc uri) Resolver_lwt_unix.system

module Rpc = struct
  include Lwt

  let uri = ref ""

  (* Retry up to 5 times with 1s intervals *)
  let rpc call =
    let body_str = Jsonrpc.string_of_call call in
    let body = Cohttp_lwt_body.of_string body_str in
    let rec retry attempts_remaining last_exn = match attempts_remaining, last_exn with
    | 0, Some e -> fail e
    | _, _ ->
      begin
        Lwt.catch
          (fun () ->
            let headers = Cohttp.Header.init () in
            let headers = Cohttp.Header.add headers "content-length" (String.length body_str |> string_of_int) in
            Client.post (Uri.of_string !uri) ~headers ~chunked:false ~body
            >>= fun x ->
            return (`Ok x))
          (function
           | Unix.Unix_error(Unix.ECONNREFUSED, _, _) as e ->
             return (`Retry e)
           | e ->
           return (`Error e))
        >>= function
        | `Ok (resp, body) ->
          if Cohttp.(Code.is_success (Code.code_of_status resp.Response.status)) then
            Cohttp_lwt_body.to_string body >>= fun body ->
            return (Jsonrpc.response_of_string body)
          else
            fail (Failure "Error talking to xenvmd")
        | `Retry e ->
          let attempts_remaining = max 0 (attempts_remaining - 1) in
          (if attempts_remaining > 0 then Lwt_unix.sleep 1. else return ())
          >>= fun () ->
          retry attempts_remaining (Some e)
        | `Error e ->
          fail e
      end in
    retry 1 None
end

module Client = Xenvm_interface.ClientM(Rpc)
