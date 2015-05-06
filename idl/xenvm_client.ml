open Cohttp_lwt_unix

let _ =
  let service svc =
    match svc with
    | "file" -> Lwt.return (Some {Resolver.name="file"; port=0; tls=false})
    | _ -> Resolver_lwt_unix.system_service svc
  in
  Resolver_lwt.set_service ~f:service Resolver_lwt_unix.system;
  Resolver_lwt.add_rewrite ~host:"local" ~f:(fun svc uri ->
      match svc.Resolver.name with
      | "file" -> Lwt.return (`Unix_domain_socket (Uri.path uri) : Conduit.endp)
      | _ -> Resolver_lwt_unix.system_resolver svc uri) Resolver_lwt_unix.system

module Rpc = struct
  include Lwt

  let uri = ref ""

  (* Retry up to 5 times with 1s intervals *)
  let rpc call =
    let body = Jsonrpc.string_of_call call |> Cohttp_lwt_body.of_string in
    let rec retry attempts_remaining last_exn = match attempts_remaining, last_exn with
    | 0, Some e -> fail e
    | _, _ ->
      begin
        Lwt.catch
          (fun () ->
            Client.post (Uri.of_string !uri) ~body
            >>= fun x ->
            return (`Ok x))
          (function
           | Unix.Unix_error(Unix.ECONNREFUSED, _, _) as e ->
             return (`Retry e)
           | e ->
           return (`Error e))
        >>= function
        | `Ok (resp, body) ->
          Cohttp_lwt_body.to_string body >>= fun body ->
          return (Jsonrpc.response_of_string body)
        | `Retry e ->
          Lwt_unix.sleep 1.
          >>= fun () ->
          retry (max 0 (attempts_remaining - 1)) (Some e)
        | `Error e ->
          fail e
      end in
    retry 5 None
end

module Client = Xenvm_interface.ClientM(Rpc)
