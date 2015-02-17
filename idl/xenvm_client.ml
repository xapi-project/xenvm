open Cohttp_lwt_unix

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
          let status = Response.status resp in
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
