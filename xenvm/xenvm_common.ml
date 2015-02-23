module Client = Xenvm_client.Client

type copts_t = {
  host : string option;
  port : int option;
  config : string; 
}

let set_uri uri =
  Xenvm_client.Rpc.uri := uri

let set_uri_from_copts copts =
  match copts.host, copts.port with
  | Some h, Some p -> 
    Printf.sprintf "http://%s:%d/" h p |> set_uri;
    copts
  | _, _ -> 
    failwith "Unset host"


