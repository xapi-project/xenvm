module Client = Xenvm_client.Client

type copts_t = {
  uri : string option; (* CLI set URI override *)
  config : string; 
}

let set_uri copts =
  match copts.uri with
  | Some uri -> Xenvm_client.Rpc.uri := uri
  | None -> Xenvm_client.Rpc.uri := "http://localhost:4000/"


