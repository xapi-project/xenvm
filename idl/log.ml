open Sexplib.Std

type traced_operation = [
  | `Set of string * string * [ `Producer | `Consumer | `Suspend | `Suspend_ack ] * [ `Int64 of int64 | `Bool of bool ]
  | `Get of string * string * [ `Producer | `Consumer | `Suspend | `Suspend_ack ] * [ `Int64 of int64 | `Bool of bool ]
] with sexp
type traced_operation_list = traced_operation list with sexp

let debug fmt = Lwt_log.debug_f fmt
let info fmt = Lwt_log.info_f fmt
let warn fmt = Lwt_log.warning_f fmt
let error fmt = Lwt_log.error_f fmt

let trace ts =
  let string_of_key = function
  | `Producer -> "producer"
  | `Consumer -> "consumer"
  | `Suspend -> "suspend"
  | `Suspend_ack -> "suspend_ack" in
  let string_of_value = function
  | `Int64 x -> Int64.to_string x
  | `Bool b -> string_of_bool b in
  let one = function
  | `Set (_, queue, key, value) ->
    Printf.sprintf "%s.%s := %s" queue (string_of_key key) (string_of_value value)
  | `Get (__, queue, key, value) ->
    Printf.sprintf "%s.%s == %s" queue (string_of_key key) (string_of_value value) in
  info "%s" (String.concat ", " (List.map one ts))
