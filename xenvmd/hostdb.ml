type connected_host = {
  mutable state : Xenvm_interface.connection_state;
  to_LVM : Rings.ToLVM.consumer;
  from_LVM : Rings.FromLVM.producer;
  free_LV : string;
  free_LV_uuid : Lvm.Vg.LVs.key;
}

type state =
  | Disconnected
  | Connecting
  | Connected of connected_host

let state : (string, state) Hashtbl.t = Hashtbl.create 11

let get host = try Hashtbl.find state host with _ -> Disconnected
let set host st =
  if st=Disconnected
  then Hashtbl.remove state host
  else Hashtbl.replace state host st
let all () = Hashtbl.fold (fun k v acc -> (k,v)::acc) state []
