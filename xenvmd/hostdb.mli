type connected_host = {
  mutable state : Xenvm_interface.connection_state;
  to_LVM : Rings.ToLVM.consumer;
  from_LVM : Rings.FromLVM.producer;
  free_LV : string;
  free_LV_uuid : Lvm.Vg.LVs.key;
}
type state = Disconnected | Connecting | Connected of connected_host

val get : string -> state
val set : string -> state -> unit
val all : unit -> (string * state) list
    
