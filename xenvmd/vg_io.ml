module Time = struct
  type 'a io = 'a Lwt.t
  let sleep = Lwt_unix.sleep
end

module Vg_IO = Lvm.Vg.Make(Log)(Block)(Time)(Clock)

