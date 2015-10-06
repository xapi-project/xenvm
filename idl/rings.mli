module ToLVM: S.XENVMRING
  with type item = ExpandVolume.t

module FromLVM: S.XENVMRING
  with type item = FreeAllocation.t
