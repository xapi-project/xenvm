LVS="free live masterJournal toLVM fromLVM LVMjournal"
for i in ${LVS}; do
  echo Removing $i
  rm -f ./djstest-$i
done
./xenvm.native shutdown
dmsetup remove_all
dd if=/dev/zero of=/dev/loop0 bs=1M count=128
killall xenvmd.native
losetup -d /dev/loop0
