./xenvm.native shutdown
killall xenvmd.native
LVS="live"
for i in ${LVS}; do
  echo Removing $i
  rm -f ./djstest-$i
done
dmsetup remove_all
dd if=/dev/zero of=/dev/loop0 bs=1M count=128
losetup -d /dev/loop0
rm -f localJournal
