# Making a 1G disk on /dev/loop0
rm -f bigdisk
dd if=/dev/zero of=bigdisk bs=1 seek=16G count=0
losetup /dev/loop0 bigdisk
./xenvm.native format /dev/loop0 --vg djstest
./xenvmd.native --daemon

./xenvm.native create --lv live
./xenvm.native activate --lv live `pwd`/djstest-live /dev/loop0

dd if=/dev/zero of=localJournal bs=1M count=4
./xenvm.native benchmark

./xenvm.native host-create host1
./xenvm.native host-connect host1

LVS="host1-free host1-toLVM host1-fromLVM"
for i in ${LVS}; do
  echo Activating $i
  ./xenvm.native activate --lv $i `pwd`/djstest-$i /dev/loop0
done

echo Run 'sudo ./local_allocator.native' and type 'djstest-live' to request an allocation
echo Run './cleanup.sh' to remove all volumes and devices
