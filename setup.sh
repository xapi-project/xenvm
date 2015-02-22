# Making a 1G disk on /dev/loop0
rm -f bigdisk
dd if=/dev/zero of=bigdisk bs=1 seek=16G count=0
losetup /dev/loop0 bigdisk
./xenvmd.native --daemon
./xenvm.native format /dev/loop0 --vg djstest
./xenvm.native open /dev/loop0
LVS="free live LVMjournal operationJournal toLVM fromLVM"
for i in ${LVS}; do
  echo Creating $i
  ./xenvm.native create --lv $i
done

for i in ${LVS}; do
  echo Activating $i
  ./xenvm.native activate --lv $i `pwd`/djstest-$i /dev/loop0
done

dd if=/dev/zero of=localJournal bs=1M count=4
rm -f djstest-LVMjournal
dd if=/dev/urandom of=djstest-LVMjournal bs=1M count=4
./xenvm.native set_redo_log --lv LVMjournal
./xenvm.native set_journal `pwd`/djstest-operationJournal
./xenvm.native benchmark

./xenvm.native register --free free --from `pwd`/djstest-fromLVM --to `pwd`/djstest-toLVM

echo Run 'sudo ./local-allocator.native' and type 'djstest-live' to request an allocation
echo Run './cleanup.sh' to remove all volumes and devices
