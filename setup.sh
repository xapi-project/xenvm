set -e
# Making a 1G disk on /dev/loop0
rm -f bigdisk
dd if=/dev/zero of=bigdisk bs=1 seek=16G count=0
losetup /dev/loop0 bigdisk
BISECT_FILE=xenvm.coverage ./xenvm.native format /dev/loop0 --vg djstest
BISECT_FILE=xenvmd.coverage ./xenvmd.native &

export BISECT_FILE=xenvm.coverage

./xenvm.native create --lv live
./xenvm.native activate --lv live `pwd`/djstest-live /dev/loop0

./xenvm.native benchmark

./xenvm.native host-create host1
./xenvm.native host-connect host1

echo Run 'sudo ./local_allocator.native' and type 'djstest-live' to request an allocation
echo Run './cleanup.sh' to remove all volumes and devices
