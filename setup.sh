set -e
set -x

# There is an important bugfix in the master branch. If you see
# ECONNREFUSED you might need this:
# opam pin add conduit git://github.com/mirage/ocaml-conduit -y
# If you are using a XenServer build environment then the patch is
# already included in the RPM.

if [ "$EUID" -ne "0" ]; then
  echo "I am not running with EUID 0. I will use the mock device mapper interface"
  USE_MOCK=1
  MOCK_ARG="--mock-devmapper"
else
  echo "I am running with EUID 0. I will use the real device mapper interface"
  USE_MOCK=0
  MOCK_ARG=""
fi

# Making a 1G disk
rm -f bigdisk
dd if=/dev/zero of=bigdisk bs=1 seek=256G count=0

if [ "$USE_MOCK" -eq "0" ]; then
  LOOP=$(losetup -f)
  echo Using $LOOP
  losetup $LOOP bigdisk
else
  LOOP=`pwd`/bigdisk
fi
cat test.xenvmd.conf.in | sed -r "s|@BIGDISK@|$LOOP|g" > test.xenvmd.conf
mkdir -p /tmp/xenvm.d
./xenvm.native format $LOOP --vg djstest --configdir /tmp/xenvm.d $MOCK_ARG
./xenvmd.native --config ./test.xenvmd.conf --daemon

./xenvm.native set-vg-info --pvpath $LOOP -S /tmp/xenvmd djstest --local-allocator-path /tmp/host1-socket --uri file://local/services/xenvmd/djstest --configdir /tmp/xenvm.d $MOCK_ARG

./xenvm.native lvcreate -n badname -L 4 djstest --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native lvrename /dev/djstest/badname /dev/djstest/live --configdir /tmp/xenvm.d $MOCK_ARG

./xenvm.native vgchange /dev/djstest -ay --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native vgchange /dev/djstest -an --configdir /tmp/xenvm.d $MOCK_ARG

./xenvm.native lvchange -ay /dev/djstest/live --configdir /tmp/xenvm.d $MOCK_ARG

./xenvm.native lvdisplay /dev/djstest --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native lvdisplay /dev/djstest -c --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native lvs /dev/djstest --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native pvs ./bigdisk --configdir /tmp/xenvm.d $MOCK_ARG

#./xenvm.native benchmark
# create and connect to hosts
./xenvm.native host-create /dev/djstest host1 --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native host-connect /dev/djstest host1 --configdir /tmp/xenvm.d $MOCK_ARG
cat test.local_allocator.conf.in | sed -r "s|@BIGDISK@|$LOOP|g"  | sed -r "s|@HOST@|host1|g" > test.local_allocator.host1.conf
./local_allocator.native --daemon --config ./test.local_allocator.host1.conf $MOCK_ARG

sleep 30 # the local allocator daemonizes too soon

./xenvm.native lvextend /dev/djstest/live -L 128M --live --configdir /tmp/xenvm.d $MOCK_ARG

./xenvm.native host-create /dev/djstest host2 --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native host-connect /dev/djstest host2 --configdir /tmp/xenvm.d $MOCK_ARG
cat test.local_allocator.conf.in | sed -r "s|@BIGDISK@|$LOOP|g"  | sed -r "s|@HOST@|host2|g" > test.local_allocator.host2.conf
./local_allocator.native --config ./test.local_allocator.host2.conf $MOCK_ARG > local_allocator.host2.log &

sleep 30
./xenvm.native host-list /dev/djstest --configdir /tmp/xenvm.d $MOCK_ARG

# destroy hosts
./xenvm.native host-disconnect /dev/djstest host2 --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native host-destroy /dev/djstest host2 --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native host-disconnect /dev/djstest host1 --configdir /tmp/xenvm.d $MOCK_ARG
./xenvm.native host-destroy /dev/djstest host1 --configdir /tmp/xenvm.d $MOCK_ARG

./xenvm.native host-list /dev/djstest --configdir /tmp/xenvm.d $MOCK_ARG

#shutdown
./xenvm.native lvchange -an /dev/djstest/live --configdir /tmp/xenvm.d $MOCK_ARG || true
./xenvm.native shutdown /dev/djstest --configdir /tmp/xenvm.d $MOCK_ARG

#echo Run 'sudo ./xenvm.native host-connect /dev/djstest host1' to connect to the local allocator'
#echo Run 'sudo ./local_allocator.native' and type 'djstest-live' to request an allocation
echo Run './cleanup.sh' to remove all volumes and devices
