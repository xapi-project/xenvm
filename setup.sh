set -e
set -x

# There is an important bugfix in the master branch. If you see
# ECONNREFUSED you might need this:
# opam pin add conduit git://github.com/mirage/ocaml-conduit -y
# If you are using a XenServer build environment then the patch is
# already included in the RPM.

if [ "$EUID" -ne "0" ]; then
  echo "Please run me as uid 0. I need to create a loop device and device mapper devices"
  exit 1
fi

# Making a 1G disk
rm -f bigdisk _build/xenvm*.out
dd if=/dev/zero of=bigdisk bs=1 seek=256G count=0

LOOP=$(losetup -f)
echo Using $LOOP
losetup $LOOP bigdisk
cat test.xenvmd.conf.in | sed -r "s|@BIGDISK@|$LOOP|g" > test.xenvmd.conf
mkdir -p /etc/xenvm.d
BISECT_FILE=_build/xenvm.coverage ./xenvm.native format $LOOP --vg djstest
BISECT_FILE=_build/xenvmd.coverage ./xenvmd.native --config ./test.xenvmd.conf --daemon

export BISECT_FILE=_build/xenvm.coverage

./xenvm.native set-vg-info --pvpath $LOOP -S /tmp/xenvmd djstest --local-allocator-path /tmp/xenvm-local-allocator --uri file://local/services/xenvmd/djstest

./xenvm.native lvcreate -n live -L 4 djstest
./xenvm.native lvchange -ay /dev/djstest/live

#./xenvm.native benchmark

# create and connect to hosts
./xenvm.native host-create /dev/djstest host1
./xenvm.native host-connect /dev/djstest host1
./xenvm.native host-create /dev/djstest host2
./xenvm.native host-connect /dev/djstest host2

./xenvm.native host-list /dev/djstest

# destroy hosts
./xenvm.native host-disconnect /dev/djstest host2
./xenvm.native host-destroy host2
./xenvm.native host-disconnect /dev/djstest host1
./xenvm.native host-destroy host1

./xenvm.native host-list /dev/djstest

#shutdown
./xenvm.native lvchange -an /dev/djstest/live || true
./xenvm.native shutdown /dev/djstest

#echo Run 'sudo ./xenvm.native host-connect /dev/djstest host1' to connect to the local allocator'
#echo Run 'sudo ./local_allocator.native' and type 'djstest-live' to request an allocation
echo Run './cleanup.sh' to remove all volumes and devices
