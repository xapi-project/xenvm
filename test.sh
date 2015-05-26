set -e
set -x

eval `opam config env`
#opam pin add ocveralls git://github.com/djs55/ocveralls#fix-vector-combine -y
make

# Making a 8G disk
rm -f bigdisk _build/xenvm*.out
dd if=/dev/zero of=bigdisk bs=1 seek=8G count=0

LOOP=$(losetup -f)
echo Using $LOOP
losetup $LOOP bigdisk
cat test.xenvmd.conf.in | sed -r "s|@BIGDISK@|$LOOP|g" > test.xenvmd.conf
mkdir -p /etc/xenvm.d
BISECT_FILE=_build/xenvm.coverage ./xenvm.native format $LOOP --vg djstest
BISECT_FILE=_build/xenvmd.coverage ./xenvmd.native --config ./test.xenvmd.conf --daemon

export BISECT_FILE=_build/xenvm.coverage

./xenvm.native set-vg-info --pvpath $LOOP -S /tmp/xenvmd djstest --local-allocator-path /tmp/xenvm-local-allocator --uri file://local/services/xenvmd/djstest

#./xenvm.native benchmark /dev/djstest

./xenvm.native lvcreate -n live -L 4 djstest
lvsize=`./xenvm.native lvdisplay /dev/djstest/live | grep 'LV Size'| awk '{print $3}'`
if [ "$lvsize"x = "8192s"x ]; then echo "Pass lvcreate Test"; else echo "Failed lvcreate Test"; fi

./xenvm.native lvchange -ay /dev/djstest/live

./xenvm.native lvextend -L 5G /dev/djstest/live
lvsize=`./xenvm.native lvdisplay /dev/djstest/live | grep 'LV Size'| awk '{print $3}'`
if [ "$lvsize"x = "10485760s"x ]; then echo "Pass lvextend Test"; else echo "Failed lvextend Test"; fi

# create and connect to hosts
./xenvm.native host-create /dev/djstest host1
./xenvm.native host-connect /dev/djstest host1
cat test.local_allocator.conf.in | sed -r "s|@BIGDISK@|$LOOP|g"  | sed -r "s|@HOST@|host1|g" > test.local_allocator.host1.conf
./local_allocator.native --config ./test.local_allocator.host1.conf > /dev/null &

./xenvm.native host-create /dev/djstest host2
./xenvm.native host-connect /dev/djstest host2
cat test.local_allocator.conf.in | sed -r "s|@BIGDISK@|$LOOP|g"  | sed -r "s|@HOST@|host2|g" > test.local_allocator.host2.conf
./local_allocator.native --config ./test.local_allocator.host2.conf > /dev/null &

sleep 30
./xenvm.native host-list /dev/djstest

# destroy hosts
./xenvm.native host-disconnect /dev/djstest host2
./xenvm.native host-destroy /dev/djstest host2
./xenvm.native host-disconnect /dev/djstest host1
./xenvm.native host-destroy /dev/djstest host1

./xenvm.native host-list /dev/djstest

#shutdown
./xenvm.native lvchange -an /dev/djstest/live
./xenvm.native shutdown /dev/djstest


wait $(pidof xenvmd.native)
echo Generating bisect report-- this fails on travis
(cd _build; bisect-report xenvm*.out -summary-only -html /vagrant/report/ || echo Ignoring bisect-report failure)
echo Sending to coveralls-- this only works on travis
`opam config var bin`/ocveralls --prefix _build _build/xenvm*.out --send || echo "Failed to upload to coveralls"

dmsetup remove_all
dd if=/dev/zero of=$LOOP bs=1M count=128
losetup -d $LOOP
rm -f localJournal bigdisk *.out