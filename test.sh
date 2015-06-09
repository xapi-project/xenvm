set -e
set -x

if [ "$EUID" -ne "0" ]; then
  echo "I am not running with EUID 0. I will use the mock device mapper interface"
  USE_MOCK=1
  MOCK_ARG="--mock-devmapper"
else
  echo "I am running with EUID 0. I will use the real device mapper interface"
  USE_MOCK=0
  MOCK_ARG=""
fi

eval `opam config env`
#opam pin add ocveralls git://github.com/djs55/ocveralls#fix-vector-combine -y
make

# Making a 8G disk
rm -f bigdisk _build/xenvm*.out chengzDisk
dd if=/dev/zero of=bigdisk bs=1 seek=8G count=0
dd if=/dev/zero of=chengzDisk bs=1 seek=8G count=0

if [ "$USE_MOCK" -eq "0" ]; then
  LOOP=$(losetup -f)
  echo Using $LOOP
  losetup $LOOP bigdisk
  CHENGZLOOP=$(losetup -f)
  losetup $CHENGZLOOP chengzDisk
  echo Using $chengzDisk
else
  LOOP=`pwd`/bigdisk
  CHENGZLOOP=`pwd`/chengzDisk
fi

cat test.xenvmd.conf.in | sed -r "s|@BIGDISK@|$LOOP|g" > test.xenvmd.conf
mkdir -p /etc/xenvm.d
BISECT_FILE=_build/xenvm.coverage ./xenvm.native format $LOOP --vg djstest  $MOCK_ARG
BISECT_FILE=_build/xenvmd.coverage ./xenvmd.native --config ./test.xenvmd.conf --daemon
export BISECT_FILE=_build/xenvm.coverage

./xenvm.native set-vg-info --pvpath $LOOP -S /tmp/xenvmd djstest --local-allocator-path /tmp/xenvm-local-allocator --uri file://local/services/xenvmd/djstest $MOCK_ARG

#./xenvm.native benchmark /dev/djstest $MOCK_ARG

./xenvm.native vgcreate chengztest $CHENGZLOOP
./xenvm.native format $CHENGZLOOP --vg chengztest
./xenvm.native vgs djstest
#./xenvm.native vgs chengztest   ###failed with Need to know the local device!
./xenvm.native vgremove chengztest
./xenvm.native vgremove chengztestsasa


./xenvm.native lvcreate -n live -L 2G djstest $MOCK_ARG

./xenvm.native lvdisplay /dev/djstest/live
lvsize=`./xenvm.native lvdisplay /dev/djstest/live $MOCK_ARG | grep 'LV Size'| awk '{print $3}'`
if [ "$lvsize"x = "4194304sx" ]; then echo "Pass lvcreate Test"; else echo "Failed lvcreate Test"; fi

./xenvm.native lvchange -ay /dev/djstest/live $MOCK_ARG


./xenvm.native lvextend -L 5G /dev/djstest/live $MOCK_ARG
lvsize=`./xenvm.native lvdisplay /dev/djstest/live $MOCK_ARG | grep 'LV Size'| awk '{print $3}'`
if [ "$lvsize"x = "10485760s"x ]; then echo "Pass lvextend Test"; else echo "Failed lvextend Test"; fi

# create and connect to hosts
./xenvm.native host-create /dev/djstest host1 $MOCK_ARG

./xenvm.native host-connect /dev/djstest host1 $MOCK_ARG

cat test.local_allocator.conf.in | sed -r "s|@BIGDISK@|$LOOP|g"  | sed -r "s|@HOST@|host1|g" > test.local_allocator.host1.conf
./local_allocator.native --config ./test.local_allocator.host1.conf $MOCK_ARG > /dev/null &

./xenvm.native host-create /dev/djstest host2 $MOCK_ARG
./xenvm.native host-connect /dev/djstest host2 $MOCK_ARG
cat test.local_allocator.conf.in | sed -r "s|@BIGDISK@|$LOOP|g"  | sed -r "s|@HOST@|host2|g" > test.local_allocator.host2.conf
./local_allocator.native --config ./test.local_allocator.host2.conf $MOCK_ARG > /dev/null &

sleep 30
./xenvm.native host-list /dev/djstest $MOCK_ARG

# destroy hosts
./xenvm.native host-disconnect /dev/djstest host2 $MOCK_ARG
./xenvm.native host-destroy /dev/djstest host2 $MOCK_ARG
./xenvm.native host-disconnect /dev/djstest host1 $MOCK_ARG
./xenvm.native host-destroy /dev/djstest host1 $MOCK_ARG

./xenvm.native host-list /dev/djstest $MOCK_ARG

xenvmdpid=`pidof xenvmd.native`
#shutdown
./xenvm.native lvchange -an /dev/djstest/live $MOCK_ARG
./xenvm.native shutdown /dev/djstest $MOCK_ARG


while [ -d /proc/$xenvmdpid ];
do
  sleep 1;
done

echo Generating bisect report-- this fails on travis
(cd _build; bisect-report xenvm*.out -summary-only -html /vagrant/report/ || echo Ignoring bisect-report failure)
echo Sending to coveralls-- this only works on travis
`opam config var bin`/ocveralls --prefix _build _build/xenvm*.out --send || echo "Failed to upload to coveralls"

if [ "$USE_MOCK" -eq "0" ]; then
  dmsetup remove_all
  dd if=/dev/zero of=$LOOP bs=1M count=128
  losetup -d $LOOP
  dd if=/dev/zero of=$CHENGZLOOP bs=1M count=128
  losetup -d $CHENGZLOOP
fi

rm -f localJournal bigdisk *.out chengzDisk
