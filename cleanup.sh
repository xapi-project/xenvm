set -ex

if [ "$EUID" -ne "0" ]; then
  echo "I am not running with EUID 0. I will use the mock device mapper interface"
  USE_MOCK=1
  MOCK_ARG="--mock-devmapper"
else
  echo "I am running with EUID 0. I will use the real device mapper interface"
  USE_MOCK=0
  MOCK_ARG=""
fi

./xenvm.native lvchange -an /dev/djstest/live || true
#./xenvm.native shutdown /dev/djstest
killall xenvmd.native || echo No killable xenvmd
killall local_allocator.native || echo No killable local allocators

if [ "$USE_MOCK" -eq "0" ]; then
  dmsetup remove_all
  LOOP=$(losetup -j bigdisk | cut -f 1 -d ':')
  dd if=/dev/zero of=$LOOP bs=1M count=128
  losetup -d $LOOP
fi

rm -f localJournal bigdisk *.out*  djstest-* dm-mock
