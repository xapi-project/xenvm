set -ex

if [ "$EUID" -ne "0" ]; then
  echo "Please run me as uid 0. I need to create a loop device and device mapper devices"
  exit 1
fi

./xenvm.native lvchange -an /dev/djstest/live || true
#./xenvm.native shutdown /dev/djstest
killall xenvmd.native
dmsetup remove_all
dd if=/dev/zero of=/dev/loop0 bs=1M count=128
losetup -d /dev/loop0
rm -f localJournal bigdisk *.out
