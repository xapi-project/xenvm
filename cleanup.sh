dmsetup remove djstest-live
dmsetup remove djstest-free
dmsetup remove djstest-masterJournal
dmsetup remove djstest-toLVM
dmsetup remove djstest-fromLVM
dd if=/dev/zero of=/dev/loop0 bs=1M count=128
./xenvm.native shutdown
killall xenvmd.native
losetup -d /dev/loop0
