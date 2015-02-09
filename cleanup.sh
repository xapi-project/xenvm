dmsetup remove djstest-live
dmsetup remove djstest-free
dmsetup remove djstest-masterJournal
dmsetup remove djstest-toLVM
dmsetup remove djstest-fromLVM
dd if=/dev/zero of=/dev/loop0 bs=1M count=128
lvremove -f djstest/free
lvremove -f djstest/live
vgremove -f djstest
losetup -d /dev/loop0
