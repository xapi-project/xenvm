lvremove -f djstest/free
lvremove -f djstest/live
vgremove -f djstest
losetup -d /dev/loop0
