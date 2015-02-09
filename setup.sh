# Making a 1G disk on /dev/loop0
rm -f bigdisk
dd if=/dev/zero of=bigdisk bs=1 seek=1G count=0
losetup /dev/loop0 bigdisk
pvcreate --metadatasize=10M /dev/loop0
vgcreate djstest /dev/loop0
# Volume djstest-free will contain the free blocks
lvcreate -L 4M djstest -n free
# Volume djstest-live is the guest LV
lvcreate -L 4M djstest -n live

# create journals: one for a slave, one for a master
lvcreate -L 1M djstest -n masterJournal
dd if=/dev/zero of=localJournal bs=1M count=1
# Clear the local free block list
rm -f djstest-free

# create the to/FromLVM rings for one host
lvcreate -L 1M djstest -n toLVM
lvcreate -L 1M djstest -n fromLVM

echo Run 'sudo ./local-allocator.native' and type 'djstest-live' to request an allocation
echo Run 'sudo ./remote-allocator.native' to see the LVM updates being picked up
echo Run './cleanup.sh' to remove all volumes and devices
