set -e

eval `opam config env`
#opam pin add ocveralls git://github.com/djs55/ocveralls#fix-vector-combine -y
make

# Making a 1G disk
rm -f bigdisk _build/xenvm*.out
dd if=/dev/zero of=bigdisk bs=1 seek=256G count=0
cat test.xenvmd.conf.in | sed -r "s|@BIGDISK@|`pwd`/bigdisk|g" > test.xenvmd.conf

BISECT_FILE=_build/xenvm.coverage ./xenvm.native format bigdisk --vg djstest
BISECT_FILE=_build/xenvmd.coverage ./xenvmd.native --config ./test.xenvmd.conf --daemon

export BISECT_FILE=_build/xenvm.coverage

./xenvm.native set-vg-info --pvpath ./bigdisk -S /tmp/xenvmd djstest --local-allocator-path /tmp/xenvm-local-allocator --uri file://local/services/xenvmd/djstest

./xenvm.native benchmark /dev/djstest

./xenvm.native shutdown /dev/djstest

wait $(pidof xenvmd.native)
echo Generating bisect report-- this fails on travis
(cd _build; bisect-report xenvm*.out -summary-only -html /vagrant/report/ || echo Ignoring bisect-report failure)
echo Sending to coveralls-- this only works on travis
`opam config var bin`/ocveralls --prefix _build _build/xenvm*.out --send || echo "Failed to upload to coveralls"
