set -e

eval `opam config env`
make

# Making a 1G disk
rm -f bigdisk _build/xenvm*.out
dd if=/dev/zero of=bigdisk bs=1 seek=16G count=0

BISECT_FILE=_build/xenvm.coverage ./xenvm.native format bigdisk --vg djstest
BISECT_FILE=_build/xenvmd.coverage ./xenvmd.native --config ./test.xenvmd.conf &

export BISECT_FILE=_build/xenvm.coverage

./xenvm.native create --lv live
./xenvm.native benchmark

./xenvm.native shutdown
wait $(pidof xenvmd.native)
echo Generating bisect report-- this fails on travis
(cd _build; bisect-report xenvm*.out -summary-only -html /vagrant/report/ || echo Ignoring bisect-report failure)
echo Sending to coveralls-- this only works on travis
`opam config var bin`/ocveralls --prefix _build _build/xenvm*.out --send || echo "Failed to upload to coveralls"
