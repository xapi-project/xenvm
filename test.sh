set -e

eval `opam config env`
make

# Making a 1G disk
rm -f bigdisk xenvm*.out
dd if=/dev/zero of=bigdisk bs=1 seek=16G count=0

BISECT_FILE=xenvm.coverage ./xenvm.native format bigdisk --vg djstest
BISECT_FILE=xenvmd.coverage ./xenvmd.native --config ./test.xenvmd.conf &

export BISECT_FILE=xenvm.coverage

./xenvm.native create --lv live
./xenvm.native benchmark

./xenvm.native shutdown
bisect-report xenvm*.out -summary-only -html /vagrant/report/
`opam config var bin`/ocveralls xenvm*.out --send
