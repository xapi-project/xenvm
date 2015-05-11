set -e

eval `opam config env`
#opam pin add ocveralls git://github.com/djs55/ocveralls#fix-vector-combine -y
export BISECT_FILE=_build/xenvm.coverage
# Needed to support Unix domain sockets:
sudo opam pin add conduit git://github.com/mirage/ocaml-conduit -y
sudo make test

echo Generating bisect report-- this fails on travis
(cd _build; bisect-report xenvm*.out -summary-only -html /vagrant/report/ || echo Ignoring bisect-report failure)
echo Sending to coveralls-- this only works on travis
`opam config var bin`/ocveralls --prefix _build _build/xenvm*.out --send || echo "Failed to upload to coveralls"
