#!/bin/sh

set -e

echo Setting up allocator
./setup.sh &

sleep 5

echo Running the local allocator
./local_allocator.native &

sleep 5

echo Requesting an allocation
echo djstest-live | nc localhost 8081

sleep 5

echo Shutting everything down
./xenvm.native shutdown

sleep 5

cd _build
echo Code coverage summary
bisect-report ../*.out -summary-only -text -
