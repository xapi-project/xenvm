#!/bin/bash

sudo apt-get clean
sudo apt-get update
sudo apt-get install -y software-properties-common

sudo add-apt-repository ppa:avsm/ppa
sudo apt-get update
sudo apt-get install -y ocaml opam m4 libdevmapper-dev libffi-dev

opam init -a
eval `opam config env`

opam switch 4.02.1
eval `opam config env`

opam install -y lwt camldm cohttp ounit oasis

git clone git://github.com/mirage/shared-block-ring
opam pin add -y shared-block-ring shared-block-ring

git clone git://github.com/xapi-project/camldm
opam pin add -y camldm camldm

git clone git://github.com/mirage/mirage-block-volume
opam pin add -y mirage-block-volume mirage-block-volume



