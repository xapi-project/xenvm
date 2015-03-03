#!/bin/bash

sudo yum clean all
sudo yum update -y

cd /etc/yum.repos.d/
sudo wget http://download.opensuse.org/repositories/home:ocaml/CentOS_7/home:ocaml.repo
sudo yum install -y opam

sudo yum install -y opam m4 gcc patch aspcud git device-mapper-libs libffi-devel device-mapper-devel

opam init -a --comp=4.02.1
eval `opam config env`

# There appears to be a dependency problem. If we don't preinstall lwt, mirage-types gets compiled
# without lwt support, even though it's mentioned as a depext
opam install lwt

opam remote add thinlvhd git://github.com/xapi-project/thin-lvhd-opam-repo
opam install -y thin-lvhd-tools

