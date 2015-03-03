# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "chef/centos-7.0"
  config.vm.provider "xenserver" do |v, override|
    override.vm.box = "jonludlam/xs-centos-7"
  end

  config.vm.provision "shell", path: "scripts/install_opam.sh", privileged: false
end
