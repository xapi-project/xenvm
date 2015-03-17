# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.define "dev" do |dev|
    dev.vm.box = "chef/centos-7.0"
    dev.vm.provider "xenserver" do |v, override|
      override.vm.box = "jonludlam/xs-centos-7"
    end
    dev.vm.provision "shell", path: "scripts/install_opam.sh", privileged: false
  end

  config.vm.define "test" do |test|
    test.vm.box_check_update = true
    test.vm.box = "jonludlam/xs-trunk-ring3"
    test.vm.provision "shell", inline: "hostname vg-tr3-test; echo vg-tr3-test > /etc/hostname"
  end
end
