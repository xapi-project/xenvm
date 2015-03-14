thin-lvhd-tools
===============

[![Build Status](https://travis-ci.org/djs55/thin-lvhd-tools.png?branch=master)](https://travis-ci.org/djs55/thin-lvhd-tools) [![Coverage Status](https://coveralls.io/repos/djs55/thin-lvhd-tools/badge.png?branch=master)](https://coveralls.io/r/djs55/thin-lvhd-tools?branch=master)

Support tools for a thin lvhd implementation as described in
[the design doc](http://xapi-project.github.io/xapi/futures/thin-lvhd/thin-lvhd.html).

To set up a test environment, run:
```
$ sudo ./setup.sh
```

This will

1. create a sparse file to simulate a large LUN, using /dev/loop0
2. formats the LUN for "XenVM": this is like LVM only with a built-in redo-log
   and operation journalling
3. creates the metadata volumes for a single client host ("host1")
4. creates 1000 LVs as a micro-benchmark

You can then query the state of the system with:

```
$ ./xenvm.native lvs
$ ./xenvm.native host-list
```

In another terminal start the local-allocator:

```
$ sudo ./local-allocator.native
```

This will take a few seconds to complete its handshake. You can then type in the
name of a dm-device to request more space. Type in `djstest-live`: you will see it
allocate from it local thin-pool, send the update to the master and update the local
device mapper device.

To shut everything down run

```
$ sudo ./clean.sh
```

Note that a clean shutdown requires local-allocators to be online and responding to
the handshake.

