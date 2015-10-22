#!/bin/bash
set -ex

ARTIFACT_DIR=./.test-artifacts

XENVMD_SOCKET=$ARTIFACT_DIR/xenvmd.socket
XENVMD_CONF=$ARTIFACT_DIR/xenvmd.conf
XENVMD_LOG=$ARTIFACT_DIR/xenvmd.log

HOST1_NAME=host1
HOST1_LA_SOCKET=$ARTIFACT_DIR/local-allocator-$HOST1_NAME.socket
HOST1_LA_JOURNAL=$ARTIFACT_DIR/local-allocator-$HOST1_NAME.journal
HOST1_TOLVM_RING=$HOST1_NAME-toLVM
HOST1_FROMLVM_RING=$HOST1_NAME-fromLVM
HOST1_LA_CONF=$ARTIFACT_DIR/local-allocator-$HOST1_NAME.conf
HOST1_LA_LOG=$ARTIFACT_DIR/local-allocator-$HOST1_NAME.log

XENVM_CONFDIR=$ARTIFACT_DIR/xenvm.d
XENVM_ARGS="--configdir $XENVM_CONFDIR --mock-devmapper $HOST1_NAME"

MOCK_DISK=$ARTIFACT_DIR/bigdisk
VG_NAME=myvg

cleanup () {
pkill -e xenvmd || true
pkill -e local_allocator || true
rm -rf $ARTIFACT_DIR dm-mock*
}

# clean up and exit if that's all we're supposed to do
###############################################################################
cleanup
if [ $1 == "clean" ]; then exit 0; fi

# get ready for a new run
###############################################################################
mkdir -p $ARTIFACT_DIR
mkdir -p $XENVM_CONFDIR
# Making a 256G disk from a _sparse_ file
dd if=/dev/zero of=$MOCK_DISK bs=1 seek=256G count=0
./xenvm.native format $MOCK_DISK --vg $VG_NAME $XENVM_ARGS

# start xenvmd
###############################################################################
cat <<EOI > $XENVMD_CONF
(
 (listenPort ())
 (listenPath (Some "$XENVMD_SOCKET"))
 (host_allocation_quantum 128)
 (host_low_water_mark 8)
 (vg $VG_NAME)
 (devices ($MOCK_DISK))
)
EOI
./xenvmd.native --config $XENVMD_CONF > $XENVMD_LOG 2>&1 &
# when we can log to file with --log, we can use --daemon and remove the & and the sleep
sleep 2

# setup the xenvm CLI config
###############################################################################
./xenvm.native set-vg-info --pvpath $MOCK_DISK -S $XENVMD_SOCKET $VG_NAME --local-allocator-path $HOST1_LA_SOCKET --uri file://local/services/xenvmd/$VG_NAME $XENVM_ARGS

# create an LV (in a round-about way to excersize some CLI commands)
###############################################################################
./xenvm.native lvcreate -n badname -L 4 $VG_NAME $XENVM_ARGS
./xenvm.native lvrename /dev/$VG_NAME/badname /dev/$VG_NAME/live $XENVM_ARGS

./xenvm.native vgchange /dev/$VG_NAME -ay $XENVM_ARGS
./xenvm.native vgchange /dev/$VG_NAME -an $XENVM_ARGS

./xenvm.native lvchange -ay /dev/$VG_NAME/live $XENVM_ARGS

./xenvm.native lvdisplay /dev/$VG_NAME $XENVM_ARGS
./xenvm.native lvdisplay /dev/$VG_NAME -c $XENVM_ARGS
./xenvm.native lvs /dev/$VG_NAME $XENVM_ARGS
./xenvm.native pvs $MOCK_DISK $XENVM_ARGS

# simulate a host-connect (and reconnect for good measure)
###############################################################################
./xenvm.native host-create /dev/$VG_NAME $HOST1_NAME $XENVM_ARGS
./xenvm.native host-connect /dev/$VG_NAME $HOST1_NAME $XENVM_ARGS

cat <<EOI > $HOST1_LA_CONF
(
 (socket $HOST1_LA_SOCKET)
 (localJournal $HOST1_LA_JOURNAL)
 (devices ($MOCK_DISK))
 (toLVM $HOST1_TOLVM_RING)
 (fromLVM $HOST1_FROMLVM_RING)
)
EOI
./local_allocator.native --config $HOST1_LA_CONF --mock-devmapper $HOST1_NAME > $HOST1_LA_LOG 2>&1 &
# when we can log to file with --log, we can use --daemon and remove the & and the sleep
sleep 10

./xenvm.native host-disconnect /dev/$VG_NAME $HOST1_NAME $XENVM_ARGS
./xenvm.native host-connect /dev/$VG_NAME $HOST1_NAME $XENVM_ARGS
./local_allocator.native --config $HOST1_LA_CONF --mock-devmapper $HOST1_NAME > $HOST1_LA_LOG 2>&1 &
# when we can log to file with --log, we can use --daemon and remove the & and the sleep
sleep 20

# check local allocator working by extending a volume using --live
./xenvm.native lvextend /dev/$VG_NAME/live -L 128M --live $XENVM_ARGS

# all done
###############################################################################
echo "Run '$0 clean' to remove all volumes and devices"
