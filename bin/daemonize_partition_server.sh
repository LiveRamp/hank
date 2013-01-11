#!/bin/sh

BINDIR=`dirname "$0"`
nohup $BINDIR/start_partition_server.sh $@ &> log/partition_server.out &