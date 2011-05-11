#!/bin/sh

BINDIR=`dirname "$0"`
nohup $BINDIR/start_part_daemon.sh $@ &>> log/part_daemon.log &