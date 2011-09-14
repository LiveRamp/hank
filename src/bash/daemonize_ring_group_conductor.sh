#!/bin/sh

BINDIR=`dirname "$0"`
nohup $BINDIR/start_ring_group_conductor.sh $@ &> log/ring_group_conductor.out &