#!/bin/sh

BINDIR=`dirname "$0"`
nohup $BINDIR/start_data_deployer.sh $@ &> log/data_deployer.out &