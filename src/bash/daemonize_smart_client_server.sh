#!/bin/sh

BINDIR=`dirname "$0"`
nohup $BINDIR/start_smart_client_server.sh $@ &> log/smart_client_server.log &