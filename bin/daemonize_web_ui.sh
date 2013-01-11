#!/bin/sh

BINDIR=`dirname "$0"`
nohup $BINDIR/start_web_ui.sh $@ &> log/web_ui.log &