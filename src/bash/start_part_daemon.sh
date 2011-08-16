#!/bin/sh

BINDIR=`dirname "$0"`

. "$BINDIR"/env.sh

if [ $# -eq 1 ]; then
  EXTRA_CONFIGURATION_SCRIPT=$1
  . $EXTRA_CONFIGURATION_SCRIPT
fi

java $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.part_daemon.PartDaemonServer $@