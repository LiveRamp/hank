#!/bin/sh

BINDIR=`dirname "$0"`

. "$BINDIR"/env.sh

if [ -f "$BINDIR"/part_daemon_extra.sh ]; then
  echo "Loading extra part daemon config from " "$BINDIR"/part_daemon_extra.sh
  . "$BINDIR"/part_daemon_extra.sh
fi

java $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.part_daemon.PartDaemonServer $@