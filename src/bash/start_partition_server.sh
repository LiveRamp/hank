#!/bin/sh

BINDIR=`dirname "$0"`

. "$BINDIR"/env.sh

if [ -f "$BINDIR"/partition_server_extra.sh ]; then
  echo "Loading extra partition server config from " "$BINDIR"/partition_server_extra.sh
  . "$BINDIR"/partition_server_extra.sh
fi

java $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.partition_server.PartitionServer $@
