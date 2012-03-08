#!/bin/sh

BINDIR=`dirname "$0"`
CONFDIR=`dirname "$0"`/../conf

. "$BINDIR"/env.sh

if [ -f "$CONFDIR"/partition_server_extra.sh ]; then
  echo "Loading extra partition server config from " "$CONFDIR"/partition_server_extra.sh
  . "$CONFDIR"/partition_server_extra.sh
fi

java -XX:+UseConcMarkSweepGC $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.partition_server.PartitionServer $@
