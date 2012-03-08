#!/bin/sh

BINDIR=`dirname "$0"`
CONFDIR=`dirname "$0"`/../conf

. "$BINDIR"/env.sh

if [ -f "$CONFDIR"/smart_client_server_extra.sh ]; then
  echo "Loading extra Smart Client Server config from " "$CONFDIR"/smart_client_server_extra.sh
  . "$CONFDIR"/smart_client_server_extra.sh
fi

java -XX:+UseConcMarkSweepGC $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.client.SmartClientDaemon $@