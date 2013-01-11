#!/bin/sh

BINDIR=`dirname "$0"`
CONFDIR=`dirname "$0"`/../conf

. "$BINDIR"/env.sh

if [ -f "$CONFDIR"/ring_group_conductor_extra.sh ]; then
  echo "Loading extra Ring Group Conductor config from " "$CONFDIR"/ring_group_conductor_extra.sh
  . "$CONFDIR"/ring_group_conductor_extra.sh
fi

java -XX:+UseConcMarkSweepGC $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.ring_group_conductor.RingGroupConductor $@