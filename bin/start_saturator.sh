#!/bin/sh

BINDIR=`dirname "$0"`
CONFDIR=`dirname "$0"`/../conf

. "$BINDIR"/env.sh

java $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.loadtest.RandomSaturator $@
