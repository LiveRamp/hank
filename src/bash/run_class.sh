#!/bin/sh

BINDIR=`dirname "$0"`
CONFDIR=`dirname "$0"`/../conf
CLASSNAME="$1"
shift

. "$BINDIR"/env.sh

java -XX:+UseConcMarkSweepGC -cp "$CLASSPATH" "$CLASSNAME" $@
