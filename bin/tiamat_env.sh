#!/bin/sh

BINDIR=`dirname "$0"`
CLASSPATH="$BINDIR/../build/tiamat.jar:$CLASSPATH"

for i in "$BINDIR"/../lib/*.jar
do
	CLASSPATH="$i:$CLASSPATH"
done

# echo $CLASSPATH
