#!/bin/sh

BINDIR=`dirname "$0"`

for i in `find $BINDIR/../ -name "*.jar"`
do
  CLASSPATH="$i:$CLASSPATH"
done
