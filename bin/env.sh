#!/bin/sh

BINDIR=`dirname "$0"`
# CLASSPATH="$CLASSPATH"

for i in `find $BINDIR/../lib -name "*.jar"`
do
  CLASSPATH="$i:$CLASSPATH"
done

# echo $CLASSPATH
