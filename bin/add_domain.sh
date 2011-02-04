#!/bin/sh

BINDIR=`dirname "$0"`

. "$BINDIR"/tiamat_env.sh

java -cp "$CLASSPATH" com.rapleaf.tiamat.cli.AddDomain $@
