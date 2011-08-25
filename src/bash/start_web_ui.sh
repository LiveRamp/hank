#!/bin/sh

BINDIR=`dirname "$0"`

. "$BINDIR"/env.sh

java -cp "$CLASSPATH" com.rapleaf.hank.ui.WebUiServer $@
