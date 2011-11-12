#!/bin/sh

BINDIR=`dirname "$0"`
CONFDIR=`dirname "$0"`/../conf

. "$BINDIR"/env.sh

if [ -f "$CONFDIR"/web_ui_extra.sh ]; then
  echo "Loading extra web ui config from " "$CONFDIR"/web_ui_extra.sh
  . "$CONFDIR"/web_ui_extra.sh
fi

java $EXTRA_JVM_ARGS -cp "$CLASSPATH" com.rapleaf.hank.ui.WebUiServer $@
