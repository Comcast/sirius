#!/bin/bash

sirius_base=`dirname $0`
sirius_base=`cd $sirius_base/..; pwd`

# Got this from the scala launcher, we somehow hose
# the tty, so we do this to restor it when we're done
exit_code=1
saved_stty=""

function onExit {
    if [ "$saved_tty" == "" ]; then
        stty $saved_stty
        exit $exit_code
    fi
}

trap onExit EXIT

SIRIUS_CLASSPATH=$sirius_base/bin/sirius.jar:${CLASSPATH:=.}
for j in `ls $sirius_base/lib/*.jar`; do
    SIRIUS_CLASSPATH=$SIRIUS_CLASSPATH:$j
done

saved_stty=`stty -g 2>/dev/null`

(echo ":load $sirius_base/bin/bootstrap.scala"; cat) | scala -cp $SIRIUS_CLASSPATH -Yrepl-sync
exit_code=$?
