#!/bin/bash
#
# Copyright 2012-2014 Comcast Cable Communications Management, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

sirius_base=`dirname $0`
sirius_base=`cd $sirius_base/..; pwd`

# Got this from the scala launcher, we somehow hose
# the tty, so we do this to restore it when we're done
exit_code=1
saved_stty=""

function onExit {
    if [ ! -z "$saved_stty" ]; then
        stty $saved_stty
    fi
    exit $exit_code
}

trap onExit EXIT

SIRIUS_CLASSPATH=${CLASSPATH:=.}:$sirius_base/conf/:$sirius_base/bin
for j in `ls $sirius_base/lib/*.jar`; do
    SIRIUS_CLASSPATH=$SIRIUS_CLASSPATH:$j
done

saved_stty=`stty -g 2>/dev/null`

(echo ":load $sirius_base/bin/bootstrap.scala"; cat) | scala -cp $SIRIUS_CLASSPATH -Yrepl-sync
exit_code=$?

