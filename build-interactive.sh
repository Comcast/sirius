#!/bin/bash

base=`dirname $0`
base=`cd $base; pwd`

DO_ARCHIVE=true
if [ "x$1" == "--skip-tar" ]; then
    DO_ARCHIVE=false
    shift
fi

function run {
    echo "$ $@"
    "$@"
    res=$?
    if [ ! $res -eq 0 ]; then
        echo 1>&2
        echo "Failed with $res" 1>&2
        echo 1>&2
        exit $res
    fi
}

if [ "$1" != "--skip-mvn" ]; then
    run mvn clean package dependency:copy-dependencies -Dmaven.test.skip=true
fi

targetdir="$base/target"
distdir="$base/target/sirius-standalone"

if [ -d $distdir ]; then
    run rm -rf "$distdir"
fi

run mkdir -p $distdir

run mkdir $distdir/bin
run cp $targetdir/sirius.jar $distdir/bin/
run cp -p $base/src/main/bin/* $distdir/bin/

run mkdir $distdir/lib
run cp $targetdir/dependency/*.jar $distdir/lib

echo
echo "octave-dist available at $targetdir/sirius-standalone/"
echo

if [ "x$DO_ARCHIVE" == "xtrue" ]; then
    run cd $targetdir
    run tar -czf sirius-standalone.tar.gz sirius-standalone/
    run cd -
    echo
    echo "sirius-standalone tarball available at $targetdir/sirius-standalone.tar.gz"
    echo
fi

