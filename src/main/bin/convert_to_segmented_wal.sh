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

function die() {
  echo "$@" >&2
  exit 1
}

function die_happy() {
  echo "$@" >&2
  exit 0
}

if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` <base_dir>" >&2
  echo "  base_dir is the directory that contains the uberstore directory to be converted to a segmented WAL." >&2
  exit 1
fi

UBERSTORE_BASE=$1
WALTOOL_BASE=$(dirname $0)
WALTOOL_BASE=$(cd $WALTOOL_BASE && pwd)
SEGMENT_SIZE=2000000

WAL_DIR=$UBERSTORE_BASE/uberstore
SEGMENTED_WAL_DIR=$UBERSTORE_BASE/uberstore-segmented
LEGACY_WAL_DIR=$UBERSTORE_BASE/uberstore-legacy

echo "Removing any existing segmented logs from $SEGMENTED_WAL_DIR"
rm -rf $SEGMENTED_WAL_DIR || die "Error removing existing segmented log."

echo "Removing any existing backup logs from LEGACY_WAL_DIR"
rm -rf $LEGACY_WAL_DIR || die "Error removing existing legacy log."

echo "Confirming $WAL_DIR is in fact a legacy WAL"
$WALTOOL_BASE/waltool is-legacy $WAL_DIR || die_happy "Will not convert non-legacy WAL, exiting quietly."

echo "Converting $WAL_DIR to $SEGMENTED_WAL_DIR"
$WALTOOL_BASE/waltool convert-to-segmented $WAL_DIR $SEGMENTED_WAL_DIR $SEGMENT_SIZE || die "Error converting to segmented log."

echo "Moving $WAL_DIR to $LEGACY_WAL_DIR"
mv $WAL_DIR $LEGACY_WAL_DIR || die "Failed moving $WAL_DIR $LEGACY_WAL_DIR"

echo "Moving $SEGMENTED_WAL_DIR to $WAL_DIR"
mv $SEGMENTED_WAL_DIR $WAL_DIR || die "Failed moving $LEGACY_WAL_DIR $WAL_DIR"

echo "Changing ownership of $WAL_DIR to tomcat so that the app can actually read/write wal files."
/usr/bin/sudo /bin/chown -R tomcat $WAL_DIR || die "Failed chown to tomcat of $WAL_DIR."

echo "Conversion from Legacy WAL to Segmented WAL Complete"
