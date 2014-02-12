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

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` <base_dir> <staging_dir>" >&2
  echo "  base_dir is the directory that contains the uberstore directory with contents to be replaced." >&2
  echo "  staging_dir is the directory that contains the new uberstore files." >&2
  exit 1
fi

UBERSTORE_BASE=$1
STAGED_WAL_DIR=$2

WAL_DIR=$UBERSTORE_BASE/uberstore
BACKUP_WAL_DIR=$UBERSTORE_BASE/uberstore-backup

echo "Removing any existing backup logs from $BACKUP_WAL_DIR"
rm -rf $BACKUP_WAL_DIR || die "Error removing existing backup log."

echo "Copying $WAL_DIR to $BACKUP_WAL_DIR"
cp -r $WAL_DIR $BACKUP_WAL_DIR || die "Error backing up log."

echo "Copying $STAGED_WAL_DIR to $WAL_DIR"
for f in `ls -1 $STAGED_WAL_DIR`
do
  echo "  Copying $f"
  # This is being done with cat to preserve ownership and permissions.
  cat $STAGED_WAL_DIR/$f > $WAL_DIR/$f || die "Error copying log into place."
done

echo "Swap Completed"
