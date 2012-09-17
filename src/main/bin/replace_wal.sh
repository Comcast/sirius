#!/bin/bash

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
