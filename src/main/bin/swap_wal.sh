#!/bin/bash

UBERSTORE_BASE=$1
STAGED_WAL_DIR=$2

WAL_DIR=$UBERSTORE_BASE/uberstore
BACKUP_WAL_DIR=$UBERSTORE_BASE/uberstore-backup

echo "Removing any existing backup logs from $BACKUP_WAL_DIR"
rm -rf $BACKUP_WAL_DIR

echo "Copying $WAL_DIR to $BACKUP_WAL_DIR"
cp -r $WAL_DIR $BACKUP_WAL_DIR

echo "Copying $STAGED_WAL_DIR to $WAL_DIR"
for f in `ls -1 $STAGED_WAL_DIR`
do
  echo "  Copying $f"
  # This is being done with cat to preserve ownership and permissions.
  cat $STAGED_WAL_DIR/$f > $WAL_DIR/$f
done

echo "Swap Completed"
