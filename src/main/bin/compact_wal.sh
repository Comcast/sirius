#!/bin/bash

function die() {
  echo "$@" >&2
  exit 1
}

if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` <base_dir>" >&2
  echo "  base_dir is the directory that contains the uberstore directory to be compacted." >&2
  exit 1
fi

if [ "x$JAVA_OPTS" == "x" ]; then
    JAVA_OPTS="-Xms8g -Xmx14g"
fi

UBERSTORE_BASE=$1

WAL_DIR=$UBERSTORE_BASE/uberstore
BACKUP_WAL_DIR=$UBERSTORE_BASE/uberstore-backup
COMPACTED_WAL_DIR=$UBERSTORE_BASE/uberstore-compacted

echo "Removing any existing compacted logs from $COMPACTED_WAL_DIR"
rm -rf $COMPACTED_WAL_DIR || die "Error removing existing compacted log."

echo "Removing any existing backup logs from $BACKUP_WAL_DIR"
rm -rf $BACKUP_WAL_DIR || die "Error removing existing backup log."

echo "Copying $WAL_DIR to $BACKUP_WAL_DIR"
cp -r $WAL_DIR $BACKUP_WAL_DIR || die "Error backing up log."

echo "Compacting $WAL_DIR into $COMPACTED_WAL_DIR"
JAVA_OPTS="$JAVA_OPTS" $UBERSTORE_BASE/sirius-standalone/bin/waltool compact two-pass $WAL_DIR $COMPACTED_WAL_DIR || die "Error compacting log."

echo "Copying $COMPACTED_WAL_DIR to $WAL_DIR"
for f in `ls -1 $COMPACTED_WAL_DIR`
do
  echo "  Copying $f"
  # This is being done with cat to preserve ownership and permissions.
  cat $COMPACTED_WAL_DIR/$f > $WAL_DIR/$f || die "Error copying log into place."
done

echo "Compaction Completed"
