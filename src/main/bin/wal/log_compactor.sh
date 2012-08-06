#!/bin/bash
ORDER_BY_SEQ_AWK=`pwd`/order_by_seq.awk
COMPACT_AWK=`pwd`/compact.awk
COUNT_ENTRIES_AWK=`pwd`/count_entries.awk

TMP_FILE=`mktemp -t "wal"`

if [ $# -ne 2 ]; then
   echo ""
   echo "USAGE: $0 /path/to/log/file /path/to/compacted/file"
   echo ""
   exit 2
fi
FILE=$1
DEST_FILE=$2
if [ ! -f $FILE ]; then
   echo "Filed does not exist: $FILE"
   exit 3       
fi

echo "Compaction started: `date`"
cat $FILE | $ORDER_BY_SEQ_AWK > $TMP_FILE
cat $FILE | awk -v FILE=$TMP_FILE -f $COMPACT_AWK > $DEST_FILE

echo "Compaction ended:   `date`"
echo ""
echo "Input file size:  `du -sh $FILE | awk '{print $1}'`"
echo "Output file size: `du -sh $DEST_FILE | awk '{print $1}'`"
echo ""
echo "Raw log entries:       `cat $FILE | wc -l`"
echo "Compacted log entries: `cat $DEST_FILE | wc -l`"
echo ""
cat $DEST_FILE | $COUNT_ENTRIES_AWK
rm -rf $TMP_FILE
