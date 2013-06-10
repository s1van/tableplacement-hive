#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
WRAPD=$CDIR/wrapper;

DIR=$1;
mkdir -p $DIR;

RGS="4 64 128";	# row group size (MB)
BUFS="64 128";	# HDFS buffer size (KB)
OSRS="512";	# os read ahead buffer size (KB)

REP=3;
SCALE=100;
HDFS_DATA_PATH=/ssb_s${SCALE};
for rg in $RGS; do
	$WRAPD/test.sh SSB-Batch $rg ssb1 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb1-RG${rg}" $HDFS_DATA_PATH;
	$WRAPD/test.sh SSB-Batch $rg ssb2 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb2-RG${rg}" $HDFS_DATA_PATH;
	#$WRAPD/test.sh SSB-Batch $rg ssb3 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb3-RG${rg}" $HDFS_DATA_PATH;
done
