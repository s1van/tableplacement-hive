#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
WRAPD=$CDIR/wrapper;

DIR=$1;
mkdir -p $DIR;

RGS="4";	# row group size (MB)
#BUFS="64,128,256";	# HDFS buffer size (KB)
BUFS="128";	# HDFS buffer size (KB)
#OSRS="0,128,256,512,1024";	# os read ahead buffer size (KB)
OSRS="256";	# os read ahead buffer size (KB)

REP=1;
SCALE=1;
HDFS_DATA_PATH=/ssb_s1;
for rg in $RGS; do
	$WRAPD/test.sh SSB-Batch $rg ssb1 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb1-RG${rg}" $HDFS_DATA_PATH;
done