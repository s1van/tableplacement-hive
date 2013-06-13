#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
WRAPD=$CDIR/wrapper;

SCALE=$1
REP=$2;
DIR=$3;
HDFS_DATA_PATH=$4;

if [ $# -lt 4 ]
then
        echo "$0 SCALE REPEAT LOG_DIR HDFS_DATA_PATH"
        exit
fi

mkdir -p $DIR;

RGS="4 64 128";	# row group size (MB)
BUFS="64,128";	# HDFS buffer sizes (KB); sparate by ','
OSRS="512";	# os read ahead buffer sizes (KB)

for rg in $RGS; do
	$WRAPD/test.sh SSB-Batch-Default $rg "$BUFS" "$OSRS" $REP $SCALE "$DIR/default-RG${rg}" $HDFS_DATA_PATH;
	$WRAPD/test.sh SSB-Batch-Trojan $rg "$BUFS" "$OSRS" $REP $SCALE "$DIR/trojan-RG${rg}" $HDFS_DATA_PATH;
done
