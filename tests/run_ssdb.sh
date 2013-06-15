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
	$WRAPD/test.sh SSDB-Batch-Default $rg "$BUFS" "$OSRS" $REP $SCALE "$DIR/default-RG${rg}" $HDFS_DATA_PATH;
	$WRAPD/test.sh SSDB-Batch-V1 $rg "$BUFS" "$OSRS" $REP $SCALE "$DIR/v1-RG${rg}" $HDFS_DATA_PATH;
done
