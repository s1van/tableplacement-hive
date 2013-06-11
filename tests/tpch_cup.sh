#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
WRAPD=$CDIR/wrapper;

DIR=$1;
mkdir -p $DIR;

RGS="4 64 128";	# row group size (MB)
BUFS="64,128";	# HDFS buffer size (KB)
OSRS="512";	# os read ahead buffer size (KB)

REP=3;
SCALE=100;
HDFS_DATA_PATH=/tpch_s${SCALE};
for rg in $RGS; do
	$WRAPD/test.sh TPCH-Batch $rg tpch2 "$BUFS" "$OSRS" $REP $SCALE "$DIR/tpch2-RG${rg}" $HDFS_DATA_PATH;
	$WRAPD/test.sh TPCH-Batch $rg tpch1 "$BUFS" "$OSRS" $REP $SCALE "$DIR/tpch1-RG${rg}" $HDFS_DATA_PATH;
	#$WRAPD/test.sh TPCH-Batch $rg tpch3 "$BUFS" "$OSRS" $REP $SCALE "$DIR/tpch3-RG${rg}" $HDFS_DATA_PATH;
	$WRAPD/test.sh TPCH-CG-Q6-Batch $rg tpch_q6 "$BUFS" "$OSRS" $REP $SCALE "$DIR/tpch_q6-RG${rg}" $HDFS_DATA_PATH;
done
