#!/bin/bash
CDIR=`dirname $0`/../;
WRAPD=$CDIR/wrapper;

DIR=$1;
mkdir -p $DIR;

RGS="4" # 16 64 256";	# row group size (MB)
#BUFS="64,128,256";	# HDFS buffer size (KB)
BUFS="256";	# HDFS buffer size (KB)
#OSRS="0,128,256,512,1024";	# os read ahead buffer size (KB)
OSRS="512";	# os read ahead buffer size (KB)

REP=4;
SCALE=1;
for rg in $RGS; do
	#$WRAPD/test.sh SSB-Batch $rg ssb1 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb1-RG${rg}";
	$WRAPD/test.sh SSB-Batch $rg ssb2 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb2-RG${rg}";
	#$WRAPD/test.sh SSB-Batch $rg ssb3 "$BUFS" "$OSRS" $REP $SCALE "$DIR/ssb3-RG${rg}";
done
