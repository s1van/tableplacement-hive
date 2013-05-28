#!/bin/bash
CDIR=`dirname $0`/../;
WRAPD=$CDIR/wrapper;

#RGS="4 16 64 256";	# row group size (MB)
RGS="64 256";	# row group size (MB)
BUFS="64,128,256";	# HDFS buffer size (KB)
OSRS="0,128,256,512,1024";	# os read ahead buffer size (KB)

for rg in $RGS; do
	$WRAPD/test.sh SSB-Batch $rg ssb2 "$BUFS" "$OSRS" 8 1 "/home/siyuan/expr/tp-hive-test/ssb2-RG${rg}";
done
