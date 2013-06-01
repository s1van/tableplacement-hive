#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

HDFS_BLK_SIZE=$1;	#MiB (256)
MAP_JAVA_HEAP_SIZE=$2;	#MiB (512)
REDUCE_JAVA_HEAP_SIZE=$3; #MiB (1024)
MR_OUT_DIR=$4;

source $CONFD/site.conf;
HBIN=$HADOOP_HOME/bin;
HCONF=$HADOOP_HOME/conf;
HADOOP=$HBIN/hadoop;
XONF=$UTILD/orc-xonf.py;

bash $HBIN/stop-all.sh;
echo "Setup HDFS Parameters";
$XONF --file=$HCONF/hdfs-site.xml --key=dfs.block.size --value=$(($HDFS_BLK_SIZE * 1024 * 1024 )) 
#$XONF --file=$HCONF/mapred-site.xml --key=mapred.child.java.opts --value="-Xmx${JAVA_HEAP_SIZE}m"
$XONF --file=$HCONF/mapred-site.xml --key=mapred.map.child.java.opts --value="-Xmx${MAP_JAVA_HEAP_SIZE}m"
$XONF --file=$HCONF/mapred-site.xml --key=mapred.reduce.child.java.opts --value="-Xmx${REDUCE_JAVA_HEAP_SIZE}m"
$XONF --file=$HCONF/mapred-site.xml --key=mapred.output.dir --value="${MR_OUT_DIR}"

bash $HBIN/start-all.sh;
$HADOOP dfsadmin -safemode wait;
