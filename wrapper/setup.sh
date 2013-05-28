#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

HDFS_BLK_SIZE=$1;	#MiB
JAVA_HEAP_SIZE=$2;	#MiB

source $CONFD/site.conf;
HBIN=$HADOOP_HOME/bin;
HCONF=$HADOOP_HOME/conf;
HADOOP=$HBIN/hadoop;
XONF=$UTILD/orc-xonf.py;

bash $HBIN/stop-all.sh;
echo "Setup HDFS Parameters";
$XONF --file=$HCONF/hdfs-site.xml --key=dfs.block.size --value=$(($HDFS_BLK_SIZE * 1024 * 1024 )) 
$XONF --file=$HCONF/mapred-site.xml --key=mapred.child.java.opts --value="-Xmx${JAVA_HEAP_SIZE}m"

bash $HBIN/start-all.sh;
$HADOOP dfsadmin -safemode wait;
