#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

HDFS_BLK_SIZE=$1;	#MiB (256)
MAP_JAVA_HEAP_SIZE=$2;	#MiB (512)
REDUCE_JAVA_HEAP_SIZE=$3; #MiB (1024)
C_MAP_NUM=$4;		#(2)[?]
C_RED_NUM=$5;		#(1)[?]
MR_OUT_DIR=$6;		#(/tmp/mapred_out)

source $CONFD/site.conf;
HBIN=$HADOOP_HOME/bin;
HCONF=$HADOOP_HOME/conf;
SLAVE=$HCONF/slaves;

HADOOP=$HBIN/hadoop;
XONF=$UTILD/orc-xonf.py;

bash $HBIN/stop-all.sh;
echo "Setup HDFS Parameters";
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/hdfs-site.xml --key=dfs.block.size --value=$(($HDFS_BLK_SIZE * 1024 * 1024 )) 
#pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.child.java.opts --value="-Xmx${JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.map.child.java.opts --value="-Xmx${MAP_JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.reduce.child.java.opts --value="-Xmx${REDUCE_JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.tasktracker.map.tasks.maximum --value="${C_MAP_NUM}"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.tasktracker.reduce.tasks.maximum --value="${C_RED_NUM}"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.output.dir --value="${MR_OUT_DIR}"

bash $HBIN/start-all.sh;
$HADOOP dfsadmin -safemode wait;
