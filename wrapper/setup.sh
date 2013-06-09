#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;


usage()
{
        echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
        echo "[description]:"
        echo "  Configure Hadoop & Hive"
        echo "[option]:"
        echo "  -b  HADOOP_BLOCK_SIZE"
        echo "  -h  MAP_JAVA_HEAP_SIZE,REDUCE_JAVA_HEAP_SIZE"
        echo "  -c  MAP_CONCURRENCY,REDUCE_CONCURRENCY"
        echo "  -o  MAPREDUCE_OUT_DIR"
        echo "  -r  #REPLICA"
        echo "  -i  (Perform initialization if set)"
        echo
}

if [ $# -lt 6 ]
then
        usage
        exit
fi

#Default Values
HDFS_BLK_SIZE=256;
MAP_JAVA_HEAP_SIZE=512;
REDUCE_JAVA_HEAP_SIZE=1024;
C_MAP_NUM=2;
C_RED_NUM=1;
MR_OUT_DIR=/tmp/mapred_out;
INIT=false;
REPLICA=1;

while getopts "b:h:c:o:ir:" OPTION
do
        case $OPTION in
                b)
                        HDFS_BLK_SIZE=$OPTARG;
                        ;;
                h)
			MAP_JAVA_HEAP_SIZE=$(echo $OPTARG| awk -F',' '{print $1}');
			REDUCE_JAVA_HEAP_SIZE=$(echo $OPTARG| awk -F',' '{print $2}');
                        ;;
                c)
			C_MAP_NUM=$(echo $OPTARG| awk -F',' '{print $1}');
			C_RED_NUM=$(echo $OPTARG| awk -F',' '{print $2}');
                        ;;
                o)
			MR_OUT_DIR=$OPTARG;
                        ;;
                i)
			INIT=true;
                        ;;
                r)
			REPLICA=$OPTARG;
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

source $CONFD/site.conf;
HADOOP_BIN=$HADOOP_HOME/bin;
HADOOP_CONF=$HADOOP_HOME/conf;
HIVE_CONF=$HIVE_HOME/conf;
SLAVE=$HADOOP_CONF/slaves;

MHOST="$(cat $HADOOP_CONF/masters)";
HADOOP_TMP=/tmp/hadoop_tmp;

HADOOP=$HADOOP_BIN/hadoop;
XONF=$UTILD/orc-xonf.py;

bash $HADOOP_BIN/stop-all.sh;
echo "Setup HDFS Parameters";
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/core-site.xml --key=hadoop.tmp.dir --value=$HADOOP_TMP
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/core-site.xml --key=fs.default.name --value=hdfs://${MHOST}:9004

pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/hdfs-site.xml --key=dfs.replication --value=$REPLICA
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/hdfs-site.xml --key=dfs.block.size --value=$(($HDFS_BLK_SIZE * 1024 * 1024 )) 

pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.job.tracker --value=${MHOST}:9005
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.map.child.java.opts --value="-Xmx${MAP_JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.reduce.child.java.opts --value="-Xmx${REDUCE_JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.tasktracker.map.tasks.maximum --value="${C_MAP_NUM}"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.tasktracker.reduce.tasks.maximum --value="${C_RED_NUM}"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.output.dir --value="${MR_OUT_DIR}"

if [ "$INIT" == "true" ]; then
	echo "Initialize Hadoop Namenode and hive-site.xml";
	rm -rf $HADOOP_TMP;
	$HADOOP_BIN/hadoop namenode -format -force;
	pdsh -R ssh -w ^${SLAVE} eval "echo -e '\<?xml version=\\\"1.0\\\" ?\>\<configuration\>\\\n\</configuration\>' > $HIVE_CONF/hive-site.xml";

	for host in $(cat $SLAVE); do
		scp $HADOOP_CONF/masters ${host}:${HADOOP_CONF}/masters &
		scp $HADOOP_CONF/slaves ${host}:${HADOOP_CONF}/slaves &
	done
	sleep 1;
	wait;
fi

echo "Setup Hive Parameters";
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HIVE_CONF/hive-site.xml --key=javax.jdo.option.ConnectionURL --value='"jdbc:derby:;databaseName=metastore_db;create=true"'
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HIVE_CONF/hive-site.xml --key=hive.stats.dbconnectionstring --value='"jdbc:derby:;databaseName=TempStatsStore;create=true"'


bash $HADOOP_BIN/start-all.sh;
$HADOOP dfsadmin -safemode wait;
