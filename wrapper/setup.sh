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
        echo "  -d  HADOOP_TMP_DIR"
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
HADOOP_TMP=/mnt/hadoop/data;
INIT=false;
REPLICA=1;

INMEM_SIZE=200;
IO_SORT_MB=200;
NN_HCOUNT=30;
MR_PCOPY=10;
IO_SORT_FACTOR=50;
MR_JT_HCOUNT=20;
TT_TNUM=20;


while getopts "b:h:c:o:ir:d:" OPTION
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
                d)
			HADOOP_TMP=$OPTARG;
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

REDUCE_JAVA_HEAP_SIZE=$(($REDUCE_JAVA_HEAP_SIZE + $INMEM_SIZE));
MAP_JAVA_HEAP_SIZE=$(($MAP_JAVA_HEAP_SIZE + $IO_SORT_MB));

source $CONFD/site.conf;
HADOOP_BIN=$HADOOP_HOME/bin;
HADOOP_CONF=$HADOOP_HOME/conf;
HIVE_CONF=$HIVE_HOME/conf;
SLAVE=$HADOOP_CONF/slaves;

MHOST="$(cat $HADOOP_CONF/masters)";

HADOOP=$HADOOP_BIN/hadoop;
XONF=$UTILD/orc-xonf.py;

bash $HADOOP_BIN/stop-all.sh;
echo "Setup HDFS Parameters";
$XONF --file=$HADOOP_CONF/core-site.xml --key=hadoop.tmp.dir --value=$HADOOP_TMP
$XONF --file=$HADOOP_CONF/core-site.xml --key=fs.default.name --value=hdfs://${MHOST}:9004
$XONF --file=$HADOOP_CONF/core-site.xml --key=fs.inmemory.size.mb --value=$INMEM_SIZE
$XONF --file=$HADOOP_CONF/core-site.xml --key=io.sort.factor --value=$IO_SORT_FACTOR
$XONF --file=$HADOOP_CONF/core-site.xml --key=io.sort.mb --value=$IO_SORT_MB

$XONF --file=$HADOOP_CONF/hdfs-site.xml --key=dfs.replication --value=$REPLICA
$XONF --file=$HADOOP_CONF/hdfs-site.xml --key=dfs.block.size --value=$(($HDFS_BLK_SIZE * 1024 * 1024 )) 
$XONF --file=$HADOOP_CONF/hdfs-site.xml --key=dfs.namenode.handler.count --value=$NN_HCOUNT

$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.job.tracker --value=${MHOST}:9005
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.map.child.java.opts --value="-Xmx${MAP_JAVA_HEAP_SIZE}m"
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.reduce.child.java.opts --value="-Xmx${REDUCE_JAVA_HEAP_SIZE}m"
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.tasktracker.map.tasks.maximum --value="${C_MAP_NUM}"
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.tasktracker.reduce.tasks.maximum --value="${C_RED_NUM}"
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.output.dir --value="${MR_OUT_DIR}"
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.reduce.parallel.copies --value=$MR_PCOPY
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.job.tracker.handler.count --value=$MR_JT_HCOUNT
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=tasktracker.http.threads --value=$TT_TNUM
$XONF --file=$HADOOP_CONF/mapred-site.xml --key=mapred.map.tasks.speculative.execution --value=false

echo "Copy Configuration to all slaves ...";
for host in $(cat $SLAVE); do
	scp $HADOOP_CONF/core-site.xml ${host}:${HADOOP_CONF}/core-site.xml &
done
sleep 1; wait;

for host in $(cat $SLAVE); do
	scp $HADOOP_CONF/mapred-site.xml ${host}:${HADOOP_CONF}/mapred-site.xml &
done
sleep 1; wait;

for host in $(cat $SLAVE); do
	scp $HADOOP_CONF/hdfs-site.xml ${host}:${HADOOP_CONF}/hdfs-site.xml &
done
sleep 1; wait;

if [ "$INIT" == "true" ]; then
	echo "Initialize Hadoop Namenode and hive-site.xml";
	rm -rf $HADOOP_TMP;
	echo -e '<?xml version="1.0" ?><configuration>\n</configuration>' > $HIVE_CONF/hive-site.xml;

	for host in $(cat $SLAVE); do
		scp $HADOOP_CONF/masters ${host}:${HADOOP_CONF}/masters &
	done
	sleep 1; wait;

	for host in $(cat $SLAVE); do
		scp $HADOOP_CONF/slaves ${host}:${HADOOP_CONF}/slaves &
	done
	sleep 1; wait;

	for host in $(cat $SLAVE); do
		ssh $host "rm -rf $HADOOP_TMP" &
	done
	sleep 1; wait;
	
	$HADOOP_BIN/hadoop namenode -format -force;
fi

echo "Setup Hive Parameters";
$XONF --file=$HIVE_CONF/hive-site.xml --key=javax.jdo.option.ConnectionURL --value='jdbc:derby:;databaseName=metastore_db;create=true'
$XONF --file=$HIVE_CONF/hive-site.xml --key=hive.stats.dbconnectionstring --value='jdbc:derby:;databaseName=TempStatsStore;create=true'


for host in $(cat $SLAVE); do
	scp $HIVE_CONF/hive-site.xml ${host}:${HIVE_CONF}/hive-site.xml &
done
sleep 1; wait;

bash $HADOOP_BIN/start-all.sh;
$HADOOP dfsadmin -safemode wait;
