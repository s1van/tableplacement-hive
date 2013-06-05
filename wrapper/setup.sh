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

while getopts "b:h:c:o:" OPTION
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
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

source $CONFD/site.conf;
HBIN=$HADOOP_HOME/bin;
HCONF=$HADOOP_HOME/conf;
SLAVE=$HCONF/slaves;

HADOOP=$HBIN/hadoop;
XONF=$UTILD/orc-xonf.py;

bash $HBIN/stop-all.sh;
echo "Setup HDFS Parameters";
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/hdfs-site.xml --key=dfs.block.size --value=$(($HDFS_BLK_SIZE * 1024 * 1024 )) 
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.map.child.java.opts --value="-Xmx${MAP_JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.reduce.child.java.opts --value="-Xmx${REDUCE_JAVA_HEAP_SIZE}m"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.tasktracker.map.tasks.maximum --value="${C_MAP_NUM}"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.tasktracker.reduce.tasks.maximum --value="${C_RED_NUM}"
pdsh -R ssh -w ^${SLAVE} $XONF --file=$HCONF/mapred-site.xml --key=mapred.output.dir --value="${MR_OUT_DIR}"

bash $HBIN/start-all.sh;
$HADOOP dfsadmin -safemode wait;
