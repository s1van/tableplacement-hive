#!/bin/bash

usage()
{
	echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
	echo "[description]:"
	echo "  execute dbgen in parallel and load the date into HDFS"
	echo "[option]:"
	echo "  -s  scale"
	echo "  -e  dbgen_path"
	echo "  -h  hadoop_exec_path"
	echo "  -p  hdfs_path"
	echo "  -f  hostlist file"
	echo "  -t  table(p,c,s,l,...)"
	echo ""
	echo
}

if [ $# -lt 12 ]
then
	usage
	exit
fi

while getopts "s:t:e:p:f:h:" OPTION
do
        case $OPTION in
                s)
                        SCALE=$OPTARG;
                        ;;
                t)
                        TABLE="$OPTARG";
                        ;;
                e)
                        DBGEN=$OPTARG;
                        ;;
                h)
                        HADOOP=$OPTARG;
                        ;;
                p)
                        HDFS_PATH=$OPTARG;
                        ;;
                f)
                        HOSTLIST="$(cat $OPTARG)";
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

rdbgen() {
	local PARTS="$1";
	local HOST=$2;
	TNAME=$(basename $HDFS_PATH);

	for part in $PARTS;do
		echo "Generate part $part of table $TABLE on $HOST ..."
		ssh $HOST eval "cd $DBGEN && $DBGEN/dbgen -f -s $SCALE -T $TABLE -C $SCALE -S $part";
	done

	sleep 1;
	echo "Copy parts $(echo $PARTS) of table $TABLE to hdfs://$HDFS_PATH from $HOST ..."
	ssh $HOST eval "ls $DBGEN| grep ${TNAME}.tbl| xargs -I % $HADOOP fs -copyFromLocal $DBGEN/% $HDFS_PATH"
	ssh $HOST eval "ls $DBGEN| grep ${TNAME}.tbl| xargs -I % rm $DBGEN/%"
}

HOST_NUM=$(echo $HOSTLIST| wc -w);
HINDEX=1;
PARTNUM=$SCALE;

for host in $HOSTLIST; do
	parts="$(seq $HINDEX $HOST_NUM $PARTNUM)";
	rdbgen "$parts" $host &

	HINDEX=$(($HINDEX + 1));
done

wait;

