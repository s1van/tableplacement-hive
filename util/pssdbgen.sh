#!/bin/bash

usage()
{
	echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
	echo "[description]:"
	echo "  execute benchGen (SS-DB) in parallel and load the date into HDFS"
	echo "[option]:"
	echo "  -c  config (tiny, small, normal, large)"
	echo "  -e  dbgen_path"
	echo "  -h  hadoop_exec_path"
	echo "  -p  hdfs_path"
	echo "  -f  hostlist file"
	echo "  -t  tile_path"
	echo "  -1  (generate only one cycle if set)"
	echo ""
	echo
}

if [ $# -lt 12 ]
then
	usage
	exit
fi
		
ONECYCLE=false;
while getopts "c:t:e:p:f:h:1" OPTION
do
        case $OPTION in
                c)
                        CONFIG=$OPTARG;
                        ;;
                t)
                        TILE="$OPTARG";
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
                1)
			ONECYCLE=true;
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

PREFIX=bench_

rdbgen() {
	local PARTS="$1";
	local HOST=$2;
	TNAME=$(basename $HDFS_PATH);

	for part in $PARTS;do
		echo "Generate part $((${part}+1))/${PARTNUM} of table $TNAME on $HOST ..."
		ssh $HOST eval "cd $DBGEN && $DBGEN/benchGen -t -c $CONFIG -n $PARTNUM -i $part $TILE";
	done

	sleep 4;
	echo "Copy parts $(echo $PARTS) of table $TNAME to hdfs://$HDFS_PATH from $HOST ..."
	ssh $HOST eval "chmod -R 777 $DBGEN"
	ssh $HOST eval "ls $DBGEN| grep ${PREFIX}| xargs -I % $HADOOP fs -copyFromLocal $DBGEN/% $HDFS_PATH"
	ssh $HOST eval "ls $DBGEN| grep ${PREFIX}| xargs -I % rm $DBGEN/%"
}

HOST_NUM=$(echo $HOSTLIST| wc -w);
HINDEX=0;

tiny()	{ CLEN=10; PARTNUM=10; }
small()	{ CLEN=20; PARTNUM=160; }
normal(){ CLEN=20; PARTNUM=400; }
large() { CLEN=20; PARTNUM=1000; }

$CONFIG; # Set configuration related parameters

if [ "$ONECYCLE" == "true" ]; then
	GLEN=$CLEN;
	HDFS_PATH=$HDFS_PATH;	
else
	GLEN=$PARTNUM;
fi

for host in $HOSTLIST; do
	parts="$(seq $HINDEX $HOST_NUM $(($GLEN-1)))";
	if [ "$parts" != "" ]; then
		rdbgen "$parts" $host &
	fi

	HINDEX=$(($HINDEX + 1));
done

wait;

