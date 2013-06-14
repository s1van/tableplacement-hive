#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
SLAVE=$HADOOP_HOME/conf/slaves;
NODE_NUM=$(cat $SLAVE| wc -l);

INSPECT=$UTILD/hhInspect.py;

update-task-info() {
	local LOG=$1;
	local MAP=$2;
	local REDUCE=$3;
	local MISC=$4;
	
	local TMP1=$(mktemp);
	echo "Deal with the last Job in $LOG ..."
	#JOBS=$(grep 'Tracking URL' $LOG| awk -F'=' '{print $3"="$4}'| sed 's/jobdetails/jobtasks/g');
	JOBS=$(grep 'Tracking URL' $LOG| awk -F'=' '{print $3"="$4}'| sed 's/jobdetails/jobtasks/g'| tail -1);
	JOBID=$(grep 'Tracking URL' $LOG| tail -1| awk -F'=' '{print $4}');
	JNAMES=$(grep 'Tracking URL' $LOG| awk -F'=' '{if(NR==1) printf "%s", $4; else printf ",%s", $4;}');

	touch $MAP $REDUCE $MISC;
	for job in $JOBS; do
	        curl $job'&type=map&pagenum=1' 2>/dev/null| egrep 'mins|sec'| sed 's/\(.*\)(\(.*\))\(.*\)/\2/g'| sed -e 's/mins/*60/g' -e 's/,/+/g' -e 's/sec//g'|bc | paste - $MAP > $TMP1
	        cp $TMP1 $MAP;
	        curl $job'&type=reduce&pagenum=1' 2>/dev/null| egrep 'mins|sec'| sed 's/\(.*\)(\(.*\))\(.*\)/\2/g'| sed -e 's/mins/*60/g' -e 's/,/+/g' -e 's/sec//g'|bc | paste - $REDUCE > $TMP1
	        cp $TMP1 $REDUCE;
	done

	$INSPECT --ip=localhost --port=50030 --jobid=$JOBID --info=MapPhaseLength,ReducePhaseLength,FinishedTime >> $MISC;
	
	rm $TMP1;
}

run_query() {
	local SQL=$1;
	local BASE=$2;
	local HEADSET=$3;

	local LOG=${BASE}.log;
	local IOS=${BASE}.iostat;
	local MAP=${BASE}.mapper;
	local REDUCE=${BASE}.reducer;
	local MISC=${BASE}.misc;
	
	echo "Cleanup Cache...";
        $UTILD/cache-cleanup.sh -g ${SLAVE};

	echo "Execute Query $SQL"
	pdsh -R ssh -w ^${SLAVE} iostat -d -t -k $MONDEV >> $IOS;
	$WRAPD/run.sh $SQL $HEADSET >> $LOG 2>&1;
	
	echo "Collect iostat info to $IOS"
	pdsh -R ssh -w ^${SLAVE} iostat -d -t -k $MONDEV >> $IOS;

	echo "Update Mapper info in $MAP"
	update-task-info $LOG $MAP $REDUCE $MISC;
}

ssb-load() { 
	local SCALE=$1;
	local LOAD_WHICH=$2;	#corresponding to templates (in reference to util/load.sh)
	local HEADSET=$3;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$4;
	local HDFS_DATA_PATH=$5;

	$WRAPD/load.sh $LOAD_WHICH $SCALE $HEADSET $HDFS_DATA_PATH > $OUTDIR/ssb_load.sql 2>&1;
}

tpch-load() { 
	local SCALE=$1;
	local LOAD_WHICH=$2;	#corresponding to templates (in reference to util/load.sh)
	local HEADSET=$3;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$4;
	local HDFS_DATA_PATH=$5;

	$WRAPD/load.sh $LOAD_WHICH $SCALE $HEADSET $HDFS_DATA_PATH > $OUTDIR/tpch_load.sql 2>&1;
}

ssb-query() { 
	local TAG=$1;	
	local HEADSET=$2;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$3;

	echo "Execute Queries ... [$TAG $HEADSET $OUTDIR]"
	run_query ssb1_1 $OUTDIR/ssb1_1_${TAG} $HEADSET 
	run_query ssb1_2 $OUTDIR/ssb1_2_${TAG} $HEADSET
	run_query ssb1_3 $OUTDIR/ssb1_3_${TAG} $HEADSET
}

ssb-query-cg() { 
	local TAG=$1;	
	local HEADSET=$2;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$3;

	echo "Execute Queries ... [$TAG $HEADSET $OUTDIR]"
	run_query ssb1_1_cg $OUTDIR/ssb1_1_${TAG} $HEADSET 
	run_query ssb1_2_cg $OUTDIR/ssb1_2_${TAG} $HEADSET
	run_query ssb1_3_cg $OUTDIR/ssb1_3_${TAG} $HEADSET
}

tpch-query() { 
	local TAG=$1;	
	local HEADSET=$2;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$3;

	echo "Execute Queries ... [$TAG $HEADSET $OUTDIR]"
	run_query tpch_q6 $OUTDIR/tpch_q6_${TAG} $HEADSET
}

tpch-query-cg() { 
	local TAG=$1;	
	local HEADSET=$2;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$3;

	echo "Execute Queries ... [$TAG $HEADSET $OUTDIR]"
	run_query tpch_q6_cg $OUTDIR/tpch_q6_${TAG} $HEADSET
}

tpch-query-q6() { 
	local TAG=$1;	
	local HEADSET=$2;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$3;

	echo "Execute Queries ... [$TAG $HEADSET $OUTDIR]"
	run_query tpch_q6_cg $OUTDIR/tpch_q6_${TAG} $HEADSET
}

batch() {
        local LOAD_WHICH=$1; # column group, other optimizations
	local RGSIZE=$2;
        local HDFS_BUF_SIZES="$(echo $3| sed 's/,/ /g')";
        local OS_READAHEAD_SIZES="$(echo $4| sed 's/,/ /g')";
        local REP=$5;
        local SCALE=$6;
        local OUTDIR=$7;
	local HDFS_DATA_PATH=$8;
	local F_LOAD=$9;
	local F_QUERY=${10};
	local C_ON=${11};
	local C_TYPE=${12};
	
	mkdir -p $OUTDIR;

	echo "Delete all tables in current metastore"
	$HIVE -e 'show tables' | xargs -I {} $HIVE -e "drop table if exists {}";
	$HIVE -e 'show tables' | xargs -I {} $HIVE -e "drop view if exists {}";

	echo "Store the Parameters"
	touch $OUTDIR/README;
	echo "Benchmark RGSIZE LOAD_WITCH HDFS_BUF_SIZE OS_READAHEAD REP SCALE OUTDIR" >> $OUTDIR/README;
	echo $@ >> $OUTDIR/README;

	echo "Set HDFS Buffer Size, Row Group Size ${RGSIZE}MiB for Loading"
	VARS="HDFS_BUF_SIZE RGSIZE COMPRESS_ON COMPRESS_TYPE REDUCER_NUM";
	VALS="524288 $(($RGSIZE * 1024 * 1024)) $C_ON $C_TYPE $NODE_NUM";
	HEADSET=$(mktemp);
	$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
	cat $HEADSET
	
	echo "Start Testing ..."

	echo "Loading Data into Hive ... [$F_LOAD $SCALE $LOAD_WHICH $HEADSET $OUTDIR]"
	$F_LOAD $SCALE $LOAD_WHICH $HEADSET $OUTDIR $HDFS_DATA_PATH;
	
	for rnum in $(seq 1 $REP); do
		for bufsize in $HDFS_BUF_SIZES; do
			for osbuf in $OS_READAHEAD_SIZES; do
				echo -e "\nSet HDFS Buffer Size ${bufsize}KB, OS Readahead Buffer ${osbuf}KB"
				VALS="$(($bufsize * 1024)) $(($RGSIZE * 1024)) $C_ON $C_TYPE 1";
				for host in $(cat $SLAVE); do
					ssh -i $SSHKEY $host "sudo blockdev --setra $(($osbuf * 2)) $DEVICE" &
				done
				wait;
				
				$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
				cat $HEADSET
				$F_QUERY "H${bufsize}_O${osbuf}" $HEADSET $OUTDIR
			done
		done
	done

	rm $HEADSET;
}


SSB-Batch-Default() {
	echo "$0 [$@]"
	batch ssb1 $@ ssb-load ssb-query false BLOCK;
}

SSB-Batch-Trojan() {
	echo "$0 [$@]"
	batch ssb2 $@ ssb-load ssb-query-cg false BLOCK;
}

TPCH-Batch-Default() {
	echo "$0 [$@]"
	batch tpch1 $@ tpch-load tpch-query false BLOCK;
}

TPCH-Batch-Trojan() {
	echo "$0 [$@]"
	batch tpch2 $@ tpch-load tpch-query-cg false BLOCK;
}

TPCH-Batch-Q6() {
	echo "$0 [$@]"
	batch tpch_q6 $@ tpch-load tpch-query-q6 false BLOCK;
}


##################
###    main    ###
##################
usage()
{
        echo "Usage: `echo $0| awk -F/ '{print $NF}'` "
        echo "[arguments]:"
        echo "  TEST: SSB-Batch, SSB-Batch-DefaultBlockCpr, TPCH-Batch-DefaultBlockCpr"
        echo "  RGSIZE"
        echo "  HDFS_BUFFER_SIZES"
        echo "  OS_READ_AHEAD_BUFFER_SIZES"
        echo "  REPEAT_TIMES"
        echo "  SCALE_FACTOR"
        echo "  LOGDIR"
        echo "  HDFS_DATA_PATH"
        echo
}

if [ $# -lt 8 ]
then
        usage
        exit
fi

$@;

