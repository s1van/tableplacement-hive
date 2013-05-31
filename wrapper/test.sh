#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
SLAVE=$HADOOP_HOME/conf/slaves;
DEV=$(echo $DEVICE| sed 's/dev//g'| sed 's/\///g');

###########
##Formats##
#########
F_CPU_TIME="Total MapReduce CPU Time Spent\t \t1\t0\t6\t8";
F_JOB_TIME="Time taken:\t \t1\t0\t3\t6";
f_job() { 
	local NUM=$1;	
	echo "Job ${NUM}:\t \t1\t0\t4\t7\t12\t18\t21"
}
###########################################################

run_query() {
	local SQL=$1;
	local FORMAT="$2";
	local OUT=$3;
	local RES=$4;
	local IOS=$5;
	local HEADSET=$6;
	
	echo "Cleanup Cache...";
        $UTILD/cache-cleanup.sh -b;

	echo "Execute Query $SQL"
	pdsh -R ssh -w ^${SLAVE} iostat -d -t -k $DEVICE >> $IOS;
	$WRAPD/run.sh $SQL $HEADSET >> $OUT 2>&1;
	pdsh -R ssh -w ^${SLAVE} iostat -d -t -k $DEVICE >> $IOS;

	echo "Extract Results into $RES ..."
	$UTILD/reshape.py --string="$FORMAT" --input=$OUT >> $RES;
}


ssb-load() { 
	local SCALE=$1;
	local LOAD_WHICH=$2;	#corresponding to templates (in reference to util/load.sh)
	local HEADSET=$3;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$4;

	$WRAPD/load.sh $LOAD_WHICH $SCALE $HEADSET > $OUTDIR/ssb_load.sql 2>&1;
}

ssb-query() { 
	local FORMAT="$1";
	local TAG=$2;	
	local HEADSET=$3;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$4;

	echo "Execute Queries ... [$TAG $HEADSET $OUTDIR]"
	run_query ssb1_1 "$FORMAT" $OUTDIR/ssb1_1_${TAG}.log $OUTDIR/ssb1_1_${TAG}.res $OUTDIR/ssb1_1_${TAG}.iostat $HEADSET 
	run_query ssb1_2 "$FORMAT" $OUTDIR/ssb1_2_${TAG}.log $OUTDIR/ssb1_2_${TAG}.res $OUTDIR/ssb1_2_${TAG}.iostat $HEADSET
	run_query ssb1_3 "$FORMAT" $OUTDIR/ssb1_3_${TAG}.log $OUTDIR/ssb1_3_${TAG}.res $OUTDIR/ssb1_3_${TAG}.iostat $HEADSET
}


batch() {
	local RGSIZE=$1;
        local LOAD_WHICH=$2; # column group, other optimizations
        local HDFS_BUF_SIZES="$(echo $3| sed 's/,/ /g')";
        local OS_READAHEAD_SIZES="$(echo $4| sed 's/,/ /g')";
        local REP=$5;
        local SCALE=$6;
        local OUTDIR=$7;
	local F_LOAD=$8;
	local F_QUERY=$9;
	
	mkdir -p $OUTDIR;

	echo "Delete all tables in current metastore"
	hive -e 'show tables' | xargs -I {} hive -e "drop table if exists {}";
	hive -e 'show tables' | xargs -I {} hive -e "drop view if exists {}";

	echo "Store the Parameters"
	touch $OUTDIR/README;
	echo "Benchmark RGSIZE LOAD_WITCH HDFS_BUF_SIZE OS_READAHEAD REP SCALE OUTDIR" >> $OUTDIR/README;
	echo $@ >> $OUTDIR/README;

	echo "Set HDFS Buffer Size, Row Group Size ${RGSIZE}MiB for Loading"
	VARS="HDFS_BUF_SIZE RGSIZE";
	VALS="524288 $(($RGSIZE * 1024 * 1024))";
	HEADSET=$(mktemp);
	$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
	cat $HEADSET
	echo "Set OS Readahead Buffer 256 blocks"
	sudo blockdev --setra 256 $DEVICE;
	
	echo "Start Testing ..."

	echo "Loading Data into Hive ... [$F_LOAD $SCALE $LOAD_WHICH $HEADSET $OUTDIR]"
	$F_LOAD $SCALE $LOAD_WHICH $HEADSET $OUTDIR;
	
	echo "Execute Batched Queries ..."
	FORMAT="${F_CPU_TIME}\n${F_JOB_TIME}\n$(f_job 0)";
	echo -e "${FORMAT}" >> $OUTDIR/res.format;

	for rnum in $(seq 1 $REP); do
		for bufsize in $HDFS_BUF_SIZES; do
			for osbuf in $OS_READAHEAD_SIZES; do
				echo -e "\nSet HDFS Buffer Size ${bufsize}KB, OS Readahead Buffer ${osbuf}KB"
				VALS="$(($bufsize * 1024)) $(($RGSIZE * 1024))";
				sudo blockdev --setra $osbuf $DEVICE;
				
				$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
				cat $HEADSET
				$F_QUERY "$(echo -e $FORMAT)" "H${bufsize}_O${osbuf}" $HEADSET $OUTDIR
			done
		done
	done

	rm $HEADSET;
}

SSB-Batch() {
	echo "SSB-Batch [$@]"
	batch $1 $2 $3 $4 $5 $6 $7 ssb-load ssb-query;
}


##################
###    main    ###
##################
$@;

