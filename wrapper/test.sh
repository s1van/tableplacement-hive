#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
WRAPD=$CDIR/wrapper;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

#Formats
F_CPU_TIME="Total MapReduce CPU Time Spent\t \t1\t0\t6\t8";
F_JOB_TIME="Time taken:\t \t1\t0\t3\t6";
f_job() { 
	local NUM=$1;	
	echo "Job ${NUM}:\t \t1\t0\t4\t7\t12\t18\t21"
}

run_query() {
	local SQL=$1;
	local REP=$2;
	local FORMAT="$3";
	local OUT=$4;
	local RES=$5;
	local HEADSET=$6;
	
	echo "Execute Query $SQL for $REP times ..."
	touch $OUT;
	for i in $(seq 1 $REP); do
		$WRAPD/run.sh $SQL $HEADSET >> $OUT 2>&1;
	done

	echo "Extract Results into $RES ..."
	$UTILD/reshape.py --string="$FORMAT" --input=$OUT >> $RES;
}


ssb() { 
	local REP=$1;
	local SCALE=$2;
	local LOAD_WHICH=$3;	#corresponding to templates (in reference to util/load.sh)
	local HEADSET=$4;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$5;

	echo "Loading Data into Hive ... [$@]"
	
	$WRAPD/load.sh $LOAD_WHICH $SCALE $HEADSET > $OUTDIR/ssb_load.sql 2>&1;
	
	echo "Queries ..."
	FORMAT="${F_CPU_TIME}\n${F_JOB_TIME}\n$(f_job 0)";
	echo -e "${FORMAT}" >> $OUTDIR/res.format;
	run_query ssb1_1 $REP "$(echo -e $FORMAT)" $OUTDIR/ssb1_1.log $OUTDIR/ssb1_1.res $HEADSET 
	run_query ssb1_2 $REP "$(echo -e $FORMAT)" $OUTDIR/ssb1_2.log $OUTDIR/ssb1_2.res $HEADSET
	run_query ssb1_3 $REP "$(echo -e $FORMAT)" $OUTDIR/ssb1_3.log $OUTDIR/ssb1_3.res $HEADSET
}

SSB-Batch() {
	local RGSIZE=$1;
        local LOAD_WHICH=$2; # column group, other optimizations
        local HDFS_BUF_SIZES="$(echo $3| sed 's/,/ /g')";
        local OS_READAHEAD_SIZES="$(echo $4| sed 's/,/ /g')";
        local REP=$5;
        local SCALE=$6;
        local OUTDIR=$7;
	
	echo "SSB-Batch $@"
	mkdir -p $OUTDIR;
	source $CONFD/site.conf;

	echo "Delete all tables in current metastore"
	hive -e 'show tables' | xargs -I {} hive -e "drop table if exists {}";
	hive -e 'show tables' | xargs -I {} hive -e "drop view if exists {}";

	echo "Store the Parameters"
	touch $OUTDIR/README;
	echo "Benchmark RGSIZE LOAD_WITCH HDFS_BUF_SIZE OS_READAHEAD REP SCALE OUTDIR" >> $OUTDIR/README;
	echo $@ >> $OUTDIR/README;

	echo "Set HDFS Buffer Size, Row Group Size"
	VARS="HDFS_BUF_SIZE RGSIZE";
	VALS="524288 $(($RGSIZE * 1024 * 1024))";
	HEADSET=$(mktemp);
	$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
	cat $HEADSET
	echo "Set OS Readahead Buffer"
	sudo blockdev --setra 256 $DEVICE;
	
	echo "Start Testing ..."
	$WRAPD/load.sh $LOAD_WHICH $SCALE $HEADSET > $OUTDIR/ssb_load.sql 2>&1;
	
	echo "Queries ..."
	FORMAT="${F_CPU_TIME}\n${F_JOB_TIME}\n$(f_job 0)";
	echo -e "${FORMAT}" >> $OUTDIR/res.format;

	for bufsize in $HDFS_BUF_SIZES; do
		for osbuf in $OS_READAHEAD_SIZES; do
			echo "Set HDFS Buffer Size, OS Readahead Buffer"
			VALS="$(($bufsize * 1024)) $(($RGSIZE * 1024))";
			sudo blockdev --setra $osbuf $DEVICE;
			
			$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
			cat $HEADSET

			run_query ssb1_1 $REP "$(echo -e $FORMAT)" $OUTDIR/ssb1_1_H${bufsize}_O${osbuf}.log $OUTDIR/ssb1_1_H${bufsize}_O${osbuf}.res $HEADSET 
			run_query ssb1_2 $REP "$(echo -e $FORMAT)" $OUTDIR/ssb1_2_H${bufsize}_O${osbuf}.log $OUTDIR/ssb1_2_H${bufsize}_O${osbuf}.res $HEADSET
			run_query ssb1_3 $REP "$(echo -e $FORMAT)" $OUTDIR/ssb1_3_H${bufsize}_O${osbuf}.log $OUTDIR/ssb1_3_H${bufsize}_O${osbuf}.res $HEADSET
		done
	done

	rm $HEADSET;
}

Test() {
	local BENCHMARK=$1;
	local RGSIZE=$2;
	local LOAD_WHICH=$3; # column group, other optimizations
	local HDFS_BUF_SIZE=$4;
	local OS_READAHEAD_SIZE=$5;
	local REP=$6;
	local SCALE=$7;
	local OUTDIR=$8;
	
	mkdir -p $OUTDIR;
	source $CONFD/site.conf;

	echo "Delete all tables in current metastore"
	hive -e 'show tables' | xargs -I {} hive -e "drop table if exists {}";
	hive -e 'show tables' | xargs -I {} hive -e "drop view if exists {}";

	echo "Store the Parameters"
	touch $OUTDIR/README;
	echo "Benchmark RGSIZE LOAD_WITCH HDFS_BUF_SIZE OS_READAHEAD REP SCALE OUTDIR" >> $OUTDIR/README;
	echo $@ >> $OUTDIR/README;

	
	echo "Set HDFS Buffer Size, Row Group Size"
	VARS="HDFS_BUF_SIZE RGSIZE";
	VALS="$HDFS_BUF_SIZE $RGSIZE";
	HEADSET=$(mktemp);
	$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template >> $HEADSET;
	cat $HEADSET
	echo "Set OS Readahead Buffer"
	sudo blockdev --setra $OS_READAHEAD_SIZE $DEVICE;
	
	echo "Start Testing ..."
	$BENCHMARK $REP $SCALE $LOAD_WHICH $HEADSET $OUTDIR;

	rm $HEADSET;
}


##################
###    main    ###
##################
$@;

