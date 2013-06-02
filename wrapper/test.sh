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

update-mapper-info() {
	local LOG=$1;
	local MAP=$2;
	
	local TMP1=$(mktemp);
	echo "Deal with the last Job in $LOG ..."
	#JOBS=$(grep 'Tracking URL' $LOG| awk -F'=' '{print $3"="$4}'| sed 's/jobdetails/jobtasks/g');
	JOBS=$(grep 'Tracking URL' $LOG| awk -F'=' '{print $3"="$4}'| sed 's/jobdetails/jobtasks/g'| tail -1);
	JNAMES=$(grep 'Tracking URL' $LOG| awk -F'=' '{if(NR==1) printf "%s", $4; else printf ",%s", $4;}');
	touch $MAP;
	for job in $JOBS; do
	        curl $job'&type=map&pagenum=1' 2>/dev/null| grep sec|sed 's/\(.*\)(\([0-9]*\)sec)\(.*\)/\2/g'| paste - $MAP > $TMP1
	        cp $TMP1 $MAP;
	done
	
	rm $TMP1;
}

run_query() {
	local SQL=$1;
	local BASE=$2;
	local HEADSET=$3;

	local LOG=${BASE}.log;
	local IOS=${BASE}.iostat;
	local MAP=${BASE}.mapper;
	
	echo "Cleanup Cache...";
        $UTILD/cache-cleanup.sh -b;

	echo "Execute Query $SQL"
	pdsh -R ssh -w ^${SLAVE} iostat -d -t -k $DEVICE >> $IOS;
	$WRAPD/run.sh $SQL $HEADSET >> $LOG 2>&1;
	
	echo "Collect iostat info to $IOS"
	pdsh -R ssh -w ^${SLAVE} iostat -d -t -k $DEVICE >> $IOS;

	echo "Update Mapper info in $MAP"
	update-mapper-info $LOG $MAP;
}


ssb-load() { 
	local SCALE=$1;
	local LOAD_WHICH=$2;	#corresponding to templates (in reference to util/load.sh)
	local HEADSET=$3;	#includes RGSIZE, HDFS_BUF_SIZE
	local OUTDIR=$4;
	local HDFS_DATA_PATH=$5;

	$WRAPD/load.sh $LOAD_WHICH $SCALE $HEADSET $HDFS_DATA_PATH > $OUTDIR/ssb_load.sql 2>&1;
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


batch() {
	local RGSIZE=$1;
        local LOAD_WHICH=$2; # column group, other optimizations
        local HDFS_BUF_SIZES="$(echo $3| sed 's/,/ /g')";
        local OS_READAHEAD_SIZES="$(echo $4| sed 's/,/ /g')";
        local REP=$5;
        local SCALE=$6;
        local OUTDIR=$7;
	local HDFS_DATA_PATH=$8;
	local F_LOAD=$9;
	local F_QUERY=${10};
	
	mkdir -p $OUTDIR;

	echo "Delete all tables in current metastore"
	$HIVE -e 'show tables' | xargs -I {} $HIVE -e "drop table if exists {}";
	$HIVE -e 'show tables' | xargs -I {} $HIVE -e "drop view if exists {}";

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
	$F_LOAD $SCALE $LOAD_WHICH $HEADSET $OUTDIR $HDFS_DATA_PATH;
	
	for rnum in $(seq 1 $REP); do
		for bufsize in $HDFS_BUF_SIZES; do
			for osbuf in $OS_READAHEAD_SIZES; do
				echo -e "\nSet HDFS Buffer Size ${bufsize}KB, OS Readahead Buffer ${osbuf}KB"
				VALS="$(($bufsize * 1024)) $(($RGSIZE * 1024))";
				sudo blockdev --setra $(($osbuf * 2)) $DEVICE;
				
				$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TPLD/head.template > $HEADSET;
				cat $HEADSET
				$F_QUERY "H${bufsize}_O${osbuf}" $HEADSET $OUTDIR
			done
		done
	done

	rm $HEADSET;
}

SSB-Batch() {
	echo "SSB-Batch [$@]"
	batch $1 $2 $3 $4 $5 $6 $7 $8 ssb-load ssb-query;
}


##################
###    main    ###
##################
$@;

