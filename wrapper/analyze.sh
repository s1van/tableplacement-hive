#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
DEV=$(echo $DEVICE| sed 's/dev//g'| sed 's/\///g');
HOSTLIST="$(cat $HADOOP_HOME/conf/slaves)";

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

stat1() {
	local RES=$1;
	local OUT=$2;
	local COLNAMES=$3;
	
	$UTILD/stat.sh quatile-var $RES $COLNAMES > $OUT;
}

iostat1() {
	local HOST=$1;
	local DEVICE=$2;
	local STAT=$3;
	local OUT=$4;

	local RES=$(mktemp);
	local TFILE=$(mktemp);
	cat $STAT| grep $HOST| grep $DEVICE| awk 'BEGIN{isStart=1} {if(isStart == 1) {r1=$6;w1=$7; isStart=0;} 
							else {r2=$6;w2=$7; print r2-r1, w2-w1; isStart=1;} }' > $RES;
	$UTILD/stat.sh quatile-var $RES "${HOST}_Read_KB,${HOST}_Write_KB"| paste $OUT - > $TFILE;
	mv $TFILE $OUT;

	rm $RES;
}

list-stat() {
        local DIR=$1;
        local ROWNAME=$2;

        RESS="$(find $DIR -iname '*.stat')";
	for res in $RESS; do
		line=$(grep $ROWNAME $res);
		echo -e "$res\t$line"|sed 's/.stat//g'| sed -e "s@${DIR}/@@g";
	done
}


ssb() {
	local LOG=$1;
	local FORMAT="$2";

	BASE=$(echo $LOG| sed 's/.log//g');

	RRES=${BASE}.res;
	echo "Extract Results into $RRES ..."
        $UTILD/reshape.py --format="$FORMAT" --input=$LOG >> $RRES;

	IOSTAT=${BASE}.iostat;
	STAT=${BASE}.stat;
	MAP=${BASE}.mapper;
	REDUCE=${BASE}.reducer;
	TMP1=$(mktemp);
	TMP2=$(mktemp);

	awk '{print $7,$8,$9,$3,$5,$6,$4}' $RRES> $TMP1;
	COLNAMES='Cumlulative_CPU,HDFS_Read,HDFS_Write,Time_taken,#Mapper,#Reducer,#Row';
	
	echo "Refine $TMP1 to form $STAT ..."
	stat1 $TMP1 $STAT "$COLNAMES";

	echo "Distill $MAP to form $STAT"
	sed 's/\t/\n/g' $MAP > $TMP2;
	stat1 $TMP2 $TMP1 "Mapper";
	paste $STAT $TMP1 > $TMP2;
	cp $TMP2 $STAT;

	echo "Distill $REDUCE to form $STAT"
	sed 's/\t/\n/g' $REDUCE > $TMP2;
	stat1 $TMP2 $TMP1 "Reducer";
	paste $STAT $TMP1 > $TMP2;
	cp $TMP2 $STAT;

	for host in $HOSTLIST; do
		iostat1 $host $DEV $IOSTAT $STAT;
	done
	
	echo "";
	rm $TMP1 $TMP2;
}

batch-extract() {
	local DIR=$1;

	echo "Generate reshape.py format ..."
        FORMAT="${F_CPU_TIME}\n${F_JOB_TIME}\n$(f_job 0)";
        echo -e "${FORMAT}" > $DIR/res.format;	

	RESS="$(find $DIR -iname '*.log')";
	for res in $RESS; do 
		ssb $res $DIR/res.format;
	done
}

ssb-list-stat() {
	local DIR=$1;
	local KEYWORD=$2;
	local QUERY=$3;

	COLNAMES='CPU:s\tHread:B\tHwrite:B\tTotal:s\t#Mapper\t#Reducer\t#Row\tMapper:s\tReducer:s\tRR:KB\tRW:KB';
	echo -e "Query\tHBuf:KB\tOBuf:KB\t$COLNAMES";
	list-stat $DIR $KEYWORD| sed -e "s@${KEYWORD}@@g" | awk -F'_' '{print $1"."$2, substr($3,2), substr($4,2)}'| grep "$QUERY"; 
}

batch-list-stat() {
	local DIR=$1;
	local PREFIX=$2;
	local ATTR=$3;

	lists=$(ls $DIR| grep "${PREFIX}-RG");
	for list in $lists; do
		RGSIZE=$(echo $list| sed -e "s@${PREFIX}-RG@@g");
		ssb-list-stat $DIR/$list $ATTR | awk -v rg=$RGSIZE '{OFS="\t"} {
			$8=$9=$10=""; 
			if (NR==1) $1 = $1 OFS "RG:M"; 
			else $1 = $1 OFS rg; 
			print $0;}' | sed -e 's/\t\t/\t/g' -e 's/\t\t/\t/g'
	done
}

summarize() {
	local DIR=$1;
	local PREFIXES="$(echo $2| sed 's/,/ /g')";

	local MEDIAN1=$(mktemp);
	local MEDIAN2=$(mktemp);
	local MEAN=$(mktemp);
	local VARIANCE=$(mktemp);

	for prefix in $PREFIXES; do
		batch-list-stat $DIR $prefix Median| cut -f -8 > $MEDIAN1;
		batch-list-stat $DIR $prefix Median| cut -f 11- > $MEDIAN2;
		batch-list-stat $DIR $prefix Mean| cut -f 9,10 > $MEAN;
		batch-list-stat $DIR $prefix Variance| cut -f 9,10 > $VARIANCE;
		paste $MEDIAN1 $MEAN $VARIANCE $MEDIAN2| awk -v pf=$prefix 'BEGIN{OFS="\t"} {
			if(NR==1) $1 = "TPL" OFS $1;
			else $1 = pf OFS $1;
			print $0;}';
	done

	rm $MEDIAN1 $MEDIAN2 $MEAN $VARIANCE;
}


##################
###    main    ###
##################
$@;

