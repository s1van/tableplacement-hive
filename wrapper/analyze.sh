#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
DEV=$(echo $MONDEV| sed 's/dev//g'| sed 's/\///g');
HOSTLIST="$(cat $HADOOP_HOME/conf/slaves)";

###########
##Formats##
#########
F_CPU_TIME="Total MapReduce CPU Time Spent\t \t1\t0\t6\t8";
f_job() {
        local NUM=$1;
        echo "Job ${NUM}:\t \t1\t0\t4\t7\t12\t18\t21"
}
###########################################################

stat1() {
	local RES=$1;
	local OUT=$2;
	local COLNAMES=$3;
	
	$UTILD/stat.sh quatile-var $RES $COLNAMES $OUT;
}

iostat1() {
	local HOST=$1;
	local DEVICE=$2;
	local STAT=$3;
	local OUT=$4;

	local RES=$(mktemp);
	local TFILE=$(mktemp);
	local CUM=$(mktemp);
	cat $STAT| grep $HOST| grep $DEVICE| awk 'BEGIN{isStart=1} {if(isStart == 1) {r1=$6;w1=$7; isStart=0;} 
							else {r2=$6;w2=$7; print r2-r1, w2-w1; isStart=1;} }' > $RES;
	$UTILD/stat.sh quatile-var $RES "${HOST}_Read_KB,${HOST}_Write_KB" $CUM
	paste $OUT $CUM > $TFILE;

	mv $TFILE $OUT;
	rm $RES $CUM;
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


extract() {
	local LOG=$1;
	local FORMAT="$2";

	BASE=$(echo $LOG| sed 's/.log//g');

	RRES=${BASE}.res;
	echo "Extract Results into $RRES ..."
        $UTILD/reshape.py --format="$FORMAT" --input=$LOG >> $RRES;

	local IOSTAT=${BASE}.iostat;
	local STAT=${BASE}.stat;
	local MAP=${BASE}.mapper;
	local REDUCE=${BASE}.reducer;
	local MISC=${BASE}.misc;

	local TMP1=$(mktemp);
	local TMP2=$(mktemp);

	awk 'BEGIN{OFS="\t"} {print $7,$8,$9,$3,$5,$6,$4}' $RRES| paste - $MISC > $TMP1;
	COLNAMES='Cumlulative_CPU,HDFS_Read,HDFS_Write,Time_taken,#Mapper,#Reducer,#Row,MapPhase,ReducePhase,MRJobTime';
	
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
	rm $TMP1 $TMP2 $RRES;
}

ssb-batch-extract() {
	local DIR=$1;

	local F_JOB_TIME="Time taken:\t \t1\t0\t3\t6";

	echo "Generate reshape.py format ..."
        FORMAT="${F_CPU_TIME}\n${F_JOB_TIME}\n$(f_job 0)";
        echo -e "${FORMAT}" > $DIR/res.format;	

	LOGS="$(find $DIR -iname '*.log')";
	for log in $LOGS; do 
		extract $log $DIR/res.format;
	done
}

tpch-batch-extract() {
	local DIR=$1;

	local F_JOB_TIME="Time taken:\t \t3\t3\t3\t3"; #last entry is a dummy

	echo "Generate reshape.py format ..."
        FORMAT="${F_CPU_TIME}\n${F_JOB_TIME}\n${F_JOB_ROW}\n$(f_job 0)";
        echo -e "${FORMAT}" > $DIR/res.format;	

	LOGS="$(find $DIR -iname '*.log')";
	for log in $LOGS; do 
		extract $log $DIR/res.format;
	done
}


call-list-stat() {
	local DIR=$1;
	local KEYWORD=$2;
	local QUERY=$3;
	
	#query.prefix_query.suffix_Hxx_Oxx
	list-stat $DIR $KEYWORD| sed -e "s@\"${KEYWORD}\"@@g" | awk -F'_' 'BEGIN{OFS="\t"} {print $1"."$2, substr($3,2), substr($4,2)}'| grep "$QUERY"; 
}

batch-list-stat() {
	local DIR=$1;
	local PREFIX=$2;
	local ATTR=$3;

	lists=$(ls $DIR| grep "${PREFIX}-RG");
	for list in $lists; do
		RGSIZE=$(echo $list| sed -e "s@${PREFIX}-RG@@g");
		#insert Row Group Size, remove #reducer, #row
		call-list-stat $DIR/$list $ATTR | awk -v rg=$RGSIZE '{OFS="\t"} {
			$9=$10=""; 
			$1 = $1 OFS rg; 
			print $0;}' | sed -e 's/\t\t/\t/g' -e 's/\t\t/\t/g'
	done
}

summarize() {
	local DIR=$1;
	local PREFIXES="$(echo $2| sed 's/,/ /g')";

	local MEDIAN1=$(mktemp);
	local MEAN_RIO=$(mktemp);
	local MEAN=$(mktemp);
	local VARIANCE=$(mktemp);

	COL_CONF='Query\tRowGroup_Size:M\tHDFS_BufSize:KB\tOS_BufSize:KB';	#1-4
	COL_HIVE='CPU:s\tHDFS_Read:B\tHDFS_Write:B\tHive_Job:s\t#Mapper';	#5-9
	COL_HADOOP='MapPhase:s\tReducePhase:s\tHADOOP_Job:s';			#10-12
	COL_MR='E(Mapper):s\tE(Reducer):s\tSD(Mapper):s\tSD(Reducer):s'		#13-14
	COL_IOSTAT='REAL_READ:KB\tREAL_WRITE:KB'				#15-

	echo -e "TablePlacement\t${COL_CONF}\t${COL_HIVE}\t${COL_HADOOP}\t${COL_MR}\t${COL_IOSTAT}";

	for prefix in $PREFIXES; do
		batch-list-stat $DIR $prefix Median| cut -f -12 > $MEDIAN1;
		batch-list-stat $DIR $prefix Mean| cut -f 15- > $MEAN_RIO;
		batch-list-stat $DIR $prefix Mean| cut -f 13,14 > $MEAN;
		batch-list-stat $DIR $prefix Variance| cut -f 13,14 > $VARIANCE;
		$UTILD/stat.sh colsum_odd_even $MEAN_RIO| paste $MEDIAN1 $MEAN $VARIANCE - | awk -v pf=$prefix 'BEGIN{OFS="\t"} {
			$1 = pf OFS $1;
			print $0;}';
	done

	rm $MEDIAN1 $MEAN_RIO $MEAN $VARIANCE;
}


##################
###    main    ###
##################
$@;

