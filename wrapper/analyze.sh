#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
DEV=$(echo $DEVICE| sed 's/dev//g'| sed 's/\///g');
HOSTLIST="$(cat $HADOOP_HOME/conf/slaves)";

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
	local RRES=$1;

	IOSTAT=$(echo $RRES| sed 's/.res/.iostat/g');
	OUT=$(echo $RRES| sed 's/.res/.stat/g');
	RES=$(mktemp);
	awk '{print $7,$8,$9,$3,$5,$6,$4}' $RRES > $RES;
	COLNAMES='Cumlulative_CPU,HDFS_Read,HDFS_Write,Time_taken,#Mapper,#Reducer,#Row';
	
	stat1 $RES $OUT "$COLNAMES";
	for host in $HOSTLIST; do
		iostat1 $host $DEV $IOSTAT $OUT;
	done
	
	rm $RES;
}

batch-ssb() {
	local DIR=$1;
	
	RESS="$(find $DIR -iname '*.res')";
	for res in $RESS; do 
		ssb $res;
	done
}

ssb-list-stat() {
	local DIR=$1;
	local KEYWORD=$2;
	local QUERY=$3;

	COLNAMES='CPU\tHDFS_Read\tHDFS_Write\tTime\t#Mapper\t#Reducer\t#Row';
	echo -e "Query\tHDFS_Buf\tOS_Buf\t$COLNAMES";
	list-stat $DIR $KEYWORD| sed -e "s@${KEYWORD}@@g" | awk -F'_' '{print $1"."$2, substr($3,2), substr($4,2)}'| grep "$QUERY"; 
}

batch-ssb-list-stat() {
	local DIR=$1;
	local PREFIX=$2;
	local ATTR=$3;

	lists=$(ls $DIR| grep "${PREFIX}-RG");
	for list in $lists; do
		RGSIZE=$(echo $list| sed -e "s@${PREFIX}-RG@@g");
		ssb-list-stat $DIR/$list $ATTR | awk -v rg=$RGSIZE '{OFS="\t"} {
			$8=$9=$10=""; 
			if (NR==1) $1 = $1 FS "RGsize"; 
			else $1 = $1 FS rg; 
			print $0;}' | sed 's/\t\t/\t/g'
	done
}


##################
###    main    ###
##################
$@;

