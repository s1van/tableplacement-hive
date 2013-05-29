#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;


stat1() {
	local RES=$1;
	local OUT=$2;
	local COLNAMES=$3;
	
	$UTILD/stat.sh quatile-var $RES $COLNAMES > $OUT;
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

	OUT=$(echo $RRES| sed 's/.res/.stat/g');
	RES=$(mktemp);
	awk '{print $7,$8,$9,$3,$5,$6,$4}' $RRES > $RES;
	COLNAMES='Cumlulative_CPU,HDFS_Read,HDFS_Write,Time_taken,#Mapper,#Reducer,#Row';
	
	stat1 $RES $OUT "$COLNAMES"
	
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

	COLNAMES='Cumlulative_CPU\tHDFS_Read\tHDFS_Write\tTime_taken\t#Mapper\t#Reducer\t#Row';
	echo -e "Query\tHDFS Buffer\tOS Buffer\t$COLNAMES";
	list-stat $DIR $KEYWORD| sed -e "s@${KEYWORD}@@g" | awk -F'_' '{print $1"."$2, substr($3,2), substr($4,2)}'| grep "$QUERY"; 
}


##################
###    main    ###
##################
$@;

