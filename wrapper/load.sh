#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
HDFS_BLK_SIZE=$($UTILD/orc-xonf.py --file=$HDFS_HOME/conf/hdfs-site.xml --key=dfs.block.size --print)

base1() {
	local TEMPLATE=$1;
	local HDFS_R=$2;
	local HIVE_R=$3;
	local SCALE=$4;
	local BVARS=$5;
	local NBVALS=$6; #Normalized Bucket Values
	local LOAD_HEAD=$7;
	
	local VARS="HDFS_ROOT HIVE_ROOT $BVARS";
	local BVALS="";
	for val in $NBVALS; do
		val=$(python -c "print int($val*$SCALE/$HDFS_BLK_SIZE)+1");
		BVALS="${BVALS}${val} ";
	done
	
	DLOAD_SQL=$(mktemp);
	echo "Generate hive parameters"
	cat $LOAD_HEAD|tee $DLOAD_SQL;

	VALS="${HDFS_R} $HIVE_R $BVALS";
	echo "####`echo $0| awk -F/ '{print $NF}'`####";
	echo "Generate load_data.sql with parameter $VALS"
	$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TEMPLATE >> $DLOAD_SQL;
	cat $DLOAD_SQL;
	
	echo "Load Data ..."
	$HIVE -f $DLOAD_SQL;
	
	rm -f $DLOAD_SQL;
}
	

SSB_BVARS="C_B_NUM D_B_NUM L_B_NUM P_B_NUM S_B_NUM";
SSB_NBVALS="2837046 0 594313001 17139259 166676"; #Set 0 for fixed size table
SSB_HDFS_R=/ssb;
SSB_HIVE_R=/user/hive/warehouse/ssb;

ssb1() { base1 $TPLD/ssb.load.sql.1.template $3 ${SSB_HIVE_R}1 $1 "$SSB_BVARS" "$SSB_NBVALS" $2; }
ssb2() { base1 $TPLD/ssb.load.sql.2.template $3 ${SSB_HIVE_R}2 $1 "$SSB_BVARS" "$SSB_NBVALS" $2; }
ssb3() { base1 $TPLD/ssb.load.sql.3.template $3 ${SSB_HIVE_R}3 $1 "$SSB_BVARS" "$SSB_NBVALS" $2; }

TPCH_BVARS="C_B_NUM L_B_NUM N_B_NUM O_B_NUM P_B_NUM PS_B_NUM R_B_NUM S_B_NUM";
TPCH_NBVALS="24346144 759863287 0 171952161 24135125 118984616 0 1409184";
TPCH_HDFS_R=/tpch;
TPCH_HIVE_R=/user/hive/warehouse/tpch;

tpch2() { base1 $TPLD/tpch.load.sql.2.template $3 ${TPCH_HIVE_R}2 $1 "$TPCH_BVARS" "$TPCH_NBVALS" $2; }


##################
###    main    ###
##################
FUNC=$1;
SCALE=$2;
LOAD_HEAD=$3;
HDFS_DATA_PATH=$4;

echo $@;
$FUNC $SCALE $LOAD_HEAD $HDFS_DATA_PATH;
