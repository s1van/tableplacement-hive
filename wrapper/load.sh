#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
HDFS_BLK_SIZE=$($UTILD/orc-xonf.py --file=$HADOOP_HOME/conf/hdfs-site.xml --key=dfs.block.size --print)

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
	echo "-----RAW Files-----";
	$HADOOP fs -ls $HDFS_R 2>&1| awk '{print $NF}'| tail -n +4| xargs -I % $HADOOP fs -ls %;
	echo "-----HIVE Files-----";
	$HADOOP fs -ls $HIVE_R 2>&1| awk '{print $NF}'| tail -n +4| xargs -I % $HADOOP fs -ls %;
	echo "-----HIVE Blocks-----";
	$HADOOP fsck $HIVE_R -files -blocks;
}
	

SSB_BVARS="C_B_NUM D_B_NUM L_B_NUM P_B_NUM S_B_NUM";	#Bucket size used in templates
SSB_NBVALS="2837046 0 366303450 17139259 166676"; #Set 0 for fixed size table (594313001) 
SSB_CG_NBVALS="2837046 0 418791940 17139259 166676"; #Set 0 for fixed size table
SSB_HDFS_R=/ssb;
SSB_HIVE_R=/user/hive/warehouse/ssb;

ssb1() { base1 $TPLD/ssb.load.sql.1.template $3 ${SSB_HIVE_R}1 $1 "$SSB_BVARS" "$SSB_CG_NBVALS" $2; } #will increase the #bucket
ssb2() { base1 $TPLD/ssb.load.sql.2.template $3 ${SSB_HIVE_R}2 $1 "$SSB_BVARS" "$SSB_CG_NBVALS" $2; }
ssb3() { base1 $TPLD/ssb.load.sql.3.template $3 ${SSB_HIVE_R}3 $1 "$SSB_BVARS" "$SSB_CG_NBVALS" $2; }


TPCH_BVARS="C_B_NUM L_B_NUM N_B_NUM O_B_NUM P_B_NUM PS_B_NUM R_B_NUM S_B_NUM";
TPCH_NBVALS="24346144 749863287 0 171952161 24135125 118984616 0 1409184";
TPCH_CG_NBVALS="30346144 813630406 0 171952161 24135125 118984616 0 1409184";
TPCH_HDFS_R=/tpch;
TPCH_HIVE_R=/user/hive/warehouse/tpch;

tpch1() { base1 $TPLD/tpch.load.sql.1.template $3 ${TPCH_HIVE_R}1 $1 "$TPCH_BVARS" "$TPCH_CG_NBVALS" $2; } #will increase the #bucket
tpch2() { base1 $TPLD/tpch.load.sql.2.template $3 ${TPCH_HIVE_R}2 $1 "$TPCH_BVARS" "$TPCH_CG_NBVALS" $2; }
tpch3() { base1 $TPLD/tpch.load.sql.3.template $3 ${TPCH_HIVE_R}3 $1 "$TPCH_BVARS" "$TPCH_CG_NBVALS" $2; }
tpch_q6() { base1 $TPLD/tpch.load.sql.cg_q6.template $3 ${TPCH_HIVE_R}_q6 $1 "$TPCH_BVARS" "$TPCH_CG_NBVALS" $2; }


SSDB_BVARS="C_B_NUM";
SSDB_HDFS_R=/ssdb;
SSDB_HIVE_R=/user/hive/warehouse/ssdb;

#SCALE = tiny(500MiB), small(99GiB), normal(999GiB), large(9.9TiB) while we only take one cycle
ssdb_tiny() { base1 $1 $2 $3 $5 "$SSDB_BVARS" "579067130" $4; }	#TPL, HDFS_PATH, HIVE_PATH, HEAD, SIZE_FACTOR(affected by SerDe)
ssdb_small() { base1 $1 $2 $3 $5 "$SSDB_BVARS" "16878055320" $4; }
ssdb_normal() { base1 $1 $2 $3 $5 "$SSDB_BVARS" "67995457900" $4; }
ssdb_large() { base1 $1 $2 $3 $5 "$SSDB_BVARS" "278600130760" $4; }

ssdb_default() { ssdb_$1 $TPLD/ssdb.load.sql.default.template $3 ${SSDB_HIVE_R}1 $2 '0.55'; } 
ssdb_v1() { ssdb_$1 $TPLD/ssdb.load.sql.v1.template $3 ${SSDB_HIVE_R}2 $2 '0.62'; } 

##################
###    main    ###
##################
FUNC=$1;
SCALE=$2;
LOAD_HEAD=$3;
HDFS_DATA_PATH=$4;

echo $@;
$FUNC $SCALE $LOAD_HEAD $HDFS_DATA_PATH;
