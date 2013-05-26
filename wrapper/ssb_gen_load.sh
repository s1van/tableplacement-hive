#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

TEMPLATE=$1;
HDFS_R=$2;
HIVE_R=$3;
SCALE=$4;
RGSIZE=$5;
COLNUM=$6;

VARS="HDFS_ROOT HIVE_ROOT RGSIZE C_B_NUM D_B_NUM L_B_NUM P_B_NUM S_B_NUM";
NBVALS="1 1 32 1 4";
BVALS="";
for val in $NBVALS; do
	BVALS="$BVALS $(($val * $SCALE))";
done

DLOAD_SQL=$(mktemp);
#VALS="/ssb /user/hive/warehouse/ssb 4194304 32 16 16 16 16 16";
VALS="$HDFS_R $HIVE_R 4194304 1 1 32 1 4";
echo "Generate load_data.sql with parameter $VALS"
#$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$CDIR/template/ssb.load.sql.2.template --output=$DLOAD_SQL;
$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TEMPLATE --output=$DLOAD_SQL;
cat $DLOAD_SQL;

echo "Load Data ..."
hive -f $DLOAD_SQL;

rm -f $DLOAD_SQL;
