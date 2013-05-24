#!/bin/bash

TEMPLATE=$1;
HDFS_R=$2;
HIVE_R=$3;

CDIR=`dirname $0`;
UTILD=$CDIR/util;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

#VARS="HDFS_ROOT HIVE_ROOT RGSIZE COLNUM C_B_NUM L_B_NUM N_B_NUM O_B_NUM P_B_NUM PS_B_NUM R_B_NUM S_B_NUM";
VARS="HDFS_ROOT HIVE_ROOT RGSIZE COLNUM C_B_NUM L_B_NUM N_B_NUM O_B_NUM P_B_NUM PS_B_NUM R_B_NUM S_B_NUM";

DLOAD_SQL=$(mktemp);
#VALS="/tpch /user/hive/warehouse/tpch 4194304 32 16 16 16 16 16 16 16 16";
VALS="$HDFS_R $HIVE_R 4194304 32 1 16 1 2 1 2 1 1";
echo "Generate load_data.sql with parameter $VALS"
#$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$CDIR/template/tpch.load.sql.2.template --output=$DLOAD_SQL;
$UTILD/fillTemplate.py --vars="$VARS" --vals="$VALS" --template=$TEMPLATE --output=$DLOAD_SQL;
cat $DLOAD_SQL;

echo "Load Data ..."
hive -f $DLOAD_SQL;

rm -f $DLOAD_SQL;
