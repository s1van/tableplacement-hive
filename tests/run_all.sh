#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
WRAPD=$CDIR/wrapper;
TESTD=$CDIR/tests;
CONFD=$CDIR/conf;
source $CONFD/site.conf;

SCALE=$1;
REPLICA=$2;
REP=$3;
LOGDIR=$4;
SDIR=$5;
HADOOP_TMP=$6;
EMAIL=$7;

if [ $# -lt 7 ]
then
	echo " input: $0 $@"
        echo "expect: $0 SCALE REPLICA REPEAT LOG_DIR HIVE_META_DIR HADOOP_TMP EMAIL"
        exit
fi


SSB_DIR=$SDIR/ssb
TPCH_DIR=$SDIR/tpch

echo "Init ... "
$WRAPD/setup.sh -b 256 -h 512,1024 -c 1,2 -r $REPLICA -d $HADOOP_TMP -i;


sleep 8;
echo "Test ssb ... "
$WRAPD/dgen.sh -s ${SCALE} -p /ssb_s${SCALE} -f ssb;
mail -s "ssb ${SCALE} generated" </dev/null "$EMAIL";
#read -e;

cd $SSB_DIR && $TESTD/run_ssb.sh ${SCALE} $REP $LOGDIR/ssb/ssb_s${SCALE} /ssb_s${SCALE};
mail -s "ssb tests complete!" </dev/null "$EMAIL";

$HADOOP fs -rmr /ssb_s${SCALE}
cd $SSB_DIR && hive -e 'drop table lineorder;'


echo "Test tpch ... "
$WRAPD/dgen.sh -s ${SCALE} -p /tpch_s${SCALE} -f tpch;
mail -s "tpch ${SCALE} generated" </dev/null "$EMAIL";
#read -e;

cd $TPCH_DIR && $TESTD/run_tpch.sh ${SCALE} $REP $LOGDIR/tpch/tpch_s${SCALE} /tpch_s${SCALE};
mail -s "tpch tests complete!" </dev/null "$EMAIL";
read -e;

$HADOOP fs -rmr /tpch_s${SCALE}
cd $TPCH_DIR && hive -e 'drop table lineitem;'
