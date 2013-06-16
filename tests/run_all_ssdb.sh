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


SSDB_DIR=$SDIR/ssdb
mkdir -p $SSDB_DIR

echo "Init ... "
#$WRAPD/setup.sh -b 256 -h 512,1280 -c 1,1 -r $REPLICA -d $HADOOP_TMP -i;


sleep 8;
echo "Test ssdb ... "
$WRAPD/dgen.sh -s ${SCALE} -p /ssdb_${SCALE} -f ssdb;
mail -s "ssdb ${SCALE} generated" </dev/null "$EMAIL";
#read -e;

cd $SSDB_DIR && $TESTD/run_ssdb.sh ${SCALE} $REP $LOGDIR/ssdb/ssdb_${SCALE} /ssdb_${SCALE};
mail -s "ssdb tests complete!" </dev/null "$EMAIL";

$HADOOP fs -rmr /ssdb_${SCALE}
cd $TPCH_DIR && hive -e 'drop table cycle;'

