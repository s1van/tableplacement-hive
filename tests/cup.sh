#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
WRAPD=$CDIR/wrapper;
TESTD=$CDIR/tests;
CONFD=$CDIR/conf;

EMAIL=$1;

SSB_DIR=/home/ubuntu/store/ssb
TPCH_DIR=/home/ubuntu/store/tpch

#$WRAPD/setup.sh -b 256 -h 512,1024 -c 1,1 -r 3 -i;

#$WRAPD/dgen.sh -s 100 -p /tpch_s100 -f tpch;
#mail -s "tpch 100 generated" </dev/null "$EMAIL";
#read -e;

cd $TPCH_DIR && $TESTD/tpch_cup.sh /home/ubuntu/expr/tpch_s100_n10_m1;
mail -s "tpch_cup.sh completes!" </dev/null "$EMAIL";
read -e;

source $CONFD/site.conf;
$HADOOP fs -rmr /tpch_s100
cd $TPCH_DIR && hive -e 'drop table lineitem_s;'


$WRAPD/dgen.sh -s 100 -p /ssb_s100 -f ssb;
mail -s "ssb 100 generated" </dev/null "$EMAIL";
#read -e;

cd $SSB_DIR && $TESTD/ssb_cup.sh /home/ubuntu/expr/ssb_s100_n10_m1;
mail -s "ssb_cup.sh completes!" </dev/null "$EMAIL";
