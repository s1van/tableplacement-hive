#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;
SSDBD=$CDIR/ssdb;

source $CONFD/site.conf;

base() {
	local SQL=$1;
	local HEADSET=$2;
	
	DLOAD_SQL=$(mktemp);
	echo "Set Hive parameters ...";
	cat $HEADSET >> $DLOAD_SQL;
	cat $CONFD/sql.head >> $DLOAD_SQL;
	
	cat $SQL >> $DLOAD_SQL;
	cat $DLOAD_SQL;
	
	echo "Execute Query ..."
	$HIVE -f $DLOAD_SQL;
	
	rm -f $DLOAD_SQL;
}
	

ssb1_1() { base $SSBD/q1_1 $1; }
ssb1_2() { base $SSBD/q1_2 $1; }
ssb1_3() { base $SSBD/q1_3 $1; }

ssb1_1_cg() { base $SSBD/q1_1.cg $1; }
ssb1_2_cg() { base $SSBD/q1_2.cg $1; }
ssb1_3_cg() { base $SSBD/q1_3.cg $1; }


tpch_q6() { base $TPCHD/q6_forecast_revenue_change.hive $1; }
tpch_q6_cg() { base $TPCHD/q6.cg.sql $1; }

ssdb_q1_easy() { base $SSDBD/ssdb.q1.global.v1.easy.sql $1; }
ssdb_q1_medium() { base $SSDBD/ssdb.q1.global.v1.medium.sql $1; }
ssdb_q1_hard() { base $SSDBD/ssdb.q1.global.v1.hard.sql $1; }

ssdb_q1_easy_cg() { base $SSDBD/ssdb.q1.global.v1.cg.easy.sql $1; }
ssdb_q1_medium_cg() { base $SSDBD/ssdb.q1.global.v1.cg.medium.sql $1; }
ssdb_q1_hard_cg() { base $SSDBD/ssdb.q1.global.v1.cg.hard.sql $1; }

##################
###    main    ###
##################
$@;
#Handle single query

