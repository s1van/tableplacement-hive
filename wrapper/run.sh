#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

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


tpch6() { base $TPCHD/q6_forecast_revenue_change.hive; }

##################
###    main    ###
##################
$@;

