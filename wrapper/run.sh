#!/bin/bash

CDIR=`dirname $0`/../;
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;


base() {
	local SQL=$1;
	local HEADSET=$2;
	
	DLOAD_SQL=$(mktemp);
	echo "Set Hive parameters ...";
	cat $HEADSET >> $DLOAD_SQL;
	cat $CONFD/sql.head >> $DLOAD_SQL;
	
	echo "Cleanup cache...";
	$UTILD/cache-cleanup.sh -f;
	sleep $(grep Cached /proc/meminfo| awk 'BEGIN{s=0} {s=1+int($2/(4*1024^2))} END{print s}');
	
	cat $SQL >> $DLOAD_SQL;
	cat $DLOAD_SQL;
	
	echo "Execute Query ..."
	hive -f $DLOAD_SQL;
	
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

