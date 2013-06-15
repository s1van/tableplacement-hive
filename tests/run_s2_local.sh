#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
TESTD=$CDIR/tests;

$TESTD/run_all.sh 2 1 2 /tmp/hive-test $HOME/store /tmp/hadoop_tmp $1;
$TESTD/run_all_ssdb.sh small 1 2 /tmp/hive-test $HOME/store /tmp/hadoop_tmp $1;
