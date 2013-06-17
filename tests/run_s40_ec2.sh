#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
TESTD=$CDIR/tests;

$TESTD/run_all.sh 40 1 2 /home/ubuntu/expr/s40 $HOME/store /mnt/hadoop/data $1;
#$TESTD/run_all_ssdb.sh small 1 2 /home/ubuntu/expr/s40 $HOME/store /mnt/hadoop/data $1;
