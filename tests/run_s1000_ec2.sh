#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
TESTD=$CDIR/tests;

$TESTD/run_all.sh 1000 3 3 $HOME/expr/s1000 $HOME/store /mnt/hadoop/data $1;
$TESTD/run_all_ssdb.sh large 3 3 $HOME/expr/s1000 $HOME/store /mnt/hadoop/data $1;
