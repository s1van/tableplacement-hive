#!/bin/bash
CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
TESTD=$CDIR/tests;

$TESTD/run_all.sh 100 3 3 $HOME/expr $HOME/store /mnt/hadoop/data $1;
