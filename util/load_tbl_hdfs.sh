#!/bin/bash

usage()
{
	echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
	echo "[description]:"
	echo "  load tables in one local fs directoy into one hdfs direcotry"
	echo "[option]:"
	echo "  -s  source"
	echo "		"
	echo "  -t  target"
	echo ""
	echo
}

if [ $# -lt 4 ]
then
	usage
	exit
fi

while getopts "s:t:" OPTION
do
        case $OPTION in
                s)
                        FROM=$OPTARG;
                        ;;
                t)
                        TO=$OPTARG;
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done
CONFD=`dirname $0`/../conf;
source $CONFD/site.conf;

HDFS="$HADOOP fs";

TABLES=$(ls $FROM| grep '.tbl'| sed 's/.tbl//g');
for tbl in $TABLES; do
	$HDFS -mkdir $TO/$tbl;
	$HDFS -copyFromLocal $FROM/${tbl}.tbl $TO/$tbl;
done

