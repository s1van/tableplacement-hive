#!/bin/bash

CDIR="$( cd "$( dirname "$0" )" && pwd )/../";
UTILD=$CDIR/util;
CONFD=$CDIR/conf;
TPLD=$CDIR/template;
TPCHD=$CDIR/tpch;
SSBD=$CDIR/ssb;

source $CONFD/site.conf;
GEN=$UTILD/pdbgen.sh;
HLIST=$HADOOP_HOME/conf/slaves;
HEXEC=$HADOOP_HOME/bin/hadoop;

tpch() {
	local SCALE=$1;
	local HDFS_PATH=$2;

	DBGEN=$TPCH_DBGEN_HOME;
	
	$HEXEC fs -mkdir $HDFS_PATH;
	for t  in $(echo "customer,c supplier,s nation,n region,r orders,O lineitem,L part,P partsupp,S"); do
		TNAME=$(echo $t| awk -F',' '{print $1}');
		TSYM=$(echo $t| awk -F',' '{print $2}');
		
		$HEXEC fs -mkdir $HDFS_PATH/$TNAME;
		$GEN -s $SCALE -t $TSYM -e $DBGEN -h $HEXEC -p $HDFS_PATH/$TNAME -f $HLIST &
	done
	wait;
}

ssb() {
	local SCALE=$1;
	local HDFS_PATH=$2;

	DBGEN=$SSB_DBGEN_HOME;
	
	$HEXEC fs -mkdir $HDFS_PATH;
	for t  in $(echo "customer,c part,p supplier,s date,d lineorder,l"); do
		TNAME=$(echo $t| awk -F',' '{print $1}');
		TSYM=$(echo $t| awk -F',' '{print $2}');
		
		$HEXEC fs -mkdir $HDFS_PATH/$TNAME;
		$GEN -s $SCALE -t $TSYM -e $DBGEN -h $HEXEC -p $HDFS_PATH/$TNAME -f $HLIST &
	done
	wait;
}

##################
###    main    ###
##################
usage()
{
        echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
        echo "[description]:"
        echo "  execute dbgen in parallel and load the date into HDFS"
        echo "[option]:"
        echo "  -s  scale (>1)"
        echo "  -p  hdfs_path"
        echo "  -f  benchmark (tpch, ssb)"
        echo ""
        echo
}

if [ $# -lt 6 ]
then
        usage
        exit
fi

while getopts "s:p:f:" OPTION
do
        case $OPTION in
                s)
                        SCALE=$OPTARG;
                        ;;
                p)
                        HPATH=$OPTARG;
                        ;;
                f)
                        FUNC=$OPTARG;
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

if [ $SCALE -le 1 ]
then
	usage;
	exit;
fi

echo $@;
$FUNC $SCALE $HPATH;
