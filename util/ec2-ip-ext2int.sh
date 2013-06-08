#!/bin/bash

usage()
{
	echo "Usage: `echo $0| awk -F/ '{print $NF}'`  [-option]"
	echo "[description]:"
	echo "  Translate external IPs to internal IPs"
	echo "[option]:"
	echo "  -f  file that stores external IPs"
	echo "		"
	echo "  -i  certificate"
	echo "		"
	echo "  -u  user"
	echo ""
}

if [ $# -lt 6 ]
then
	usage
	exit
fi

while getopts "f:i:u:" OPTION
do
        case $OPTION in
                f)
                        EFILE=$OPTARG;
                        ;;
                i)
                        CERT=$OPTARG;
                        ;;
                u)
                        USER=$OPTARG;
                        ;;
                ?)
                        echo "unknown arguments"
                        usage
                        exit
                        ;;
        esac
done

for ip in $(cat $EFILE); do
	ssh -o "StrictHostKeyChecking no" -i $CERT ${USER}@${ip} ifconfig 2>&1|grep Bcast| sed 's/inet addr://g'| awk '{print $1}'| sed 's/\./-/g'| awk '{print "ip-"$1".ec2.internal"}';
done
