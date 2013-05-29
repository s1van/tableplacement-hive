#!/bin/bash

quatile-var() {
INPUT=$1;
COLNAMES=$2;

R --slave --vanilla --quiet --no-save << EEE
	m <- read.table("$INPUT")
	q <- sapply(m, summary)
	v <- matrix(sapply(m, sd), nrow=1)
	rownames(v) <- "Variance"

	res <- rbind(q, v)
	colnames(res) <- strsplit("$COLNAMES", ',')[[1]]
	print(res)
EEE
}


##########
##MAIN()##
##########
$@;
