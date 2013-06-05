#!/bin/bash

quatile-var() {
INPUT=$1;
COLNAMES=$2;
OUT=$3;

R --slave --vanilla --quiet --no-save << EEE
	m <- read.table("$INPUT")
	q <- sapply(m, summary)
	v <- round(matrix(sapply(m, sd), nrow=1), digits=4)
	rownames(v) <- "Variance"

	res <- rbind(q, v)
	colnames(res) <- strsplit("$COLNAMES", ',')[[1]]
	write.table(res, file = "$OUT", sep = "\t") 
EEE
}


##########
##MAIN()##
##########
$@;
