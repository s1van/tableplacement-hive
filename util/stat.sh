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

colsum_odd_even() {
INPUT=$1;

R --slave --vanilla --quiet --no-save << EEE
	m <- read.table("$INPUT")
	ll<- length(m)
	
	if (ll == 2) {
		cc <- m
	} else {
		c1 <- rowSums(m[,seq(1,ll,2)])
		c2 <- rowSums(m[,seq(2,ll,2)])
		cc <- cbind(c1,c2)
	}
	write.table(cc, row.names=FALSE, col.names=FALSE, sep='\t')
EEE
}

##########
##MAIN()##
##########
$@;
