#!/usr/bin/python
from xml.dom.minidom import parse
import getopt
import os
import sys

def usage():
	print "Usage: columnGrpSQL.py [--help] --columns=column_names --types=column_types" 
	print "		--groups=column_groups --key=primary_keys --table=table_name"
	print "Example: columnGrpSQL.py --columns=a,b,c --types=INT,TINYINT,STRING --groups=2,0:1 --keys=0,1 --table=lineorder"
	print ""

def columnSplit(s):
	return s.split(',')

def main():
	if len(sys.argv) < 3:
		print "ERROR: not enough arguments"
		usage()
		sys.exit()
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "c:t:g:hk:n:", ["columns=","types=","help", "groups=", "keys=", "table="]) 
	except getopt.GetoptError:           
		usage()                          
		sys.exit(2)
                     
	for opt, arg in opts:                
		if opt in ("-h", "--help"):      
			usage()                     
			sys.exit()                  
		elif opt in ("-c", "--columns"): 
			columns = arg.split(',')
		elif opt in ("-t", "--types"): 
			types = arg.split(',')    
		elif opt in ("-k", "--keys"): 
			keys = map(int, arg.split(','))
		elif opt in ("-n", "--table"): 
			table = arg
		elif opt in ("-g", "--groups"): 
			groups = map(columnSplit, arg.split(':'))
		else:
			print "unhandled option"
			usage()
			sys.exit()

	cnum = len(columns)
	if cnum != len(types):
		print "ERROR: unequal number of columns and types"
		sys.exit()
	
	cet = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table + "_t (\n\t"
	for i in range(cnum):
		if i < cnum - 1:
			cet = cet + columns[i] + ' ' + types[i] + ', '
			if (i+1)%4 == 0:
				cet = cet + '\n\t'
		else:
			cet = cet + columns[i] + ' ' + types[i] + ')'
	cet = cet + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
        cet = cet + "\nSTORED AS TEXTFILE LOCATION '%%HDFS_ROOT%%/" + table + "';"

	
	cst = "CREATE TABLE " + table + "_s (\n\t"
	gindex = 1 
	for group in groups:
		cst = cst + 'CG' + str(gindex) + ' STRUCT<'
		cgpos = 0;
		for cindex in group:
			cst = cst + columns[int(cindex)] + ':' + types[int(cindex)]
			if cindex != group[-1]:
				cst = cst + ', '
				if (cgpos + 1)%4 == 0:
					cst = cst + '\n\t'
			else:
				cst = cst + '>,\n\t' 
			cgpos = cgpos + 1
		gindex = gindex + 1
	for kindex in keys:
		cst = cst + columns[kindex] + ' ' + types[kindex]
		if kindex != keys[-1]:
			cst = cst + ', '
		else:
			cst = cst + ')'

	cst = cst + "\nCLUSTERED BY (" 
	for kindex in keys:
		cst = cst + columns[kindex] 
		if kindex != keys[-1]:
			cst = cst + ', '
		else:
			cst = cst + ')'
	cst = cst + " INTO %%B_NUM%% BUCKETS"
        cst = cst + "\nROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
        cst = cst + "\nSTORED AS RCFILE LOCATION '%%HIVE_ROOT%%/" + table + "';"


	
	ist = "INSERT OVERWRITE TABLE " + table + "_s \nSELECT\n\t"
	for group in groups:
		ist = ist + 'named_struct(' 
		cgpos = 0;
		for cindex in group:
			ist = ist + "'" + columns[int(cindex)] + "'" + ', ' + columns[int(cindex)]
			if cindex != group[-1]:
				ist = ist + ', '
				if (cgpos + 1)%3 == 0:
					ist = ist + '\n\t'
			else:
				ist = ist + '),\n\t'
			cgpos = cgpos + 1

	for kindex in keys:
		ist = ist + columns[kindex] 
		if kindex != keys[-1]:
			ist = ist + ', '
		else:
			ist = ist + '\n'
	ist = ist + "FROM " + table + "_t\n\tCLUSTER BY "
	for kindex in keys:
		ist = ist + columns[kindex] 
		if kindex != keys[-1]:
			ist = ist + ', '
		else:
			ist = ist + ';'
	

	det = "DROP TABLE " + table + "_t;"

	cvt = "CREATE VIEW IF NOT EXISTS " + table + "\nAS SELECT\n\t"
	gindex = 1 
	for group in groups:
		cgpos = 0;
		for cindex in group:
			cvt = cvt + 'CG' + str(gindex) + '.' + columns[int(cindex)] + " AS " + columns[int(cindex)]
			if cindex != group[-1]:
				cvt = cvt + ', '
				if (cgpos + 1)%3 == 0:
					cvt = cvt + '\n\t'
			else:
				if group != groups[-1]:	
					cvt = cvt + ',\n\t' 
				else:
					cvt += '\n'
			cgpos = cgpos + 1
		gindex += 1	
	cvt += "FROM " + table + "_s;"



	print cet + "\n\n"
	print cst + "\n\n"
	print ist + "\n\n"
	print det + "\n\n"
	print cvt + "\n\n"

if __name__ == "__main__":
	main()
