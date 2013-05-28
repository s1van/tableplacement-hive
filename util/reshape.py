#!/usr/bin/python

import sys, getopt, string, tempfile

def usage():
	print "Usage: reshape.py [--help] [--format=formatFile [--input=inputFile]] [--string=formatString]"
	print """Example: cat input| reshape.py --format=xxx.format"""
	print """Format: KEYWORD\tDELIMITER\tSKIP\tOFFSET\tCOL#1\tCOL#2..."""
	print """NOTE: format entries are separated by tabs"""

def main():
	if len(sys.argv) < 2:
		print "not enough arguments"
		usage()
		sys.exit()
	
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hf:i:s:", ["help", "format=", "input=", "string="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	noInputFile = True
	noFormatFile= True
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt in ("-f", "--format"):
			formatFile = open(arg, 'r')
			noFormatFile= False
		elif opt in ("-s", "--string"):
			formatFile = arg.split('\n')
		elif opt in ("-i", "--input"):
			inputFile = open(arg, 'r')
			noInputFile = False
		else:
			print "unhandled option"
			usage()
			sys.exit()

	if noInputFile:
		inputFile = tempfile.TemporaryFile()
		while 1:
			pipeinLine = sys.stdin.readline()
			if not pipeinLine:
				break
			inputFile.write(pipeinLine)
	
	#parse the formatFile
	keywords = []
	delimiters = []
	skips = []
	offsets = []
	columns = []
	colNum = 0
	for line in formatFile:
		elems = line.strip('\n').split("\t");
		#keyword, delimiter, skip, offset, cols
		if (len(elems) < 5):
			continue
		keywords.append(elems[0].strip('"').strip("'"))
		delim = elems[1].strip("'").strip('"')
		if (delim == "TAB"):
			delimiters.append("\t")
		else:
			delimiters.append(elems[1].strip("'").strip('"'))
		skips.append(elems[2].strip("'").strip('"'))
		offsets.append(elems[3].strip("'").strip('"'))
		columns.append(elems[4:])
		colNum = colNum + len(elems[4:])
	#print keywords,delimiters,skips, offsets, columns
	
	groupNum = len(keywords)
	results=[]
	for i in range(groupNum):
		results.append([])

	for i in range(len(keywords)):
		inputFile.seek(0)
		rowcur = 0
		for rawline in inputFile:
			line = rawline.strip('\n')
			if (line.find(keywords[i]) == -1):
				continue
			rowcur=rowcur+1
			if ((rowcur - int(offsets[i])) % int(skips[i]) != 0):
				continue				
			elems = line.split(delimiters[i])
			#print keywords[i],delimiters[i],columns[i]
			rline=[]
			for index in columns[i]:
				rline.append(elems[int(index)-1]) #list starts from 0
			results[i].append(rline)
	
	rows=[]
	for i in range(groupNum):
		rows.append(len(results[i]))
	MAXROWS = max(rows)
	
	#print results		
	for row in range(MAXROWS):
		for col in range(groupNum):
			res = results[col]
			if row < len(res):
				for item in res[row]:
					sys.stdout.write(item)
					sys.stdout.write('\t')
		print ""
	
	if (not noFormatFile):
		formatFile.close()
	if (not noInputFile):
		inputFile.close()

if __name__ == "__main__":
    main()
