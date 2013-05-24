#!/usr/bin/python
from xml.dom.minidom import parse
import getopt
import os
import sys

def usage():
	print "Usage: fillTemplate.py [--help] --data=excel_form --template=template_file --path=output"
	print "--vars=varaibles"
	print "		"
	print "--vals=values"
	print "		"
	print "--template=template_file "
	print "		"
	print "--output"
	print "		"
	print ""

def main():
	if len(sys.argv) < 3:
		print "not enough arguments"
		usage()
		sys.exit()
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "x:v:ho:t:", ["vars=","vals=","help", "template=", "output="]) 
	except getopt.GetoptError:           
		usage()                          
		sys.exit(2)
                     
	appendEnabled = False
	printEnabled = False
	for opt, arg in opts:                
		if opt in ("-h", "--help"):      
			usage()                     
			sys.exit()                  
		elif opt in ("-x", "--vars"): 
			variables = arg.split(' ')
		elif opt in ("-v", "--vals"): 
			values = arg.split(' ')    
		elif opt in ("-t", "--template"): 
			template_file = arg    
		elif opt in ("-o", "--output"): 
			output = arg    
		else:
			print "unhandled option"
			usage()
			sys.exit()

	template = open(template_file, 'r').read()
	

	report = template.decode('utf8')
	for i in range(len(variables)):
		colname = "%%" + variables[i]  + '%%'
		tovalue = unicode(values[i])
		report = report.replace(colname, tovalue)
	try:
    		output
	except NameError:
		output = None
	
	if output is None:
		print report
	else:
		f = open(output, 'w')
		f.write(report.encode("UTF-8"))
		f.close()

if __name__ == "__main__":
	main()
