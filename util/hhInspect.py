#!/usr/bin/python
import getopt
import os
import sys
import urllib
import urllib2
import re
from bs4 import BeautifulSoup


def usage():
	print "Usage: hhInspect.py [--help] --ip=master_address --port=webUI_port --jobid=hadoop_jobid --info=info1,info2" 
	print "Info Option: MapPhaseLength, ReducePhaseLength, FinishedTime"
	print "Notice that the script only searches the first page of the jobhistoryhome.jsp"
	print ""

def getMapPhaseLength(server, jobid):
	jumpto = BeautifulSoup(urllib2.urlopen(server + "jobhistoryhome.jsp")).findAll('a', {'href': re.compile(jobid)}).pop().get('href')
	lastEntry = BeautifulSoup(urllib2.urlopen(server + jumpto)).findAll('tr')[2].findAll('td')[-1].string

	ltime = map(int, re.findall('[0-9]+',re.findall(r'\(.+\)', lastEntry)[0]))
	tunit = [3600, 60, 1]

	stime=0
	for x in range(1, len(ltime)+1):
		stime = ltime[-x] * tunit[-x] + stime
	
	return stime;

def getReducePhaseLength(server, jobid):
	jumpto = BeautifulSoup(urllib2.urlopen(server + "jobhistoryhome.jsp")).findAll('a', {'href': re.compile(jobid)}).pop().get('href')
	lastEntry = BeautifulSoup(urllib2.urlopen(server + jumpto)).findAll('tr')[3].findAll('td')[-1].string

	ltime = map(int, re.findall('[0-9]+',re.findall(r'\(.+\)', lastEntry)[0]))
	tunit = [3600, 60, 1]

	stime=0
	for x in range(1, len(ltime)+1):
		stime = ltime[-x] * tunit[-x] + stime
	
	return stime;

def getFinishedTime(server, jobid):
	jumpto = BeautifulSoup(urllib2.urlopen(server + "jobhistoryhome.jsp")).findAll('a', {'href': re.compile(jobid)}).pop().get('href')
	cc = BeautifulSoup(urllib2.urlopen(server + jumpto)).stripped_strings
	i = 0
	for s in cc:
		i = i + 1
		if (i == 16):
			raw = s
	ltime = map(int, re.findall('[0-9]+',re.findall(r'\(.+\)', raw)[0]))
	tunit = [3600, 60, 1]

	stime=0
	for x in range(1, len(ltime)+1):
		stime = ltime[-x] * tunit[-x] + stime
	
	return stime;

	
def main():
	if len(sys.argv) < 5:
		print "ERROR: not enough arguments"
		usage()
		sys.exit()
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "i:p:j:o:h", ["ip=","port=","help", "jobid=", "info="]) 
	except getopt.GetoptError:           
		usage()                          
		sys.exit(2)
                     
	for opt, arg in opts:                
		if opt in ("-h", "--help"):      
			usage()                     
			sys.exit()                  
		elif opt in ("-i", "--ip"): 
			ip = arg
		elif opt in ("-p", "--port"): 
			port= arg
		elif opt in ("-j", "--jobid"): 
			jobid = arg
		elif opt in ("-o", "--info"): 
			tags = arg.split(',')
		else:
			print "unhandled option"
			usage()
			sys.exit()

	webserver = 'http://' + ip + ':' + port + '/'

	ret = []
	for tag in tags:
		ret.append(globals()['get' + tag](webserver, jobid))

	print '\t'.join(map(str, ret))

if __name__ == "__main__":
	main()
