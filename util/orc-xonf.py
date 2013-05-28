#!/usr/bin/python

from xml.dom.minidom import parse
import getopt
import sys

def usage():
	print "Usage: orc-xonf.py [--help] --file=path --key=key --value=value [--append] [--print]"
	print "--append "
	print "		append the property to the bottotm of the configuration were it not defined before "
	print """Example: python orc-conf.py --file="./mapred-site.xml" --key='mapred.tasktracker.map.tasks.maximum' --value=2 """

def main():
	if len(sys.argv) < 2:
		print "not enough arguments"
		usage()
		sys.exit()
	
	try:                                
		opts, args = getopt.getopt(sys.argv[1:], "ahpk:v:f:", ["help", "key=", "value=", "file=", "append", "print"]) 
	except getopt.GetoptError:           
		usage()                          
		sys.exit(2)
                     
	appendEnabled = False
	printEnabled = False
	for opt, arg in opts:                
		if opt in ("-h", "--help"):      
			usage()                     
			sys.exit()                  
		elif opt in ("-k", "--key"): 
			key = arg    
		elif opt in ("-v", "--value"): 
			val = arg    
		elif opt in ("-f", "--file"): 
			path = arg    
		elif opt in ("-a", "--append"): 
			appendEnabled = True    
		elif opt in ("-p", "--print"): 
			printEnabled = True
		else:
			print "unhandled option"
			usage()
			sys.exit()

	isFound = False;
	anyNode = False;
	dom = parse(path);
	conf = dom.childNodes[-1];
	properties=conf.childNodes[1::2];
	for property in properties:
		if property.nodeType == property.COMMENT_NODE:
			continue
		else:
			anyNode = property
		nameElem = property.getElementsByTagName("name")[0]
		name = nameElem.childNodes[0]
		if name.data == key:
			valueElem = property.getElementsByTagName("value")[0]
			value = valueElem.childNodes[0]
			if printEnabled:
				print value.data
			else:
				value.data = unicode(val)
			isFound = True;
			break;
			
	#if ~isFound	and anyNode and appendEnabled:
	if (isFound	== False) and anyNode:
		newNode = anyNode.cloneNode(anyNode)
		newNode.getElementsByTagName("name")[0].childNodes[0].data = unicode(key)
		newNode.getElementsByTagName("value")[0].childNodes[0].data = unicode(val)
		blankNode = conf.childNodes[-1].cloneNode(conf.childNodes[-1]) 
		conf.childNodes.append(newNode)
		conf.childNodes.append(blankNode)
	
	f = open(path, 'w')
	dom.writexml(f)
	f.close()

if __name__ == "__main__":
	main()
