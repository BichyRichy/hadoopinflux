import subprocess
import requests
import time
import sys

dirs = {"CMS": "/cms/*", "Phedex": "/cms/phedex/store/*", "Store": "/cms/store/*", "Group": "/cms/store/group/*", "Users": "/cms/store/user/*", "All": "/*"}
url = 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_metrics_db'
header = {'Authorization': 'Token hadoop_writer:Hadoop3r'}

# Stats of the entire storage
try:		
	fs = subprocess.Popen(['hadoop', 'fs', '-df'],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	stdout, stderr = fs.communicate()
except:
	print "error:", sys.exc_info()[0]
try:
	line = stdout.splitlines()[1]
	fs, size, used, available, perc = line.split()
	perc = perc[:-1]
	data = 'size,dir=%(fs)s value=%(s)s\n\
used,dir=%(fs)s value=%(u)s\n\
available,dir=%(fs)s value=%(a)s\n\
percent_used,dir=%(fs)s value=%(p)s' % {"fs":fs,"s":size,"u":used,"a":available,"p":perc}
	r = requests.post(url, headers = header, data=data, timeout=40)
	print(r.text)
except ValueError:
	print "error: too many arguments"

for key in dirs:
	fsdu = subprocess.Popen(['hadoop', 'fs', '-du', dirs[key]],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	stdout,stderr = fsdu.communicate()
	for line in stdout.splitlines():
		try:
			size, usage, path = line.split()
			#data = 		
		except ValueError:
			#print "Error: Too many values"
			continue
