import subprocess
import requests
import json
import re
import sys

dirs = {"CMS": "/cms/*", "Phedex": "/cms/phedex/store/*", "Store": "/cms/store/*", "Group": "/cms/store/group/*", "Users": "/cms/store/user/*", "All": "/*"}
url = 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_metrics_db'

with open('conf.json') as h:
	header = json.load(h)

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
	r = requests.post(url, headers=header, data=data, timeout=40)
except ValueError:
	print "error: too many arguments"

for key in dirs:
	fsdu = subprocess.Popen(['hadoop', 'fs', '-du', dirs[key]],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	stdout,stderr = fsdu.communicate()
	for line in stdout.splitlines():
		try:
			size, usage, path = line.split()
		except ValueError:
			continue
		if size != '0':
			data = 'size,dir=%(p)s value=%(s)s\nusage,dir=%(p)s value=%(u)s' % {"p":path,"s":size,"u":usage}
			r = requests.post(url, headers=header, data=data, timeout=40)

try:
	report = subprocess.Popen(['/usr/bin/hdfs', 'dfsadmin', '-report'],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	stdout, stderr = report.communicate()
except:
	print "error:", sys.exc_info()[0]

for line in stdout.splitlines():
	if re.match("Missing blocks: [0-9]+$", line):
		missing = int(filter(str.isdigit, line))
		data = 'missing_blocks, value=%d' % (missing)
		r = requests.post(url, headers=header, data=data, timeout=40)
