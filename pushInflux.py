import subprocess
import requests
import json
import re

dirs = {"CMS": "/hadoop/cms/*", "Phedex": "/hadoop/cms/phedex/store/*", "Store": "/hadoop/cms/store/*", "Group": "/hadoop/cms/store/group/*", "Users": "/hadoop/cms/store/user/*", "All": "/hadoop/*"}
url = 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_metrics_db'

with open('conf.json') as h:
	header = json.load(h)

# Stats of the entire storage
fs = subprocess.Popen(['hadoop', 'fs', '-df'],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
if fs.returncode == 0:
	stdout, stderr = fs.communicate()
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
else:
	print "could not run hadoop df check"

# Stats of each subdirectory
for key in dirs:
	fsdu = subprocess.Popen(['hadoop', 'fs', '-du', '-s', dirs[key]],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	if fsdu.returncode == 0:
		stdout,stderr = fsdu.communicate()
		for line in stdout.splitlines():
			try:
				size, usage, path = line.split()
			except ValueError:
				continue
			if size != '0':
				data = 'size,dir=%(p)s value=%(s)s\nusage,dir=%(p)s value=%(u)s' % {"p":path,"s":size,"u":usage}
				r = requests.post(url, headers=header, data=data, timeout=40)
	else:
		print "could not run hadoop du check"

# Missing blocks and nodes
report = subprocess.Popen(['/usr/bin/hdfs', 'dfsadmin', '-report'],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
if report.returncode == 0:
	stdout, stderr = report.communicate()
	for line in stdout.splitlines():
		if re.match("Missing blocks: [0-9]+$", line):
			missing = int(filter(str.isdigit, line))
			data = 'missing_blocks value=%d' % (missing)
			r = requests.post(url, headers=header, data=data, timeout=40)
		elif re.match("Live datanodes \([0-9]+\):", line):
			nodes = int(filter(str.isdigit, line))
			data = 'live_nodes value=%d' % (nodes)
			r = requests.post(url, headers=header, data=data, timeout=40)
else:
	print "could not run hdfs"