import subprocess
import requests
import time
import sys

dirs = {"CMS": "/cms/*", "Phedex": "/cms/phedex/store/*", "Store": "/cms/store/*", "Group": "/cms/store/group/*", "Users": "/cms/store/user/*", "All": "/*"}
url = 'http://graph.t2.ucsd.edu:8086/write?db=hadoop_metrics_db'
header = {'Authorization': 'Token hadoop_writer:Hadoop3r'}

for key in dirs:
	out = subprocess.Popen(['hadoop', 'fs', '-du', dirs[key]],stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	stdout,stderr = out.communicate()
	for line in stdout.splitlines():
		try:
			size, usage, path = line.split()
			data = 		
		except ValueError:
			print "Error: Too many values"
		
