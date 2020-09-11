import requests
import json
url = 'http://graph.t2.ucsd.edu:8086/query?pretty=true&db=hadoop_metrics_db'
with open('conf.json') as h:
	header = json.load(h)


q = '&q=SELECT \"value\" FROM \"missing_blocks\"'

r = requests.get(url+q, headers=header)
print(r.text)