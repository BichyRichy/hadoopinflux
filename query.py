import requests
url = 'http://graph.t2.ucsd.edu:8086/query?pretty=true&db=hadoop_metrics_db'

q = '&q=SELECT \"value\" FROM \"percent_used\"'

r = requests.get(url+q, headers=header)
print(r.text)