#!/usr/bin/python
from kafka import KafkaConsumer
import json
import urllib2
import base64
import sys
from datetime import datetime

req = urllib2.Request('http://localhost:8089/zoomdata/service/upload')
req.add_header('Content-Type', 'application/json')

consumer = KafkaConsumer(bootstrap_servers='instance-18171.bigstep.io:9092,instance-18169.bigstep.io:9092,instance-18170.bigstep.io:9092')
consumer.subscribe(['clickstream'])

zoomdata = "http://localhost:8089/zoomdata/service/upload?source=Kafka%20Real%20Time%20Source"

username = "admin"
password = "emagxbigstep"
base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')

for msg in consumer:
	
	try:
		req = urllib2.Request(zoomdata)
		req.add_header('Content-Type', 'application/json; charset=utf-8')


		jsonarr = json.loads(msg.value)["data"]
		
		i = datetime.utcnow()	
		datestr = i.strftime('%Y-%m-%d %H:%M:%S')
		jsonarr["date_inserted"]=datestr
		print(datestr)
		jsondata = json.dumps(jsonarr)	

		jsondataasbytes = jsondata.encode('utf-8')   # needs to be bytes
		req.add_header('Content-Length', len(jsondataasbytes))
		req.add_header("Authorization", "Basic %s" % base64string)

		urllib2.urlopen(req,jsondataasbytes)
		print("Sent"+jsondata)

	except Exception as e:
	    print("Unexpected error:", e)


