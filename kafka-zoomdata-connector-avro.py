#!/usr/bin/python
from kafka import KafkaConsumer
import urllib2
import base64
import sys
from datetime import datetime
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import avro
import io
import time
import sys

KAFKA_BROKERS="instance-18171.bigstep.io:9092,instance-18169.bigstep.io:9092,instance-18170.bigstep.io:9092"
KAFKA_TOPIC="clickstream"

zoomdata = "http://localhost:8089/zoomdata/service/upload?source=Kafka%20Real%20Time%20Source"
username = "admin"
password = "emagxbigstep"
base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
schema_name ="emag.avsc"
groupid="zoomdata2"

req = urllib2.Request(zoomdata)
req.add_header('Content-Type', 'application/json; charset=utf-8')
#req.add_header('Content-Length', len(jsondataasbytes))
req.add_header("Authorization", "Basic %s" % base64string)
	

def start_consumer(kafka_servers,kafka_topic,groupid,clientid): 

#	consumer = KafkaConsumer(bootstrap_servers=kafka_servers, group_id=groupid,client_id=clientid)
	consumer = KafkaConsumer(bootstrap_servers=kafka_servers)
	consumer.subscribe([kafka_topic])

	schema = avro.schema.parse(open(schema_name).read())
	reader = avro.io.DatumReader(schema)
	print ("starting with groupid="+groupid+" clientid="+clientid)

	total_events_handled=0	
	events_handled=0
	start_time = time.time()
	for message in consumer:
		handle_event(message, reader)
		events_handled+=1
		if(events_handled % 5000==0):
			total_events_handled+=events_handled
			elapsed_time = time.time() - start_time
			print("events_handled=%d (%d in total) in %f seconds =%f req/s" % (events_handled,total_events_handled, elapsed_time,events_handled/elapsed_time))
			start_time = time.time()
			events_handled=1	

	


def ascii_bytes(id):
	return bytes(id, 'us-ascii')

def avro2dict(avrobytes,reader):
	decoder = avro.io.BinaryDecoder(avrobytes)
	return reader.read(decoder)

def send2zoomdata(msg):
	jsondata = json.dumps(msg)	
	jsondataasbytes = jsondata.encode('utf-8')   # needs to be bytes
#	print(jsondata)
	return urllib2.urlopen(req,jsondataasbytes)

def handle_event(message, reader):
			
	message_bytes = io.BytesIO(message.value)
	event = avro2dict(message_bytes,reader) 
	
	i = datetime.utcnow()	
	datestr = i.strftime('%Y-%m-%d %H:%M:%S')

	event["date_inserted"]=datestr
#	print(event)
	sys.stdout.write('.')
	sys.stdout.flush()
#	print(event)
	ret=send2zoomdata(event)
#        print("ret="+ret.read())


i = datetime.utcnow()
clientid = i.strftime('zoom-%Y-%m-%d%H%M%S')
start_consumer(KAFKA_BROKERS,KAFKA_TOPIC,groupid,clientid)



