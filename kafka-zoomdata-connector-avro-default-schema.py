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

KAFKA_BROKERS="instance-18171.bigstep.io:9092,instance-18169.bigstep.io:9092,instance-18170.bigstep.io:9092"
KAFKA_TOPIC="clickstream"

zoomdata = "http://localhost:8089/zoomdata/service/upload?source=Kafka%20Real%20Time%20Source%20default%20schema"
username = "admin"
password = "emagxbigstep"
base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')


def start_consumer(kafka_servers,kafka_topic): 
	print(kafka_servers)
	consumer = KafkaConsumer(bootstrap_servers=kafka_servers)
	consumer.subscribe([kafka_topic])

	schema = avro.schema.parse(open("DefaultEventRecord.avsc").read())
	reader = avro.io.DatumReader(schema)
	
	for message in consumer:
		handle_event(message, reader)

def ascii_bytes(id):
	return bytes(id, 'us-ascii')

def avro2dict(avrobytes,reader):
	decoder = avro.io.BinaryDecoder(avrobytes)
	return reader.read(decoder)

def send2zoomdata(msg):
	jsondata = json.dumps(msg)	
	jsondataasbytes = jsondata.encode('utf-8')   # needs to be bytes


	req = urllib2.Request(zoomdata)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(jsondataasbytes))
	req.add_header("Authorization", "Basic %s" % base64string)
	return urllib2.urlopen(req,jsondataasbytes)
	
def handle_event(message, reader):
			
	message_bytes = io.BytesIO(message.value)
	event = avro2dict(message_bytes,reader) 
	
	print(event)

	i = datetime.utcnow()	
	datestr = i.strftime('%Y-%m-%d %H:%M:%S')

#main
start_consumer(KAFKA_BROKERS,KAFKA_TOPIC)
