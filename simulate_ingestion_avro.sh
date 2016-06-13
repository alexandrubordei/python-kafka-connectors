#!/bin/bash

SLEEP_UP_TO=100
TOPIC=test
AVRO_FILE=hdfs://instance-18168.bigstep.io:8020/user/emagtest/part-m-00000.avro
BROKERS=instance-18171.bigstep.io:9092,instance-18169.bigstep.io:9092,instance-18170.bigstep.io:9092

hadoop jar avro-tools-1.8.1.jar cat --offset 0 --limit 10 $AVRO_FILE - | pv -L 100 | kafka-console-producer --broker-list $BROKERS --topic $TOPIC

