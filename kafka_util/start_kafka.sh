#!/bin/bash

KAFKA_HOME=/data/kafka_2.11-0.10.1.1
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > /data/logs/kafka_server.log 2>&1 &
