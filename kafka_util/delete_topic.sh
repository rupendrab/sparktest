#!/bin/bash

if [ "$1" = "" ]
then
  echo "Usage :$(basename $0) <Topic>"
  exit 1
fi

topic="$1"

KAFKA_HOME=/data/kafka_2.11-0.10.1.1
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic "$topic"
