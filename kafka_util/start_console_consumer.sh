#!/bin/bash

if [ "$1" = "" ]
then
  echo "Usage $(basename $0) <Topic Name>"
  exit 1
fi

topic=$1
shift

KAFKA_HOME=/data/kafka_2.11-0.10.1.1
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "${topic}" ${1+$@}
