#!/bin/bash

KAFKA_HOME=/data/kafka_2.11-0.10.1.1
$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper localhost:2181
