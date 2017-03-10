#!/bin/bash

KAFKA_HOME=/data/kafka_2.11-0.10.1.1
SPARK_HOME=/data/spark-2.1.0-bin-hadoop2.6
$SPARK_HOME/bin/spark-shell --jars $SPARK_HOME/extjars/spark-streaming-kafka-0-10_2.11-2.1.0.jar,$KAFKA_HOME/libs/kafka-clients-0.10.1.1.jar
