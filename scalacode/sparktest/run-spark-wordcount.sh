#!/bin/bash

bindir=$(cd $(dirname $0); pwd)
cd ${bindir}

export SPARK_HOME=/data/spark-2.1.0-bin-hadoop2.6

$SPARK_HOME/bin/spark-submit --class example.WordCount ./target/scala-2.11/sparktest_2.11-0.1.0.jar ./github.txt 2>console.log
