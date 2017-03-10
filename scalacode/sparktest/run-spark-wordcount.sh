#!/bin/bash

bindir=$(cd $(dirname $0); pwd)
cd ${bindir}
$SPARK_HOME/bin/spark-submit --class example.WordCount ./target/scala-2.11/sparktest_2.11-0.1.0.jar ./github.txt 2>console.log
