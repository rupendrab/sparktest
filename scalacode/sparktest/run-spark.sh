#!/bin/bash

if [ "$1" = "" ]
then
  echo "Usage: run-spark.sh <Class Name>"
  exit 1
fi

main_class=${1}
shift

bindir=$(cd $(dirname $0); pwd)
cd ${bindir}
$SPARK_HOME/bin/spark-submit --class ${main_class} target/scala-2.11/sparktest_2.11-0.1.0.jar ${1+$@} 2>console.log
