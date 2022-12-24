#!/bin/sh

cp target/scala-2.13/spark-scala3-example-assembly-0.1.0-SNAPSHOT.jar ./spark-apps/spark-scala3-example.jar

docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 2 \
  --class "$1" \
  --driver-memory 4G \
  --executor-memory 1G \
  /opt/spark-apps/spark-scala3-example.jar
