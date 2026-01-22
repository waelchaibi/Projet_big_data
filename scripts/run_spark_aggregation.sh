#!/bin/bash
# Script wrapper pour lancer l'agrégation Spark avec le bon cache Ivy

set -euo pipefail

# The apache/spark image can default HOME to /nonexistent; force a sane HOME.
export HOME=/root

# Use a dedicated mounted volume for Ivy cache (writable)
IVY_CACHE_DIR=/ivy
mkdir -p "${IVY_CACHE_DIR}/cache" "${IVY_CACHE_DIR}/jars"

export IVY_HOME="${IVY_CACHE_DIR}"
export SPARK_LOCAL_DIRS=/tmp/spark-local

cd /opt/spark-apps
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  spark_weather_aggregation.py
