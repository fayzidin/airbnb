#!/bin/bash

# Download and install the BigQuery connector
BIGQUERY_CONNECTOR_VERSION="0.41.1"
HADOOP_VERSION="3.2"
CONNECTOR_URL="https://storage.googleapis.com/hadoop-lib/bigquery/bigquery-connector-with-dependencies-${BIGQUERY_CONNECTOR_VERSION}-${HADOOP_VERSION}.jar"
HIVE_CONNECTOR_URL="https://storage.googleapis.com/hadoop-lib/bigquery/bigquery-connector-hive-${BIGQUERY_CONNECTOR_VERSION}.jar"
sudo wget -O /usr/lib/spark/jars/spark-bigquery.jar $CONNECTOR_URL
sudo wget -O /usr/lib/hive/lib/bigquery-connector-hive-0.41.1.jar $HIVE_CONNECTOR_URL


# Confirm installation
ls -l /usr/lib/spark/jars/spark-bigquery.jar
