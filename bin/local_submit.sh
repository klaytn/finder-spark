#!/bin/sh

# If there is --build option
if [ "$1" = "--build" ]; then
  echo "Build jar file"
  sbt klaytn-spark/clean klaytn-spark/assembly
fi

spark-submit \
--master local[2] \
--packages org.apache.hadoop:hadoop-aws:3.3.1 \
--packages mysql:mysql-connector-java:5.1.37 \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=3g \
--conf spark.app.phase=dev \
--conf spark.app.chain=cypress \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.app.s3a.access.key=$(aws configure get aws_access_key_id) \
--conf spark.app.s3a.secret.key=$(aws configure get aws_secret_access_key) \
--conf spark.driver.extraJavaOptions=-Dconfig.resource=io/klaytn/apps/block/BlockToDBStreaming-dev-cypress.conf \
--conf spark.executor.extraJavaOptions=-Dconfig.resource=io/klaytn/apps/block/BlockToDBStreaming-dev-cypress.conf \
--name dev-test \
--class io.klaytn.apps.block.BlockToDBStreaming \
project-app/spark/target/scala-2.12/klaytn-spark.jar
