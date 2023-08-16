#!/bin/sh

date=`date +%Y%m%d_%H`
date2=`date +%Y%m%d_%H%M%S`
whoami=`whoami`
upload_path="${S3_JAR_PATH}/${whoami}/${date}/klaytn-spark-${date2}.jar"

if [[ "$SKIP_BUILD" != "true" ]]; then
  ./sbt clean assembly
fi

aws s3 cp project-app/spark/target/scala-2.12/klaytn-spark.jar $upload_path
