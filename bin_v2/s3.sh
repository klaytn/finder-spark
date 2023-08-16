#!/usr/bin/env bash

function upload_app_jar() {
  # TODO - classname
  local DATE=`date +%Y%m%d_%H`
  local DATE2=`date +%Y%m%d_%H%M%S`
  local USER=`whoami`
  local SOURCE_JAR_PATH="project-app/spark/target/scala-2.12/klaytn-spark.jar"
  local S3_DEPLOY_PATH="${S3_JAR_PATH}/${USER}/${DATE}/klaytn-spark-${DATE2}.jar"

  aws s3 cp $SOURCE_JAR_PATH $S3_DEPLOY_PATH --quiet
  local result=$?
  if [[ $result -eq 0 ]]; then
    echo $S3_DEPLOY_PATH
  else
    echo "upload jar failed ($result)"
    exit 1
  fi
}

if [[ "$RELEASE" == "" ]];then
  case $1 in
    "upload_app_jar")
      upload_app_jar
      ;;
    *)
      echo "invalid usage (available: upload_app_jar)"
      ;;
  esac
fi
