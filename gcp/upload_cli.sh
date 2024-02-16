#!/usr/bin/env bash

TARGET_CLUSTER=$1
if [[ "$TARGET_CLUSTER" == "" ]]; then
  echo "missing target cluster (usage: $0 \$TARGET_CLUSTER)"
  exit 1
fi

case $TARGET_CLUSTER in
  "klaytn")
    MONITORING_EC2_INSTANCE=i-076d0f7db97bc7b5d
    S3_BUCKET="klaytn-prod-spark"
    ;;
  *)
    echo "unknown target cluster: $0"
    exit 1
    ;;
esac


SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
MODULE_ROOT=$PROJECT_ROOT/project-tool

if [[ "$SKIP_BUILD" != "true" ]]; then
  cd $PROJECT_ROOT
  $PROJECT_ROOT/sbt klaytn-cli/clean klaytn-cli/assembly
  cd $MODULE_ROOT
fi

if [[ $? -ne 0 ]]; then
  echo "build failed"
  exit 1
fi

gcloud gcs cp $MODULE_ROOT/cli/target/scala-2.12/klaytn-cli.jar gs://${S3_BUCKET}/temp/klaytn-cli.jar

echo ""
echo "MONITORING_EC2_INSTANCE: $MONITORING_EC2_INSTANCE"
echo "S3 PATH: gs://${S3_BUCKET}/temp/klaytn-cli.jar"
