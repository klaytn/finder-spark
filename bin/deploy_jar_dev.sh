#!/bin/sh

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
SCRIPT_ROOT=$PROJECT_ROOT/bin

export S3_JAR_PATH="s3://klaytn-dev-spark/jars"

$SCRIPT_ROOT/deploy_jar.sh
