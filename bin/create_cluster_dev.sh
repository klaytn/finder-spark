#!/bin/sh

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
SCRIPT_ROOT=$PROJECT_ROOT/bin

export PHASE=dev
export MASTER_INSTANCE_TYPE="m6g.xlarge"
export CORE_INSTANCE_TYPE="m6g.xlarge"
export SUBNET_ID=""
export SERVICE_ACCESS_SECURITY_GROUP=""
export EMR_MANAGED_SLAVE_SECURITY_GROUP=""
export EMR_MANAGED_MASTER_SECURITY_GROUP=""
export EMR_INSTANCE_PROFILE=""
export S3_LOG_URI="s3n://klaytn-dev-spark/emr"

$SCRIPT_ROOT/create_cluster.sh $@
