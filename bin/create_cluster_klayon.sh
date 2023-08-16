#!/bin/sh

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
SCRIPT_ROOT=$PROJECT_ROOT/bin

export PHASE=prod
export MASTER_INSTANCE_TYPE="m6g.xlarge"
export CORE_INSTANCE_TYPE="m6g.xlarge"
export SERVICE_ACCESS_SECURITY_GROUP=""
export SUBNET_ID=""
export EMR_MANAGED_SLAVE_SECURITY_GROUP=""
export EMR_MANAGED_MASTER_SECURITY_GROUP=""
export S3_LOG_URI=""

export EC2_ATTRIBUTES='{"KeyName":"DEFAULT","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":'"\"${SERVICE_ACCESS_SECURITY_GROUP}\""',"SubnetId":'"\"${SUBNET_ID}\""',"EmrManagedSlaveSecurityGroup":'"\"${EMR_MANAGED_SLAVE_SECURITY_GROUP}\""',"EmrManagedMasterSecurityGroup":'"\"${EMR_MANAGED_MASTER_SECURITY_GROUP}\""'}'

$SCRIPT_ROOT/create_cluster.sh $@
