#!/bin/sh

if [ -z $1 ] ; then
	echo "usage: bin/cc.sh cluster_name executor_instances"
	exit -1
fi

if [ -z $2 ] ; then
	echo "usage: bin/cc.sh cluster_name executor_instances"
	exit -1
fi

if [ -z $PHASE ] ; then
  echo "phase must be exist. invalid script access"
  exit -1
fi


SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
SCRIPT_ROOT=$PROJECT_ROOT/bin

PRIVATE2_SUBNET01=""
PRIVATE2_SUBNET02=""
PRIVATE2_SUBNET03=""

export PHASE=dev
export MASTER_INSTANCE_TYPE="m6g.xlarge"
export CORE_INSTANCE_TYPE="m6g.xlarge"
export SUBNET_ID=$PRIVATE2_SUBNET03
export SERVICE_ACCESS_SECURITY_GROUP=""
export EMR_MANAGED_SLAVE_SECURITY_GROUP=""
export EMR_MANAGED_MASTER_SECURITY_GROUP=""
export EMR_INSTANCE_PROFILE=""
export S3_LOG_URI="s3n://klaytn-prod-spark/emr"

CLUSTER_NAME=$1
EXECUTOR_INSTANCES=$2
INSTANCE_GROUPS='[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":'"\"${MASTER_INSTANCE_TYPE}\""',"Name":"Master Instance Group - 1"},{"InstanceCount":'"${EXECUTOR_INSTANCES}"',"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":'"\"${CORE_INSTANCE_TYPE}\""',"Name":"Core Instance Group - 2"}]'
CONFIGURATION='[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]'

if [ -z $EC2_ATTRIBUTES ]; then
  EC2_ATTRIBUTES='{"InstanceProfile":'"\"$EMR_INSTANCE_PROFILE\""',"ServiceAccessSecurityGroup":'"\"${SERVICE_ACCESS_SECURITY_GROUP}\""',"SubnetId":'"\"${SUBNET_ID}\""',"EmrManagedSlaveSecurityGroup":'"\"${EMR_MANAGED_SLAVE_SECURITY_GROUP}\""',"EmrManagedMasterSecurityGroup":'"\"${EMR_MANAGED_MASTER_SECURITY_GROUP}\""'}'
fi

## m6g.xlarge   core: 4,  mem: 16gb
## m6g.2xlarge  core: 8,  mem: 32gb
## m6g.4xlarge  core: 16, mem: 64gb
## m6g.8xlarge  core: 32, mem: 128gb
## m6g.12xlarge core: 64, mem: 256gb

aws emr create-cluster \
--termination-protected \
--applications Name=Hadoop Name=Spark \
--ec2-attributes $EC2_ATTRIBUTES \
--release-label emr-6.5.0 \
--log-uri "$S3_LOG_URI" \
--instance-groups "$INSTANCE_GROUPS" \
--configurations "$CONFIGURATION" \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--ebs-root-volume-size 10 \
--service-role EMR_DefaultRole \
--enable-debugging \
--name "$CLUSTER_NAME" \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region ap-northeast-2

#--auto-termination-policy '{"IdleTimeout":600}' \
##--auto-termination-policy '{"IdleTimeout":3600}' \
