#!/usr/bin/env bash

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)

source $PROJECT_ROOT/bin_v3/util/common.sh

function cluster_create {
  local USAGE="usage: $BASH_SOURCE create spot_yn[y|n] \$CLUSTER_NAME \$MASTER_COUNT \$CORE_COUNT"

  if [ -z $1 ] ; then
  	echo $USAGE
  	exit 1
  fi

  if [ -z $2 ] ; then
  	echo $USAGE
  	exit 1
  fi

  if [ -z $3 ] ; then
    echo $USAGE
    exit 1
  fi

  if [ -z $4 ] ; then
    echo $USAGE
    exit 1
  fi

  SPOT_YN=$1
  CLUSTER_NAME=$2
  MASTER_INSTANCES=$3
  EXECUTOR_INSTANCES=$4


  if [ $SPOT_YN = "y" ] ; then

    INSTANCE_GROUPS='[{"InstanceCount":'"${MASTER_INSTANCES}"',"BidPrice":"OnDemandPrice","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":'"\"${MASTER_INSTANCE_TYPE}\""',"Name":"Master Instance Group - 1"},{"InstanceCount":'"${EXECUTOR_INSTANCES}"',"BidPrice":"OnDemandPrice","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":64,"VolumeType":"gp2"},"VolumesPerInstance":4}]},"InstanceGroupType":"CORE","InstanceType":'"\"${CORE_INSTANCE_TYPE}\""',"Name":"Core Instance Group - 2"}]'

    AUTO_TERMINATION='--auto-termination-policy IdleTimeout=60 '

  else

    INSTANCE_GROUPS='[{"InstanceCount":'"${MASTER_INSTANCES}"',"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":'"\"${MASTER_INSTANCE_TYPE}\""',"Name":"Master Instance Group - 1"},{"InstanceCount":'"${EXECUTOR_INSTANCES}"',"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":'"\"${CORE_INSTANCE_TYPE}\""',"Name":"Core Instance Group - 2"}]'

    AUTO_TERMINATION=' '
  fi


  CONFIGURATION="file://${PROJECT_ROOT}/emr_config/configuration.json"

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
  --release-label emr-6.7.0 \
  --log-uri "$S3_LOG_URI" \
  --instance-groups "$INSTANCE_GROUPS" \
  --configurations "$CONFIGURATION" \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --ebs-root-volume-size 10 \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --name "$CLUSTER_NAME" $AUTO_TERMINATION \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region ap-northeast-2

  #--auto-termination-policy '{"IdleTimeout":600}' \
  ##--auto-termination-policy '{"IdleTimeout":3600}' \
}

function cluster_list {
  gcloud dataproc clusters list --region asia-northeast3 --filter='status.state = ACTIVE'
}

function cluster_status {
  if [[ "$1" == "" ]]; then
    error "missing cluster id"
    exit 1
  fi
  gcloud dataproc clusters describe finder-prod-streaming-v2 --region asia-northeast3 | grep state: | awk '{print $2}' | grep -v state: | xargs
}

# return yes or no
function cluster_is_runnable {
  local stat=`cluster_status $1`

  case "$stat" in
    "RUNNING"|"WAITING"|"STARTING")
      echo "yes"
      ;;
    *)
      echo "no"
      ;;
  esac
}

if [[ "$RELEASE" == "" ]];then
  case $1 in
    "create")
      cluster_create $2 $3 $4 $5
      ;;
    "list")
      cluster_list
      ;;
    "status")
      cluster_status $2
      ;;
    *)
      echo "invalid usage (available: create, list, status)"
      ;;
  esac

fi
