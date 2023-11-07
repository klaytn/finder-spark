#!/usr/bin/env bash

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
source $PROJECT_ROOT/bin_v3/util/common.sh
source $PROJECT_ROOT/bin_v3/login.sh
source $PROJECT_ROOT/bin_v3/validator.sh
source $PROJECT_ROOT/bin_v3/hocon_cli.sh
source $PROJECT_ROOT/bin_v3/validator.sh
source $PROJECT_ROOT/bin_v3/code.sh

function step_add() {
  local usage="$0 step_add \$cluster_id \$main_class \$chain \$s3_jar_path"

  local cluster_id=$1
  local main_class=$2
  local chain=$3
  local s3_jar_path=$4

  validator_validate_step_add "$cluster_id" "$main_class" "$chain" "$usage"

  local spark_app_name="`echo $main_class | rev | cut -d. -f1 | rev`-$PHASE-$chain"
  local step_name="$main_class-$PHASE-$chain"
  local config_resource=`code_get_config_resource $main_class $PHASE $chain`

  local driver_cores=`hocon_get_or_else $config_resource spark.driver.cores 1`
  local driver_memory=`hocon_get_or_else $config_resource spark.driver.memory 2000`
  local executor_num=`hocon_get_or_else $config_resource spark.executor.instances 2`
  local executor_cores=`hocon_get_or_else $config_resource spark.executor.cores 1`
  local executor_memory=`hocon_get_or_else $config_resource spark.executor.memory 2000`
  local max_rate_per_partition=`hocon_get_or_else $config_resource spark.streaming.kafka.maxRatePerPartition 0`
  local max_rate=`hocon_get_or_else $config_resource spark.streaming.receiver.maxRate 0`

  log "——————————————————————————————————"
  log " Configuration File : ${config_resource}"
  log "——————————————————————————————————"
  warn "spark.driver.cores = $driver_cores"
  warn "spark.driver.memory = $driver_memory"
  warn "spark.executor.instances = $executor_num"
  warn "spark.executor.cores = $executor_cores"
  warn "spark.executor.memory = $executor_memory"
  if [ $max_rate_per_partition -ne 0 ] ; then
    warn "spark.streaming.kafka.maxRatePerPartition = $max_rate_per_partition"
  fi
  if [ $max_rate -ne 0 ] ; then
    warn "spark.streaming.receiver.maxRate = $max_rate"
  fi

  local args=$(cat <<EOF
[
--driver-required-vcore=$driver_cores,
--driver-required-memory-mb=$driver_memory,
--num-executors,$executor_num,
--executor-cores,$executor_cores,
--executor-memory,$executor_memory,
--properties=[spark.app.phase=$PHASE,spark.app.chain=$chain,spark.yarn.maxAppAttempts=1,spark.driver.extraJavaOptions=-Dconfig.resource=$config_resource,spark.executor.extraJavaOptions=-Dconfig.resource=$config_resourcespark.dynamicAllocation.enabled=false,spark.serializer=org.apache.spark.serializer.KryoSerializer,spark.kryoserializer.buffer.max=128m],
--properties-file=/etc/spark/conf/spark-defaults.conf,
--cluster=$cluster_id,
--class=$main_class,
--jar=$s3_jar_path,
--region=asia-northeast3
]
EOF)

  if [ $max_rate_per_partition -ne 0 ] ; then
    args=$(echo $args | sed "s/#CUSTOM_CONFIG#/\-\-conf,spark.streaming.kafka.maxRatePerPartition=$max_rate_per_partition,#CUSTOM_CONFIG#/g")
  fi
  if [ $max_rate -ne 0 ] ; then
    args=$(echo $args | sed "s/#CUSTOM_CONFIG#/\-\-conf,spark.streaming.receiver.maxRate=$max_rate,#CUSTOM_CONFIG#/g")
  fi
  args=$(echo $args | sed 's/#CUSTOM_CONFIG#//g')
  args=$(echo $args | sed 's/ //g')

  log "spark-submit arguments: $args"

  gcloud dataproc jobs submit spark \
    --region=asia-northeast3 \
    --cluster=$cluster_id \
    --class=$main_class \
    --jars=$s3_jar_path \
    --properties=spark.app.phase=$PHASE,spark.app.chain=$chain,spark.yarn.maxAppAttempts=1,spark.driver.extraJavaOptions=-Dconfig.resource=$config_resource,spark.executor.extraJavaOptions=-Dconfig.resource=$config_resource,spark.dynamicAllocation.enabled=false,spark.serializer=org.apache.spark.serializer.KryoSerializer,spark.kryoserializer.buffer.max=128m \
    --region=asia-northeast3 \
    -- $s3_jar_path
}

if [[ "$RELEASE" == "" ]];then
  case $1 in
    "add")
      step_add $2 $3 $4 $5
      ;;
    *)
      echo "invalid usage (available: add)"
      ;;
  esac
fi
