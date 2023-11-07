#!/usr/bin/env bash

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)

source $PROJECT_ROOT/bin_v3/util/common.sh
source $PROJECT_ROOT/bin_v3/cluster.sh
source $PROJECT_ROOT/bin_v3/gcs.sh

function validator_validate_step_add() {
  local cluster_id=$1
  local main_class=$2
  local chain=$3
  local usage=$4

  if [[ "$cluster_id" == "" ]]; then
    cluster_list
    error $usage
    exit 1
  fi

  if [[ "$main_class" == "" ]]; then
    error "Class is not given."
    error $usage
    exit 1
  fi

  if [[ "$chain" == "" ]]; then
    error "Chain is no given. (Refer to Chain.scala)"
    exit 1
  fi

  if [[ "$cluster_id" == "" ]]; then
    error "Cluster-ID is not given."
    cluster_list
    error $usage
    exit 1
  fi

  local is_cluster_runnable=`cluster_is_runnable $cluster_id`
  if [[ "$is_cluster_runnable" == "no" ]]; then
    error "'$cluster_id' is not running"
    cluster_list
    error $usage
    exit 1
  fi
}