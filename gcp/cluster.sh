#!/usr/bin/env bash

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)

source $PROJECT_ROOT/gcp/util/common.sh


function cluster_list {
  gcloud dataproc clusters list --region asia-northeast3 --filter='status.state = ACTIVE'
}

function cluster_status {
  if [[ "$1" == "" ]]; then
    error "missing cluster id"
    exit 1
  fi
  gcloud dataproc clusters describe $1 --region asia-northeast3 | grep state: | awk '{print $2}' | grep -v state: | xargs
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
