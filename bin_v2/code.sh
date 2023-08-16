#!/usr/bin/env bash

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
source $PROJECT_ROOT/bin_v2/util/common.sh

function code_load_project_name_from_class() {
  local class_name=$1
  local phase=$2
  local chain=$3

  local class2path=`sed "s/[.]/\//g" <<< ${class_name}`
  local findpath=`find . -name "*.scala" | grep -v "/target/" | grep "${class2path}.scala"`
  regex='/([^/]*)/([^/]*)/src/main'
  [[ $findpath =~ $regex ]]

  local config_file_path="${findpath%.*}-$phase-$chain.conf"
  if [[ ! -f ${config_file_path} ]]; then
    error "${class2path}-$phase-$chain.conf file was not found.."
    exit 1
  fi

  PROJECT_BASE_DIR_NAME=${BASH_REMATCH[1]}
  PROJECT_NAME=${BASH_REMATCH[2]}
}

function code_get_config_resource() {
  local main_class=$1
  local phase=$2
  local chain=$3

  code_load_project_name_from_class $main_class $phase $chain

  local config_resource="`sed "s/[.]/\//g" <<< ${main_class}`-${phase}-${chain}.conf"
  local config_resource_path=$PROJECT_ROOT/$PROJECT_BASE_DIR_NAME/$PROJECT_NAME/src/main/scala/$config_resource
  if [[ ! -f "$config_resource_path" ]]; then
    error "'$config_resource_path' missing"
    exit 1
  else
    echo $config_resource
  fi
}

