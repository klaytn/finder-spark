#!/usr/bin/env bash
SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(/bin/pwd)
JAVA_CLASSPATH="${PROJECT_ROOT}/project-tool/cli/target/scala-2.12/klaytn-cli.jar"
JAVA_CLASSPATH="$JAVA_CLASSPATH:${PROJECT_ROOT}/project-app/spark/src/main/scala"
JAVA_CLASSPATH="$JAVA_CLASSPATH:${PROJECT_ROOT}/project-app/spark/src/main/resources"

function hocon_get() {
  java -cp ${JAVA_CLASSPATH} io.klaytn.tools.HoconCli $*
}

function hocon_get_or_else() {
  local resource=$1
  local key=$2
  local default=$3
  local config=`hocon_get $resource $key`
  if [[ -z $config ]]; then
    echo $default
  else
    echo $config
  fi
}
