#!/bin/sh

SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/../;/bin/pwd)
SCRIPT_ROOT=$PROJECT_ROOT/bin

CONFIGURATION="file://${PROJECT_ROOT}/emr_config/configuration.json"

echo $CONFIGURATION
