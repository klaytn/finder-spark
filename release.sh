#!/usr/bin/env bash

RELEASE="true"
SCRIPT_PATH=${BASH_SOURCE[0]}
PROJECT_ROOT=$(cd $(dirname $(readlink ${SCRIPT_PATH} || echo ${SCRIPT_PATH}))/;/bin/pwd)
source $PROJECT_ROOT/gcp/util/common.sh
source $PROJECT_ROOT/gcp/login.sh
source $PROJECT_ROOT/gcp/gcs.sh
source $PROJECT_ROOT/gcp/step.sh
source $PROJECT_ROOT/gcp/code.sh
source $PROJECT_ROOT/gcp/validator.sh

function parse_parameters {
  while [[ $# -gt 0 ]]; do
    case $1 in
      --build)
        BUILD="true"
        shift
        ;;
      --deploy)
        DEPLOY="true"
        shift
        ;;
      *)
        POSITIONAL_ARGS+=("$1")
        shift
        ;;
    esac
  done

  # restore positional parameters
  set -- "${POSITIONAL_ARGS[@]}"
}

function sbt_clean_package() {
  log "-- sbt clean assembly -"
  sudo $PROJECT_ROOT/sbt klaytn-spark/clean klaytn-spark/assembly
  check_and_exit
}

DEPLOY=""
BUILD=""
PROGRAM_ARGS=""
POSITIONAL_ARGS=()
parse_parameters $@


PS3="Select phase: "
    select PHASE in "prod" "dev"
    do
        case $PHASE in
            "prod")
                break
                ;;
            "dev")
                break
                ;;
            *)
                echo "Invalid PHASE"
                exit 1
                break
                ;;
        esac
    done

export S3_BUCKET="klaytn-$PHASE-lake"
export S3_JAR_PATH="gs://klaytn-$PHASE-spark/jars"

function main() {
  local usage="$0 \$CLUSTER_ID \$MAIN_CLASS \$CHAIN"

  local cluster_id=$1
  local main_class=$2
  local chain=$3

  validator_validate_step_add "$cluster_id" "$main_class" "$chain" "$usage"

  local step_name="$main_class-$PHASE-$chain"
  local config_resource=`code_get_config_resource $main_class $PHASE $chain`

  if [[ "$BUILD" == "true" ]]; then
    sbt_clean_package
  fi

# TODO
#  if [[ "$DEPLOY" == "true "]]; then
  local s3_path=`upload_app_jar`

  log "-- upload s3 path: $s3_path"
#  fi

  # TODO: print meta file location
  step_add $1 $2 $3 $s3_path
}

main $*
