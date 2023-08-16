#!/usr/bin/env bash

function main() {
  if [[ "$AWS_PROFILE" == "" ]]; then
    echo "'\$AWS_PROFILE' not exist"
    exit 1
  fi

  aws s3 ls $S3_BUCKET &> /dev/null
  RESULT=$?

  if [[ "$RESULT" != "0" ]];then
    echo "LOGIN FAILED. EXECUTE AWS_VAULT (Command Run Again)"
    unset AWS_VAULT
    aws-vault exec $AWS_PROFILE
    exit
  elif [[ "$CUSTOM_PS1" != "" ]]; then
    export PS1="[$CUSTOM_PS1] "
  else
    export PS1="[$AWS_PROFILE] "
  fi
}

main