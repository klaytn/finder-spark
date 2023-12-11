#!/usr/bin/env bash

function main() {
  # Check if logined
  gcloud auth list | grep ACTIVE > /dev/null
  if [[ $? -eq 0 ]]; then
    echo "already logined"
  else
  gcloud auth login
  gcloud auth application-default login
  fi
}

main
