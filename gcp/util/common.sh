#!/usr/bin/env bash
function check_and_exit() {
    if [[ $? -eq 0 ]]; then
        echo OK
    else
        exit -1
    fi
}

function log {
    msg=$*
    echo "$(echo -e '\x1B[32m')${msg}$(echo -e '\x1B[0m')"
}

function warn {
    msg=$*
    echo "$(echo -e '\x1B[31m')${msg}$(echo -e '\x1B[0m')"
}

function error {
    msg=$*
    echo "$(echo -e '\x1B[31m')${msg}$(echo -e '\x1B[0m')"
}
