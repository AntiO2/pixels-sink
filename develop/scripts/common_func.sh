#!/bin/bash


SOURCE_PATH=`readlink -f $BASH_SOURCE`

if [[ -z ${SOURCE_PATH} ]]; then
    # for debug
    SOURCE_PATH=`readlink -f $0`
fi

SCRIPT_DIR=`dirname ${SOURCE_PATH}`
PROJECT_DIR=`dirname ${SCRIPT_DIR}`

source ${SCRIPT_DIR}/log_func.sh
source ${SCRIPT_DIR}/clean_func.sh
source ${SCRIPT_DIR}/check_func.sh