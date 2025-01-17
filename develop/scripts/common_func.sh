#!/bin/bash


SOURCE_PATH=`readlink -f $BASH_SOURCE` 2>/dev/null

if [[ -z ${SOURCE_PATH} ]]; then
    # for debug
    SOURCE_PATH=`readlink -f $0`
fi

SCRIPT_DIR=`dirname ${SOURCE_PATH}`
PROJECT_DIR=`dirname ${SCRIPT_DIR}`
SECRETS_DIR=${PROJECT_DIR}/secrets
CONFIG_DIR=${PROJECT_DIR}/config
IMAGE_DIR=${PROJECT_DIR}/images

source ${PROJECT_DIR}/.env
source ${SCRIPT_DIR}/log_func.sh
source ${SCRIPT_DIR}/docker_func.sh
source ${SCRIPT_DIR}/util_func.sh