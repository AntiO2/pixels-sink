#!/bin/bash

# Copyright 2026 PixelsDB.
#
# This file is part of Pixels.
#
# Pixels is free software: you can redistribute it and/or modify
# it under the terms of the Affero GNU General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# Pixels is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Affero GNU General Public License for more details.
#
# You should have received a copy of the Affero GNU General Public
# License along with Pixels.  If not, see
# <https://www.gnu.org/licenses/>.



SOURCE_PATH=`readlink -f $BASH_SOURCE` 2>/dev/null

if [[ -z ${SOURCE_PATH} ]]; then
    # for debug
    SOURCE_PATH=`readlink -f $0`
fi

SCRIPT_DIR=`dirname ${SOURCE_PATH}`
DEVELOP_DIR=`dirname ${SCRIPT_DIR}`
PROJECT_DIR=`dirname ${DEVELOP_DIR}`

SECRETS_DIR=${DEVELOP_DIR}/secrets
CONFIG_DIR=${DEVELOP_DIR}/config
IMAGE_DIR=${DEVELOP_DIR}/images

source ${DEVELOP_DIR}/.env
source ${SCRIPT_DIR}/build_func.sh
source ${SCRIPT_DIR}/log_func.sh
source ${SCRIPT_DIR}/docker_func.sh
source ${SCRIPT_DIR}/util_func.sh
source ${SCRIPT_DIR}/gen_data.sh
