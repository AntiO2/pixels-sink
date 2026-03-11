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


log() {
    local level="$1"
    shift
    local message="$@"
    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")

    case "$level" in
        DEBUG)
            echo -e "\e[34m[$timestamp] [DEBUG] $message\e[0m" >&2 ;; 
        INFO)
            echo -e "\e[32m[$timestamp] [INFO] $message\e[0m" >&2 ;;
        WARNING)
            echo -e "\e[33m[$timestamp] [WARNING] $message\e[0m" >&2 ;;
        FATAL)
            echo -e "\e[31m[$timestamp] [FATAL] $message\e[0m" >&2 ;;
        *)
            echo -e "\e[37m[$timestamp] [UNKNOWN] $message\e[0m" >&2 ;;
    esac
}

log_debug() {
    log DEBUG "$@"
}

log_info() {
    log INFO "$@"
}

log_warning() {
    log WARNING "$@"
}

log_fatal() {
    log FATAL "$@"
}

log_fatal_exit() {
    log_fatal "$@"
    exit 1
}

