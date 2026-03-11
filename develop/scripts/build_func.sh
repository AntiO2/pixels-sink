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

build_pixels_sink_image() {
    mvn clean package dockerfile:build -f ${PROJECT_DIR}/pom.xml
    check_fatal_exit "Fail to build pixels sink image"
    log_info "success build pixels_sink ${PIXELS_SINK_VERSION}"
}

build_image() {
    local BUILD_DIR=$1
    local IMAGE_NAME=$2
    local IMAGE_VERSION=${3:-${PIXELS_SINK_VERSION}}
    if [ -z "$BUILD_DIR" ]; then
        log_fatal_exit "Please provide a directory to build the image."
    fi
    if [ ! -d "$BUILD_DIR" ]; then
        log_fatal_exit "Directory '$BUILD_DIR' does not exist."
    fi

    if [ -z "$IMAGE_NAME" ]; then 
        log_fatal_exit "image name is empty"
    fi

    log_info "Building Docker image ${IMAGE_NAME}:${IMAGE_VERSION} from directory: $BUILD_DIR"
    docker build -t ${IMAGE_NAME}:${IMAGE_VERSION} $BUILD_DIR

    check_fatal_exit "Failed to build Docker image."
    
    log_info "Succ Build ${IMAGE_NAME}:${IMAGE_VERSION}"
}
