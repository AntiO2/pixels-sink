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

shutdown_containers() {
    docker compose  -f ${DEVELOP_DIR}/docker-compose.yml down -v
}

clean_images() {
    local images=("pixels-debezium:${PIXELS_SINK_VERSION}")

    for image in "${images[@]}"; do
        log_info "Deleting image: $image"
        docker rmi -f "$image" 2>/dev/null || echo "Failed to delete image: $image"
    done
}


