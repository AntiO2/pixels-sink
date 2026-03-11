#!/bin/sh

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

# image entrypoint
set -euo pipefail

JVM_CONFIG_FILE="${JVM_CONFIG_FILE:-/app/jvm.conf}"
PROPERTIES_FILE="${PROPERTIES_FILE:-pixels-sink.properties}"

if [ -f "${JVM_CONFIG_FILE}" ]; then
  JVM_OPTION=$(grep -v '^[[:space:]]*#' "${JVM_CONFIG_FILE}" | grep -v '^[[:space:]]*$' | xargs)
else
  JVM_OPTION="${JVM_OPTION:--Xmx4096m -Xmn1024m}"
fi

echo "Starting Pixels Sink"
echo "JAR_FILE        = ${JAR_FILE}"
echo "PROPERTIES_FILE = ${PROPERTIES_FILE}"
echo "JVM_CONFIG_FILE = ${JVM_CONFIG_FILE}"
echo "JVM_OPTION      = ${JVM_OPTION}"

exec java ${JVM_OPTION} -jar "${JAR_FILE}" -c "${PROPERTIES_FILE}"
