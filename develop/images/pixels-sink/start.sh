#!/bin/sh
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
