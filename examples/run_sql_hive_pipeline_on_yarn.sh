#!/bin/sh

set -eu

if [ -z "${SPARK_HOME:-}" ]; then
  echo "Please export SPARK_HOME before running this script." >&2
  exit 1
fi

if [ -z "${SPARK_HIVE_CONF:-}" ]; then
  echo "Please export SPARK_HIVE_CONF as hive-site.xml or its containing directory." >&2
  exit 1
fi

if [ -d "${SPARK_HIVE_CONF}" ]; then
  HIVE_SITE_PATH="${SPARK_HIVE_CONF%/}/hive-site.xml"
else
  HIVE_SITE_PATH="${SPARK_HIVE_CONF}"
fi

if [ ! -f "${HIVE_SITE_PATH}" ]; then
  echo "Could not find hive-site.xml: ${HIVE_SITE_PATH}" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/sql-hive-insert-pipeline"
SPARK_SDP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

set -- \
  --spec "${PROJECT_ROOT}" \
  --files "${HIVE_SITE_PATH}"

exec "${SPARK_SDP_HOME}/bin/spark-sdp.sh" "$@"
