#!/bin/sh

set -eu

if [ -z "${SPARK_HOME:-}" ]; then
  echo "Please export SPARK_HOME before running this script." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/sql-batch-pipeline"
SPARK_SDP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

exec "${SPARK_SDP_HOME}/bin/spark-sdp.sh" \
  --spec "${PROJECT_ROOT}"
