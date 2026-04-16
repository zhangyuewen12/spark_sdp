#!/usr/bin/env bash

set -euo pipefail

if [[ -z "${SPARK_HOME:-}" ]]; then
  echo "Please export SPARK_HOME before running this script." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/sql-hive-insert-pipeline"
SPARK_SDP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

EXTRA_ARGS=()
if [[ -n "${HIVE_SITE_FILE:-}" ]]; then
  EXTRA_ARGS+=(--files "${HIVE_SITE_FILE}")
fi

cd "${PROJECT_ROOT}"

"${SPARK_SDP_HOME}/bin/spark-sdp" \
  --master yarn \
  --deploy-mode cluster \
  --queue default \
  "${EXTRA_ARGS[@]}" \
  run
