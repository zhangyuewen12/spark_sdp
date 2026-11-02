#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: start_pipeline_job.sh <model-directory> <job-name>
Example: ./bin/start_pipeline_job.sh dws order

Automatically loads:
  lib/spark-sdp-1.0.jar
  lib/spark-connector-1.0.jar
  models/<model-directory>/<job-name>.sql
  config/<model-directory>/<job-name>.yaml
  properties/application.properties

The command submits one Spark job. No additional submit options are accepted.
USAGE
}

fail() {
  echo "Error: $*" >&2
  exit 2
}

if [[ $# -eq 1 && ( "$1" == '--help' || "$1" == '-h' ) ]]; then
  usage
  exit 0
fi
[[ $# -eq 2 ]] || { usage >&2; exit 2; }
MODEL_DIRECTORY="${1%/}"
JOB_NAME="${2%.sql}"
case "${MODEL_DIRECTORY}" in
  ''|/*|..|../*|*/../*|*/..|-*) fail "Model directory must be relative to models/, without '..'." ;;
esac
case "${JOB_NAME}" in
  ''|.|..|*/*|-*) fail "Job name must be a file name such as order, without '/'." ;;
esac
MODEL="${MODEL_DIRECTORY}/${JOB_NAME}"

MAIN_JAR="${PROJECT_HOME}/lib/spark-sdp-1.0.jar"
CONNECTOR_JAR="${PROJECT_HOME}/lib/spark-connector-1.0.jar"
TASK_FILE="${PROJECT_HOME}/models/${MODEL}.sql"
SPEC_FILE="${PROJECT_HOME}/config/${MODEL}.yaml"
ENV_FILE="${PROJECT_HOME}/properties/application.properties"

for jar in "${MAIN_JAR}" "${CONNECTOR_JAR}"; do
  [[ -f "${jar}" && -r "${jar}" && -s "${jar}" ]] ||
    fail "Platform JAR is missing, unreadable or empty: ${jar}. Contact the platform team."
done
for file in "${TASK_FILE}" "${SPEC_FILE}" "${ENV_FILE}"; do
  [[ -f "${file}" && -r "${file}" && -s "${file}" ]] ||
    fail "Project file is missing, unreadable or empty: ${file}"
done

JAVA_BIN="java"
if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA_BIN="${JAVA_HOME}/bin/java"
fi
command -v "${JAVA_BIN}" >/dev/null 2>&1 || fail "Java executable not found: ${JAVA_BIN}"

# Resolve relative Spark dependency/file paths against the SQL project, regardless
# of the caller's working directory. The project owns the environment properties.
cd "${PROJECT_HOME}"
exec "${JAVA_BIN}" -cp "${MAIN_JAR}:${CONNECTOR_JAR}" \
  com.bocom.rdss.spark.sdp3x.starter.SparkSubmitStarter \
  --spec "${SPEC_FILE}" --task-file "${TASK_FILE}" --props "${ENV_FILE}"
