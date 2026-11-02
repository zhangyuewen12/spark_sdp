#!/usr/bin/env bash
# Platform entrypoint: operation, project root, workflow directory, environment.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ds-sync.sh <generate|apply> <projectHome> <workflowHome> <env>
Examples:
  ./bin/ds-sync.sh generate /opt/spark/my-project ads sit
  ./bin/ds-sync.sh apply /opt/spark/my-project ads uat

projectHome   SQL project root containing models/config/properties/lib.
workflowHome  Group name under models/, e.g. ads or dws/trade.
env           sit/uat/prd etc.; reads properties/application-<env>.properties.
generate is offline. Every apply creates a new versioned workflow.
USAGE
}

if [[ $# -eq 1 && ( "$1" == '--help' || "$1" == '-h' ) ]]; then
  usage
  exit 0
fi
[[ $# -eq 4 ]] || { usage >&2; exit 2; }
[[ "$1" == generate || "$1" == apply ]] || { usage >&2; exit 2; }
[[ -n "$2" && -n "$3" && "$4" =~ ^[A-Za-z0-9_-]+$ ]] || {
  echo 'projectHome, workflowHome and env are required; no default environment is used.' >&2
  exit 2
}
[[ -d "$2" ]] || { echo 'projectHome must be an existing project directory.' >&2; exit 2; }
PROJECT_HOME="$(cd -- "$2" && pwd)"
MAIN_JAR="${PROJECT_HOME}/lib/spark-sdp-1.0.jar"
ENV_FILE="${PROJECT_HOME}/properties/application-$4.properties"
[[ -f "${ENV_FILE}" && -r "${ENV_FILE}" && -s "${ENV_FILE}" ]] || {
  echo "Environment configuration is missing, unreadable or empty: ${ENV_FILE}" >&2
  exit 2
}
[[ -r "${MAIN_JAR}" && -f "${MAIN_JAR}" && -s "${MAIN_JAR}" ]] || {
  echo 'Runtime jar not found. Ask the platform team for lib/spark-sdp-1.0.jar.' >&2
  exit 1
}
[[ -n "${SPARK_HOME:-}" && -d "${SPARK_HOME}/jars" ]] || {
  echo 'Spark parser libraries are unavailable; contact the platform team.' >&2
  exit 1
}
JAVA_BIN="java"
if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA_BIN="${JAVA_HOME}/bin/java"
fi
# DS configuration is selected by the explicit env argument and read from the
# project's properties file by Java. No operating-system variables are forwarded.
exec "${JAVA_BIN}" -cp "${MAIN_JAR}:${SPARK_HOME}/jars/*" \
  com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsSyncApplication \
  "$1" "${PROJECT_HOME}" "$3" "$4"
