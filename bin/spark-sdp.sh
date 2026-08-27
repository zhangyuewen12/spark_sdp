#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

resolve_jar() {
  if [[ -n "${SPARK_SDP_JAR:-}" ]]; then
    [[ -f "${SPARK_SDP_JAR}" ]] || {
      echo "Configured SPARK_SDP_JAR does not exist: ${SPARK_SDP_JAR}" >&2
      exit 1
    }
    printf '%s\n' "${SPARK_SDP_JAR}"
    return
  fi

  local packaged_jar="${SCRIPT_DIR}/spark-sdp-1.0.jar"
  local build_jar="${PROJECT_HOME}/spark-core/target/spark-sdp-1.0.jar"
  if [[ -f "${packaged_jar}" ]]; then
    printf '%s\n' "${packaged_jar}"
  elif [[ -f "${build_jar}" ]]; then
    printf '%s\n' "${build_jar}"
  else
    echo "Could not find spark-sdp-1.0.jar. Run: ./mvnw -pl spark-core package" >&2
    exit 1
  fi
}

MAIN_JAR="$(resolve_jar)"

# SparkSubmitStarter is the jar's Main-Class. Java owns configuration parsing,
# job archiving, spark-submit command construction and process execution.
exec java ${SPARK_SDP_JAVA_OPTS:-} -jar "${MAIN_JAR}" "$@"
