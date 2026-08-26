#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_SDP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"
MAIN_CLASS="com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain"
PROJECT_ARCHIVE_ALIAS="spark-sdp-project"

resolve_jar() {
  if [[ -n "${SPARK_SDP_JAR:-}" ]]; then
    if [[ ! -f "${SPARK_SDP_JAR}" ]]; then
      echo "Configured SPARK_SDP_JAR does not exist: ${SPARK_SDP_JAR}" >&2
      exit 1
    fi
    printf '%s\n' "${SPARK_SDP_JAR}"
    return
  fi

  local jar="${SCRIPT_DIR}/spark-sdp-1.0.jar"
  local module_jar="${SPARK_SDP_HOME}/spark-core/target/spark-sdp-1.0.jar"
  if [[ -f "${jar}" ]]; then
    printf '%s\n' "${jar}"
    return
  fi
  if [[ -f "${module_jar}" ]]; then
    printf '%s\n' "${module_jar}"
    return
  fi

  if [[ ! -f "${jar}" ]]; then
    cat >&2 <<EOF
Could not find ${jar} or ${module_jar}.

Build and place it next to this script:
  cd ${SPARK_SDP_HOME}
  ./mvnw -pl spark-core package
  cp spark-core/target/spark-sdp-1.0.jar ${SCRIPT_DIR}/
EOF
    exit 1
  fi
}

require_spark_home() {
  if [[ -z "${SPARK_HOME:-}" ]]; then
    echo "SPARK_HOME is not set. Please point it to your local Spark installation." >&2
    exit 1
  fi
  if [[ ! -x "${SPARK_HOME}/bin/spark-submit" ]]; then
    echo "Could not find executable spark-submit under ${SPARK_HOME}/bin/spark-submit" >&2
    exit 1
  fi
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

absolute_path() {
  local path="$1"
  if [[ "${path}" == /* ]]; then
    printf '%s\n' "${path}"
  else
    printf '%s/%s\n' "${PWD}" "${path}"
  fi
}

split_args() {
  SPARK_SUBMIT_ARGS=()
  PIPELINE_ARGS=()

  local found_command="false"
  for arg in "$@"; do
    if [[ "${found_command}" == "false" && ( "${arg}" == "run" || "${arg}" == "dry-run" || "${arg}" == "help" ) ]]; then
      found_command="true"
      PIPELINE_ARGS+=("${arg}")
    elif [[ "${found_command}" == "true" ]]; then
      PIPELINE_ARGS+=("${arg}")
    else
      SPARK_SUBMIT_ARGS+=("${arg}")
    fi
  done
}

find_pipeline_spec() {
  local current_dir="$1"
  local found=()
  if [[ -f "${current_dir}/spark-pipeline.properties" ]]; then
    found+=("${current_dir}/spark-pipeline.properties")
  fi
  if [[ -f "${current_dir}/spark-pipeline.yml" ]]; then
    found+=("${current_dir}/spark-pipeline.yml")
  fi
  if [[ -f "${current_dir}/spark-pipeline.yaml" ]]; then
    found+=("${current_dir}/spark-pipeline.yaml")
  fi

  if [[ ${#found[@]} -eq 1 ]]; then
    printf '%s\n' "${found[0]}"
    return
  fi
  if [[ ${#found[@]} -gt 1 ]]; then
    echo "Multiple pipeline spec files found under ${current_dir}" >&2
    exit 1
  fi

  echo "Could not find spark-pipeline.properties, spark-pipeline.yml, or spark-pipeline.yaml under ${current_dir}." >&2
  exit 1
}

resolve_pipeline_spec() {
  local command="$1"
  shift
  local args=("$@")
  local i=1
  local positional=""

  while [[ ${i} -lt ${#args[@]} ]]; do
    local arg="${args[${i}]}"
    if [[ "${arg}" == "--spec" ]]; then
      local spec_path="${args[$((i + 1))]:-}"
      if [[ -z "${spec_path}" ]]; then
        echo "Missing value for --spec." >&2
        exit 1
      fi
      if [[ "${spec_path}" != /* ]]; then
        spec_path="$(cd "${PWD}" && printf '%s/%s\n' "${PWD}" "${spec_path}")"
      fi
      printf '%s\n' "${spec_path}"
      return
    elif [[ "${arg}" == "--master" ]]; then
      i=$((i + 2))
      continue
    elif [[ "${arg}" == "--submitted" || "${arg}" == "--dry-run" ]]; then
      i=$((i + 1))
      continue
    fi
    if [[ "${arg}" != --* && -z "${positional}" ]]; then
      positional="${arg}"
    fi
    i=$((i + 1))
  done

  if [[ -n "${positional}" ]]; then
    if [[ "${positional}" != /* ]]; then
      positional="$(cd "${PWD}" && printf '%s/%s\n' "${PWD}" "${positional}")"
    fi
    if [[ -f "${positional}" ]]; then
      printf '%s\n' "${positional}"
      return
    fi
    if [[ -d "${positional}" ]]; then
      find_pipeline_spec "${positional}"
      return
    fi
    echo "Pipeline path does not exist: ${positional}" >&2
    exit 1
  fi

  find_pipeline_spec "${PWD}"
}

is_cluster_deploy_mode() {
  local i=0
  while [[ ${i} -lt ${#SPARK_SUBMIT_ARGS[@]} ]]; do
    if [[ "${SPARK_SUBMIT_ARGS[${i}]}" == "--deploy-mode" && $((i + 1)) -lt ${#SPARK_SUBMIT_ARGS[@]} ]]; then
      if [[ "${SPARK_SUBMIT_ARGS[$((i + 1))]}" == "cluster" ]]; then
        return 0
      fi
    fi
    i=$((i + 1))
  done
  return 1
}

create_project_archive() {
  local project_root="$1"
  local archive_dir
  archive_dir="$(mktemp -d "${TMPDIR:-/tmp}/spark-sdp-project.XXXXXX")"
  local archive_path="${archive_dir}/project.zip"

  if command -v zip >/dev/null 2>&1; then
    (
      cd "${project_root}"
      zip -qr "${archive_path}" .
    )
  else
    (
      cd "${project_root}"
      jar cfM "${archive_path}" .
    )
  fi

  printf '%s\n' "${archive_path}"
}

append_spark_option() {
  local key="$1"
  local value="$2"
  local normalized_key
  normalized_key="$(printf '%s' "${key}" | tr '[:upper:]_' '[:lower:]-')"

  case "${normalized_key}" in
    master)
      HAS_MASTER=true
      SPARK_SUBMIT_ARGS+=(--master "${value}")
      ;;
    deploy-mode|deploymode)
      HAS_DEPLOY_MODE=true
      SPARK_SUBMIT_ARGS+=(--deploy-mode "${value}")
      ;;
    queue)
      SPARK_SUBMIT_ARGS+=(--queue "${value}")
      ;;
    name)
      SPARK_SUBMIT_ARGS+=(--name "${value}")
      ;;
    driver-memory|drivermemory|driver.memory)
      SPARK_SUBMIT_ARGS+=(--driver-memory "${value}")
      ;;
    driver-cores|drivercores|driver.cores)
      SPARK_SUBMIT_ARGS+=(--driver-cores "${value}")
      ;;
    executor-memory|executormemory|executor.memory)
      SPARK_SUBMIT_ARGS+=(--executor-memory "${value}")
      ;;
    executor-cores|executorcores|executor.cores)
      SPARK_SUBMIT_ARGS+=(--executor-cores "${value}")
      ;;
    num-executors|numexecutors|executor.num|executor.number)
      SPARK_SUBMIT_ARGS+=(--num-executors "${value}")
      ;;
    total-executor-cores|totalexecutorcores|total.executor.cores)
      SPARK_SUBMIT_ARGS+=(--total-executor-cores "${value}")
      ;;
    files)
      SPARK_SUBMIT_ARGS+=(--files "${value}")
      ;;
    jars)
      SPARK_SUBMIT_ARGS+=(--jars "${value}")
      ;;
    archives)
      SPARK_SUBMIT_ARGS+=(--archives "${value}")
      ;;
    principal)
      SPARK_SUBMIT_ARGS+=(--principal "${value}")
      ;;
    keytab)
      SPARK_SUBMIT_ARGS+=(--keytab "${value}")
      ;;
    conf.*)
      SPARK_SUBMIT_ARGS+=(--conf "${key#conf.}=${value}")
      ;;
    configuration.*)
      SPARK_SUBMIT_ARGS+=(--conf "${key#configuration.}=${value}")
      ;;
    spark.*)
      SPARK_SUBMIT_ARGS+=(--conf "${key}=${value}")
      ;;
    catalog|database|libraries)
      ;;
    *)
      fail "Unsupported resource config key '${key}'. Use spark.xxx/conf.spark.xxx for arbitrary Spark conf."
      ;;
  esac
}

load_resource_config() {
  local config_file="$1"
  local line_no=0

  while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
    line_no=$((line_no + 1))
    raw_line="${raw_line%$'\r'}"
    local line
    line="$(trim "${raw_line}")"

    [[ -z "${line}" ]] && continue
    [[ "${line}" == \#* ]] && continue

    if [[ "${line}" != *=* ]]; then
      fail "Invalid line ${line_no} in ${config_file}: expected key=value."
    fi

    local key="${line%%=*}"
    local value="${line#*=}"
    key="$(trim "${key}")"
    value="$(trim "${value}")"

    [[ -z "${key}" ]] && fail "Invalid line ${line_no} in ${config_file}: empty key."
    [[ -z "${value}" ]] && fail "Invalid line ${line_no} in ${config_file}: empty value for ${key}."

    append_spark_option "${key}" "${value}"
  done < "${config_file}"
}

is_named_job_invocation() {
  if [[ $# -ne 2 ]]; then
    return 1
  fi
  case "$1" in
    run|dry-run|help|--*)
      return 1
      ;;
  esac
  return 0
}

submit_run_command() {
  require_spark_home
  if is_cluster_deploy_mode; then
    SPEC_PATH="$(resolve_pipeline_spec "${COMMAND}" "${PIPELINE_ARGS[@]}")"
    PROJECT_ROOT="$(cd "$(dirname "${SPEC_PATH}")" && pwd)"
    SPEC_BASENAME="$(basename "${SPEC_PATH}")"
    PROJECT_ARCHIVE="$(create_project_archive "${PROJECT_ROOT}")"

    exec "${SPARK_HOME}/bin/spark-submit" \
      ${SPARK_SUBMIT_ARGS[@]+"${SPARK_SUBMIT_ARGS[@]}"} \
      --archives "${PROJECT_ARCHIVE}#${PROJECT_ARCHIVE_ALIAS}" \
      --class "${MAIN_CLASS}" \
      --jars /Users/ywzhang/hadoopapp/spark-3.3.1/jars/spark-hive_2.12-3.3.1.jar \
      --conf spark.driver.extraClassPath=./spark-hive_2.12-3.3.1.jar \
      "${MAIN_JAR}" \
      --submitted \
      run \
      --spec "${PROJECT_ARCHIVE_ALIAS}/${SPEC_BASENAME}"
  fi
  exec "${SPARK_HOME}/bin/spark-submit" \
    ${SPARK_SUBMIT_ARGS[@]+"${SPARK_SUBMIT_ARGS[@]}"} \
    --class "${MAIN_CLASS}" \
    "${MAIN_JAR}" \
    --submitted \
    "${PIPELINE_ARGS[@]}"
}

submit_named_job() {
  local job_name="$1"
  local jobs_root
  jobs_root="$(absolute_path "$2")"
  local job_dir="${jobs_root}/${job_name}"

  [[ -d "${jobs_root}" ]] || fail "Jobs root does not exist: ${jobs_root}"
  [[ -d "${job_dir}" ]] || fail "Job '${job_name}' does not exist under ${jobs_root}: ${job_dir}"

  local pipeline_spec
  pipeline_spec="$(find_pipeline_spec "${job_dir}")"
  if [[ "${pipeline_spec}" != *.properties ]]; then
    fail "Submitting by job name requires spark-pipeline.properties under ${job_dir}."
  fi

  SPARK_SUBMIT_ARGS=()
  HAS_MASTER=false
  HAS_DEPLOY_MODE=false

  load_resource_config "${pipeline_spec}"

  if [[ "${HAS_MASTER}" == "false" ]]; then
    SPARK_SUBMIT_ARGS=(--master yarn "${SPARK_SUBMIT_ARGS[@]}")
  fi
  if [[ "${HAS_DEPLOY_MODE}" == "false" ]]; then
    SPARK_SUBMIT_ARGS=(--deploy-mode cluster "${SPARK_SUBMIT_ARGS[@]}")
  fi

  PIPELINE_ARGS=(run --spec "${pipeline_spec}")
  COMMAND="run"

  echo "Submitting SDP job '${job_name}'"
  echo "  job directory: ${job_dir}"
  echo "  pipeline spec: ${pipeline_spec}"

  submit_run_command
}

MAIN_JAR="$(resolve_jar)"

if is_named_job_invocation "$@"; then
  submit_named_job "$1" "$2"
fi

split_args "$@"

if [[ ${#PIPELINE_ARGS[@]} -eq 0 ]]; then
  exec java ${SPARK_SDP_JAVA_OPTS:-} -jar "${MAIN_JAR}" "$@"
fi

COMMAND="${PIPELINE_ARGS[0]}"
if [[ "${COMMAND}" == "run" ]]; then
  submit_run_command
fi

if [[ "${COMMAND}" == "dry-run" ]]; then
  require_spark_home
  exec "${SPARK_HOME}/bin/spark-submit" \
    ${SPARK_SUBMIT_ARGS[@]+"${SPARK_SUBMIT_ARGS[@]}"} \
    --class "${MAIN_CLASS}" \
    "${MAIN_JAR}" \
    "${PIPELINE_ARGS[@]}"
fi

exec java ${SPARK_SDP_JAVA_OPTS:-} -jar "${MAIN_JAR}" "${PIPELINE_ARGS[@]}"
