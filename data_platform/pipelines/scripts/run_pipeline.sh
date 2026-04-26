#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PARENT_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
SPARK_REQUIREMENTS_FILE="${ROOT_DIR}/docker/spark/requirements.txt"
LOG_DIR="${ROOT_DIR}/logs/sentiment_pipeline"
PID_DIR="${LOG_DIR}/pids"

VENV_CANDIDATES=(
  "${ROOT_DIR}/.venv/bin/activate"
  "${ROOT_DIR}/venv/bin/activate"
  "${PARENT_DIR}/.venv/bin/activate"
  "${PARENT_DIR}/venv/bin/activate"
)

KAFKA_TOPICS=(
  "crypto_prices"
  "btc_reddit"
  "btc_yt"
  "btc_news"
  "btc_telegram"
)

STAGE_LABELS=(
  "cryptobert_api"
  "kafka_to_delta"
  "silver"
  "gold"
  "reddit"
  "youtube"
  "news"
  "telegram"
)

SPARK_APP_NAMES=(
  "btc-sentiment-platform-bronze"
  "btc-sentiment-platform-silver"
  "btc-sentiment-platform-gold"
)

REQUIRED_FILES=(
  "${ROOT_DIR}/docker-compose.yml"
  "${ROOT_DIR}/configs/spark.yaml"
  "${ROOT_DIR}/configs/kafka.yaml"
  "${ROOT_DIR}/scripts/run_api.sh"
  "${ROOT_DIR}/pipelines/jobs/streaming/bronze/kafka_to_delta.py"
  "${ROOT_DIR}/pipelines/jobs/streaming/silver/clean_merge_stream.py"
  "${ROOT_DIR}/pipelines/jobs/streaming/gold/sentiment_enrichment.py"
  "${ROOT_DIR}/pipelines/ingestion/streaming/sentiment/reddit_stream_job.py"
  "${ROOT_DIR}/pipelines/ingestion/streaming/sentiment/youtube_stream_job.py"
  "${ROOT_DIR}/pipelines/ingestion/streaming/sentiment/news_stream_job.py"
  "${ROOT_DIR}/pipelines/ingestion/streaming/sentiment/telegram_stream_job.py"
)

activate_venv_if_present() {
  local activate_path
  for activate_path in "${VENV_CANDIDATES[@]}"; do
    if [[ -f "${activate_path}" ]]; then
      # shellcheck disable=SC1090
      source "${activate_path}"
      echo "Activated virtual environment: ${activate_path}"
      return 0
    fi
  done

  echo "Warning: no project virtual environment found; using system Python."
}

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Error: required command '${cmd}' is not available in PATH." >&2
    exit 1
  fi
}

check_python_compatibility() {
  python3 - <<'PY'
import sys

major, minor = sys.version_info[:2]
if (major, minor) >= (3, 13):
  print(
    f"Unsupported Python {major}.{minor} for this Spark pipeline.",
    file=sys.stderr,
  )
  print(
    "Use Python 3.10-3.12 (recommended 3.11) in a project .venv and rerun.",
    file=sys.stderr,
  )
  raise SystemExit(1)
PY
}

check_required_files() {
  local missing=0
  for file in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "${file}" ]]; then
      echo "Missing required file: ${file}" >&2
      missing=1
    fi
  done

  if [[ "${missing}" -ne 0 ]]; then
    return 1
  fi
}

check_python_syntax() {
  local failed=0
  for file in "${REQUIRED_FILES[@]}"; do
    if [[ "${file}" == *.py ]]; then
      if ! python3 -m py_compile "${file}"; then
        echo "Python syntax check failed: ${file}" >&2
        failed=1
      fi
    fi
  done

  if [[ "${failed}" -ne 0 ]]; then
    return 1
  fi
}

check_runtime_imports() {
  PYTHONPATH="${ROOT_DIR}" python3 - <<'PY'
import importlib
import sys

required_modules = [
    "pyspark",
    "delta",
    "kafka",
    "requests",
    "feedparser",
    "bs4",
    "curl_cffi",
    "googleapiclient",
    "telethon",
    "pipelines.jobs.streaming.bronze.kafka_to_delta",
    "pipelines.jobs.streaming.silver.clean_merge_stream",
    "pipelines.jobs.streaming.gold.sentiment_enrichment",
    "pipelines.ingestion.streaming.sentiment.reddit_stream_job",
    "pipelines.ingestion.streaming.sentiment.youtube_stream_job",
    "pipelines.ingestion.streaming.sentiment.news_stream_job",
    "pipelines.ingestion.streaming.sentiment.telegram_stream_job",
]

missing = []
for module_name in required_modules:
    try:
        importlib.import_module(module_name)
    except Exception as exc:
        missing.append((module_name, repr(exc)))

if missing:
    for module_name, error_text in missing:
        print(f"Import check failed for {module_name}: {error_text}", file=sys.stderr)
    raise SystemExit(1)
PY
}

ensure_runtime_dependencies() {
  local required_modules=(
    "pyspark"
    "delta"
    "kafka"
    "requests"
    "feedparser"
    "bs4"
    "curl_cffi"
    "googleapiclient"
    "telethon"
  )
  local missing_modules=()
  local module

  for module in "${required_modules[@]}"; do
    if ! python3 -c "import ${module}" >/dev/null 2>&1; then
      missing_modules+=("${module}")
    fi
  done

  if [[ ${#missing_modules[@]} -eq 0 ]]; then
    return 0
  fi

  if [[ ! -f "${SPARK_REQUIREMENTS_FILE}" ]]; then
    echo "Missing Python modules (${missing_modules[*]}) and requirements file not found: ${SPARK_REQUIREMENTS_FILE}" >&2
    return 1
  fi

  echo "Installing runtime dependencies for sentiment pipeline: ${missing_modules[*]}"
  python3 -m pip install -r "${SPARK_REQUIREMENTS_FILE}"
}

check_compose_config() {
  docker compose -f "${COMPOSE_FILE}" config >/dev/null
}

wait_for_kafka() {
  local timeout_seconds=90
  local waited=0

  while (( waited < timeout_seconds )); do
    if docker inspect -f '{{.State.Running}}' crypto-kafka 2>/dev/null | grep -q '^true$'; then
      echo "Kafka container is running."
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done

  echo "Timed out waiting for Kafka container to become ready." >&2
  return 1
}

ensure_kafka_topics() {
  echo "Ensuring Kafka topics exist..."

  local bootstrap="localhost:9092"
  local retries=15
  local delay_seconds=2
  local attempt=1

  while (( attempt <= retries )); do
    if docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "${bootstrap}" >/dev/null 2>&1; then
      break
    fi
    sleep "${delay_seconds}"
    attempt=$((attempt + 1))
  done

  if (( attempt > retries )); then
    echo "Kafka admin endpoint did not become ready on ${bootstrap}." >&2
    return 1
  fi

  local topic
  for topic in "${KAFKA_TOPICS[@]}"; do
    docker exec crypto-kafka /opt/kafka/bin/kafka-topics.sh \
      --create \
      --if-not-exists \
      --topic "${topic}" \
      --bootstrap-server "${bootstrap}" \
      --partitions 3 \
      --replication-factor 1 >/dev/null
  done
}

is_valid_stage() {
  local stage="$1"
  local label
  for label in "${STAGE_LABELS[@]}"; do
    if [[ "${label}" == "${stage}" ]]; then
      return 0
    fi
  done
  return 1
}

stage_command() {
  local label="$1"
  case "${label}" in
    cryptobert_api)
      echo "ENV=host API_RELOAD=false PYTHONPATH=\"${ROOT_DIR}\" ./scripts/run_api.sh"
      ;;
    kafka_to_delta)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.jobs.streaming.bronze.kafka_to_delta"
      ;;
    silver)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.jobs.streaming.silver.clean_merge_stream"
      ;;
    gold)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.jobs.streaming.gold.sentiment_enrichment"
      ;;
    reddit)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.ingestion.streaming.sentiment.reddit_stream_job"
      ;;
    youtube)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.ingestion.streaming.sentiment.youtube_stream_job"
      ;;
    news)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.ingestion.streaming.sentiment.news_stream_job"
      ;;
    telegram)
      echo "ENV=host PYTHONPATH=\"${ROOT_DIR}\" python3 -m pipelines.ingestion.streaming.sentiment.telegram_stream_job"
      ;;
    *)
      echo ""
      ;;
  esac
}

start_stage_command() {
  local label="$1"
  local command="$2"
  local logfile="${LOG_DIR}/${label}.log"
  local pidfile="${PID_DIR}/${label}.pid"

  if [[ -f "${pidfile}" ]]; then
    local existing_pid
    existing_pid="$(cat "${pidfile}")"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      echo "Stage '${label}' is already running with PID ${existing_pid}."
      return 0
    fi
  fi

  echo "Starting stage: ${label}"
  nohup bash -c "cd \"${ROOT_DIR}\" && ${command}" >"${logfile}" 2>&1 &
  local pid=$!
  echo "${pid}" >"${pidfile}"

  sleep 5
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "Stage '${label}' failed to start. Last log lines:" >&2
    tail -n 40 "${logfile}" >&2 || true
    return 1
  fi

  echo "Stage '${label}' started with PID ${pid}."
}

start_individual_process() {
  local label="$1"
  if ! is_valid_stage "${label}"; then
    echo "Unknown stage: ${label}" >&2
    usage
    exit 1
  fi

  local command
  command="$(stage_command "${label}")"
  if [[ -z "${command}" ]]; then
    echo "No command mapped for stage '${label}'." >&2
    exit 1
  fi

  start_stage_command "${label}" "${command}"
}

start_combined_process() {
  local label
  for label in "${STAGE_LABELS[@]}"; do
    start_individual_process "${label}"
  done
}

stop_stage() {
  local label="$1"
  local pidfile="${PID_DIR}/${label}.pid"

  if [[ ! -f "${pidfile}" ]]; then
    return 0
  fi

  local pid
  pid="$(cat "${pidfile}")"

  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "Stopping stage '${label}' (PID ${pid})"
    kill "${pid}" || true
    sleep 2
    if kill -0 "${pid}" 2>/dev/null; then
      kill -9 "${pid}" || true
    fi
  fi

  rm -f "${pidfile}"
}

stop_individual_process() {
  local label="$1"
  if ! is_valid_stage "${label}"; then
    echo "Unknown stage: ${label}" >&2
    usage
    exit 1
  fi
  stop_stage "${label}"
}

stop_combined_process() {
  local i
  for (( i=${#STAGE_LABELS[@]}-1; i>=0; i-- )); do
    stop_stage "${STAGE_LABELS[$i]}"
  done
}

cleanup_orphan_stage_processes() {
  local patterns=(
    "uvicorn api.app:app"
    "pipelines.jobs.streaming.bronze.kafka_to_delta"
    "pipelines.jobs.streaming.silver.clean_merge_stream"
    "pipelines.jobs.streaming.gold.sentiment_enrichment"
    "pipelines.ingestion.streaming.sentiment.reddit_stream_job"
    "pipelines.ingestion.streaming.sentiment.youtube_stream_job"
    "pipelines.ingestion.streaming.sentiment.news_stream_job"
    "pipelines.ingestion.streaming.sentiment.telegram_stream_job"
  )

  local found=0
  local pattern
  for pattern in "${patterns[@]}"; do
    if pgrep -f "${pattern}" >/dev/null 2>&1; then
      found=1
      pkill -f "${pattern}" || true
    fi
  done

  local app_name
  for app_name in "${SPARK_APP_NAMES[@]}"; do
    if pgrep -f "spark.app.name=${app_name}" >/dev/null 2>&1; then
      found=1
      pkill -f "spark.app.name=${app_name}" || true
    fi
  done

  if [[ "${found}" -eq 1 ]]; then
    echo "Cleaned orphan stage processes."
  fi
}

status_pipeline() {
  local target="${1:-all}"
  echo "Pipeline status:"

  local label
  for label in "${STAGE_LABELS[@]}"; do
    if [[ "${target}" != "all" && "${target}" != "${label}" ]]; then
      continue
    fi

    local pidfile="${PID_DIR}/${label}.pid"
    if [[ -f "${pidfile}" ]]; then
      local pid
      pid="$(cat "${pidfile}")"
      if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        echo "  ${label}: running (PID ${pid})"
      else
        echo "  ${label}: not running (stale pid file)"
      fi
    else
      echo "  ${label}: not running"
    fi
  done

  if docker inspect -f '{{.State.Running}}' crypto-kafka 2>/dev/null | grep -q '^true$'; then
    echo "  kafka: running (container crypto-kafka)"
  else
    echo "  kafka: not running"
  fi
}

check_pipeline() {
  echo "Running pipeline checks..."
  require_command python3
  check_python_compatibility
  check_required_files
  check_python_syntax
  check_runtime_imports

  if command -v docker >/dev/null 2>&1; then
    check_compose_config
  else
    echo "Warning: docker is not installed, skipped docker-compose config check."
  fi

  echo "All checks passed."
}

prepare_runtime() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  activate_venv_if_present

  require_command python3
  require_command docker
  check_python_compatibility
  check_required_files
  check_python_syntax
  check_compose_config
  ensure_runtime_dependencies

  echo "Building Docker image for sentiment pipeline dependencies..."
  docker compose -f "${COMPOSE_FILE}" build spark

  echo "Starting Kafka services..."
  docker compose -f "${COMPOSE_FILE}" up -d kafka topic-init
  wait_for_kafka
  ensure_kafka_topics
}

start_pipeline() {
  local target="${1:-all}"
  prepare_runtime

  if [[ "${target}" == "all" ]]; then
    start_combined_process
  else
    start_individual_process "${target}"
  fi

  echo "Sentiment pipeline start completed for target: ${target}"
  echo "Logs directory: ${LOG_DIR}"
  status_pipeline "${target}"
}

stop_pipeline() {
  local target="${1:-all}"

  if [[ "${target}" == "all" ]]; then
    stop_combined_process
  else
    stop_individual_process "${target}"
  fi

  cleanup_orphan_stage_processes

  if [[ "${target}" == "all" ]]; then
    echo "Stopping Kafka services..."
    docker compose -f "${COMPOSE_FILE}" stop topic-init kafka >/dev/null 2>&1 || true
  fi

  echo "Sentiment pipeline stop completed for target: ${target}"
}

clear_delta_logs() {
  local delta_root="${ROOT_DIR}/delta"

  if [[ ! -d "${delta_root}" ]]; then
    echo "Delta directory not found: ${delta_root}"
    return 0
  fi

  echo "Stopping running stages before clearing Delta logs..."
  stop_pipeline all

  if [[ "${delta_root}" == "/" || -z "${delta_root}" ]]; then
    echo "Refusing to clear unsafe delta path: ${delta_root}" >&2
    return 1
  fi

  echo "Clearing all files and directories inside: ${delta_root}"
  find "${delta_root}" -mindepth 1 -exec rm -rf {} +

  echo "Delta directory cleared: ${delta_root}"
}

clear_data() {
  clear_delta_logs
}

usage() {
  cat <<'USAGE'
Usage:
  run_pipeline.sh start [all|stage]
  run_pipeline.sh stop [all|stage]
  run_pipeline.sh restart [all|stage]
  run_pipeline.sh status [all|stage]
  run_pipeline.sh check
  run_pipeline.sh clear-delta-logs
  run_pipeline.sh clear-data

Stages:
  cryptobert_api | kafka_to_delta | silver | gold | reddit | youtube | news | telegram

Examples:
  run_pipeline.sh start all
  run_pipeline.sh start silver
  run_pipeline.sh stop telegram
  run_pipeline.sh restart all
  run_pipeline.sh status gold
USAGE
}

main() {
  local action="${1:-start}"
  local target="${2:-all}"

  case "${action}" in
    start)
      start_pipeline "${target}"
      ;;
    stop)
      stop_pipeline "${target}"
      ;;
    restart)
      stop_pipeline "${target}"
      start_pipeline "${target}"
      ;;
    status)
      status_pipeline "${target}"
      ;;
    check)
      activate_venv_if_present
      check_pipeline
      ;;
    clear-delta-logs)
      clear_delta_logs
      ;;
    clear-data)
      clear_data
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      echo "Unknown action: ${action}" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
