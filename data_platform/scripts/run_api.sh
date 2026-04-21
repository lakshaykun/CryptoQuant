#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_ACTIVATE="${ROOT_DIR}/.venv/bin/activate"
API_REQUIREMENTS_FILE="${ROOT_DIR}/api/requirements.txt"

if [[ -f "${VENV_ACTIVATE}" ]]; then
  # Use the project virtual environment when it is available.
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
fi

cd "${ROOT_DIR}"

ensure_api_dependencies() {
  local required_modules=(fastapi uvicorn pandas numpy requests)
  local missing_modules=()

  for module in "${required_modules[@]}"; do
    if ! python3 -c "import ${module}" >/dev/null 2>&1; then
      missing_modules+=("${module}")
    fi
  done

  if [[ ${#missing_modules[@]} -eq 0 ]]; then
    return 0
  fi

  if [[ ! -f "${API_REQUIREMENTS_FILE}" ]]; then
    echo "Missing API dependencies (${missing_modules[*]}), and requirements file not found at ${API_REQUIREMENTS_FILE}." >&2
    return 1
  fi

  echo "Installing missing API dependencies: ${missing_modules[*]}" >&2
  python3 -m pip install -r "${API_REQUIREMENTS_FILE}"
}

ensure_api_dependencies

if [[ "${API_RELOAD:-true}" == "true" ]]; then
  exec python3 -m uvicorn api.app:app --port 8000 --reload
fi

exec python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000
