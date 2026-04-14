#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_ACTIVATE="${ROOT_DIR}/.venv/bin/activate"

if [[ -f "${VENV_ACTIVATE}" ]]; then
  # Use the project virtual environment when it is available.
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
fi

cd "${ROOT_DIR}"
exec python3 -m uvicorn api.app:app --port 8000 --reload
