#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

usage() {
  cat <<'EOF'
Usage:
  ./filedrop.sh <source> [source2 ...] [--no-recursive] [--dry-run]

Source supports:
  - Single file: /path/video.mp4
  - Directory : /path/folder
  - Wildcard  : /path/*.mp4
  - Recursive : /path/**/*.mkv

Examples:
  ./filedrop.sh "/home/runner/uploads/*"
  ./filedrop.sh "/home/runner/uploads/folder"
  ./filedrop.sh "/home/runner/uploads/**/*.mp4"
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

args=()
if [[ -n "${FILEDROP_KEY_FILE:-}" ]]; then
  args+=(--key-file "${FILEDROP_KEY_FILE}")
fi

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/filedrop.py" "${args[@]}" "$@"
