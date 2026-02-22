#!/usr/bin/env bash

set -euo pipefail

API_BASE="https://www.mediafire.com/api/1.5"
UPLOAD_SIMPLE_ENDPOINT="${API_BASE}/upload/simple.php"
POLL_UPLOAD_ENDPOINT="${API_BASE}/upload/poll_upload.php"

# Isi sekali di sini supaya tidak perlu paste key setiap kali menjalankan script.
# Boleh isi key mentah (contoh: abc123) atau URL filedrop lengkap.
DEFAULT_FILEDROP_KEY=""

usage() {
  cat <<'EOF'
MediaFire FileDrop uploader

Usage:
  filedrop.sh --file <path> [options]
  filedrop.sh <path> [options]

Options:
  -f, --file <path>           File yang akan di-upload.
  -k, --filedrop-key <key>    FileDrop key MediaFire.
      --path <path>           Path relatif/absolut tujuan di dalam FileDrop.
      --action <mode>         Aksi jika nama file sudah ada: skip|keep|replace.
      --poll-interval <sec>   Jeda polling status upload. Default: 2.
      --timeout <sec>         Batas waktu polling. Default: 300.
  -h, --help                  Tampilkan bantuan.

Environment:
  MEDIAFIRE_FILEDROP_KEY      Override key dari environment (opsional).
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || die "Command '$cmd' tidak ditemukan."
}

uri_encode() {
  jq -nr --arg v "$1" '$v|@uri'
}

file_sha256() {
  local f="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$f" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$f" | awk '{print $1}'
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$f" | awk '{print $NF}'
    return
  fi
  die "Butuh salah satu command ini untuk hitung SHA256: sha256sum, shasum, atau openssl."
}

normalize_filedrop_key() {
  local key="$1"
  if [[ "$key" == *"drop="* ]]; then
    key="${key#*drop=}"
    key="${key%%&*}"
  fi
  printf '%s' "$key"
}

get_json_value() {
  local json="$1"
  local jq_filter="$2"
  jq -r "$jq_filter // empty" <<<"$json"
}

FILE_PATH=""
FILEDROP_KEY="${MEDIAFIRE_FILEDROP_KEY:-$DEFAULT_FILEDROP_KEY}"
DEST_PATH=""
ACTION_ON_DUPLICATE=""
POLL_INTERVAL=2
TIMEOUT_SECONDS=300

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      FILE_PATH="$2"
      shift 2
      ;;
    -k|--filedrop-key)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      FILEDROP_KEY="$2"
      shift 2
      ;;
    --path)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      DEST_PATH="$2"
      shift 2
      ;;
    --action)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      ACTION_ON_DUPLICATE="$2"
      shift 2
      ;;
    --poll-interval)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      POLL_INTERVAL="$2"
      shift 2
      ;;
    --timeout)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      die "Opsi tidak dikenali: $1"
      ;;
    *)
      if [[ -z "$FILE_PATH" ]]; then
        FILE_PATH="$1"
      else
        die "Argumen tidak dikenali: $1"
      fi
      shift
      ;;
  esac
done

[[ -n "$FILE_PATH" ]] || {
  usage
  die "File belum ditentukan."
}

[[ -f "$FILE_PATH" ]] || die "File tidak ditemukan: $FILE_PATH"
[[ -s "$FILE_PATH" ]] || die "File kosong: $FILE_PATH"

FILEDROP_KEY="$(normalize_filedrop_key "$FILEDROP_KEY")"
[[ -n "$FILEDROP_KEY" ]] || die "FileDrop key wajib diisi via --filedrop-key atau MEDIAFIRE_FILEDROP_KEY."

case "$ACTION_ON_DUPLICATE" in
  ""|skip|keep|replace) ;;
  *)
    die "Nilai --action tidak valid: '$ACTION_ON_DUPLICATE'. Gunakan skip, keep, atau replace."
    ;;
esac

[[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || die "--poll-interval harus bilangan bulat >= 0."
[[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || die "--timeout harus bilangan bulat >= 0."

require_cmd curl
require_cmd jq

FILE_NAME="$(basename "$FILE_PATH")"
FILE_SIZE="$(wc -c <"$FILE_PATH" | tr -d '[:space:]')"
FILE_HASH="$(file_sha256 "$FILE_PATH")"

query="filedrop_key=$(uri_encode "$FILEDROP_KEY")&response_format=json"
if [[ -n "$DEST_PATH" ]]; then
  query="${query}&path=$(uri_encode "$DEST_PATH")"
fi
if [[ -n "$ACTION_ON_DUPLICATE" ]]; then
  query="${query}&action_on_duplicate=$(uri_encode "$ACTION_ON_DUPLICATE")"
fi

upload_url="${UPLOAD_SIMPLE_ENDPOINT}?${query}"

echo "Uploading '$FILE_NAME' ke MediaFire FileDrop..." >&2

upload_response="$(
  curl -sS --fail-with-body \
    -X POST \
    -H "Content-Type: application/octet-stream" \
    -H "x-filename: ${FILE_NAME}" \
    -H "x-filesize: ${FILE_SIZE}" \
    -H "x-filehash: ${FILE_HASH}" \
    --data-binary "@${FILE_PATH}" \
    "$upload_url"
)"

api_result="$(get_json_value "$upload_response" '.response.result // .result')"
if [[ "$api_result" != "Success" ]]; then
  api_error="$(get_json_value "$upload_response" '.response.error // .error')"
  api_message="$(get_json_value "$upload_response" '.response.message // .message')"
  die "Upload gagal. error=${api_error:-unknown} message='${api_message:-unknown error}'."
fi

upload_precheck_result="$(get_json_value "$upload_response" '.response.doupload.result // .doupload.result')"
if [[ -n "$upload_precheck_result" && "$upload_precheck_result" != "0" ]]; then
  die "Upload ditolak server (doupload.result=$upload_precheck_result)."
fi

upload_key="$(get_json_value "$upload_response" '.response.doupload.key // .doupload.key')"
[[ -n "$upload_key" ]] || die "Upload key tidak ditemukan di response."

echo "Upload key: $upload_key" >&2
echo "Menunggu verifikasi upload..." >&2

deadline=$((SECONDS + TIMEOUT_SECONDS))

quickkey=""
remote_filename=""
remote_size=""

while :; do
  poll_query="key=$(uri_encode "$upload_key")&response_format=json"
  poll_url="${POLL_UPLOAD_ENDPOINT}?${poll_query}"

  poll_response="$(
    curl -sS --fail-with-body "$poll_url"
  )"

  poll_api_result="$(get_json_value "$poll_response" '.response.result // .result')"
  if [[ "$poll_api_result" != "Success" ]]; then
    poll_error="$(get_json_value "$poll_response" '.response.error // .error')"
    poll_message="$(get_json_value "$poll_response" '.response.message // .message')"
    die "Polling gagal. error=${poll_error:-unknown} message='${poll_message:-unknown error}'."
  fi

  status="$(
    get_json_value "$poll_response" '
      .response.doupload.status //
      .doupload.status //
      .response.douploads[0].status //
      .douploads[0].status
    '
  )"
  description="$(
    get_json_value "$poll_response" '
      .response.doupload.description //
      .doupload.description //
      .response.douploads[0].description //
      .douploads[0].description
    '
  )"
  file_error="$(
    get_json_value "$poll_response" '
      .response.doupload.fileerror //
      .doupload.fileerror //
      .response.douploads[0].fileerror //
      .douploads[0].fileerror
    '
  )"
  quickkey="$(
    get_json_value "$poll_response" '
      .response.doupload.quickkey //
      .doupload.quickkey //
      .response.douploads[0].quickkey //
      .douploads[0].quickkey
    '
  )"
  remote_filename="$(
    get_json_value "$poll_response" '
      .response.doupload.filename //
      .doupload.filename //
      .response.douploads[0].filename //
      .douploads[0].filename
    '
  )"
  remote_size="$(
    get_json_value "$poll_response" '
      .response.doupload.size //
      .doupload.size //
      .response.douploads[0].size //
      .douploads[0].size
    '
  )"

  if [[ -n "$file_error" && "$file_error" != "0" ]]; then
    die "Upload error dari poll_upload: fileerror=$file_error status=$status description='${description:-unknown}'."
  fi

  if [[ "$status" == "99" ]]; then
    break
  fi

  if (( SECONDS >= deadline )); then
    die "Timeout menunggu upload selesai (status terakhir: ${status:-unknown} ${description:-})."
  fi

  echo "Status: ${status:-unknown} (${description:-no description})" >&2
  sleep "$POLL_INTERVAL"
done

echo "Upload selesai."
echo "upload_key=${upload_key}"
if [[ -n "$quickkey" ]]; then
  echo "quickkey=${quickkey}"
  echo "url=https://www.mediafire.com/file/${quickkey}"
fi
if [[ -n "$remote_filename" ]]; then
  echo "filename=${remote_filename}"
fi
if [[ -n "$remote_size" ]]; then
  echo "size=${remote_size}"
fi
