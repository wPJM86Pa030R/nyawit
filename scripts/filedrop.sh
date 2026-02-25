#!/usr/bin/env bash
set -euo pipefail

FILEDROP_KEY="ISI_DENGAN_TOKEN_LU_WOK"

API_VERSION="${API_VERSION:-1.3}"
API_BASE_URL="https://www.mediafire.com/api/${API_VERSION}"
CHECK_URL="${API_BASE_URL}/upload/check.php"
RESUMABLE_URL="${API_BASE_URL}/upload/resumable.php"
INSTANT_URL="${API_BASE_URL}/upload/instant.php"
POLL_URL="${API_BASE_URL}/upload/poll_upload.php"

REQUEST_RETRY_MAX="${REQUEST_RETRY_MAX:-5}"
CHUNK_RETRY_MAX="${CHUNK_RETRY_MAX:-5}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-2}"
MAX_RESUME_ROUNDS="${MAX_RESUME_ROUNDS:-8}"
PROGRESS_EVERY_UNITS="${PROGRESS_EVERY_UNITS:-25}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-3}"
MAX_POLL_ATTEMPTS="${MAX_POLL_ATTEMPTS:-30}"

CHECK_ALL_UNITS_READY=""
CHECK_UNIT_SIZE=""
CHECK_NUMBER_OF_UNITS=""
CHECK_DUPLICATE_QUICKKEY=""
CHECK_HASH_EXISTS=""
CHECK_MESSAGE=""
CHECK_ERROR_CODE=""
CHECK_BITMAP_COUNT=""
declare -a CHECK_BITMAP_WORDS=()

LAST_UPLOAD_KEY=""

log() {
  echo "$*" >&2
}

die() {
  log "Error: $*"
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Command '$1' tidak ditemukan."
}

url_encode() {
  jq -rn --arg v "$1" '$v|@uri'
}

calc_sha256() {
  local file="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi

  die "Perlu 'sha256sum' atau 'shasum' untuk menghitung hash."
}

resolve_single_file() {
  local pattern="$1"
  local -a matches=()
  local -a file_matches=()
  local candidate

  mapfile -t matches < <(compgen -G "$pattern" || true)

  if [[ ${#matches[@]} -eq 0 && -e "$pattern" ]]; then
    matches=("$pattern")
  fi

  for candidate in "${matches[@]}"; do
    if [[ -f "$candidate" ]]; then
      file_matches+=("$candidate")
    fi
  done

  if [[ ${#file_matches[@]} -eq 0 ]]; then
    die "Tidak ada file yang cocok dengan pola: $pattern"
  fi

  if [[ ${#file_matches[@]} -gt 1 ]]; then
    {
      echo "Error: Pola cocok ke lebih dari 1 file. Harus tepat 1 file."
      printf ' - %s\n' "${file_matches[@]}"
    } >&2
    exit 1
  fi

  printf '%s\n' "${file_matches[0]}"
}

parse_check_response() {
  local response="$1"

  CHECK_ALL_UNITS_READY="$(jq -r '(.response // .).resumable_upload.all_units_ready // empty' <<<"$response")"
  CHECK_UNIT_SIZE="$(jq -r '(.response // .).resumable_upload.unit_size // empty' <<<"$response")"
  CHECK_NUMBER_OF_UNITS="$(jq -r '(.response // .).resumable_upload.number_of_units // empty' <<<"$response")"
  CHECK_DUPLICATE_QUICKKEY="$(jq -r '(.response // .).duplicate_quickkey // empty' <<<"$response")"
  CHECK_HASH_EXISTS="$(jq -r '(.response // .).hash_exists // empty' <<<"$response")"
  CHECK_MESSAGE="$(jq -r '(.response // .).message // empty' <<<"$response")"
  CHECK_ERROR_CODE="$(jq -r '(.response // .).error // empty' <<<"$response")"
  CHECK_BITMAP_COUNT="$(jq -r '(.response // .).resumable_upload.bitmap.count // empty' <<<"$response")"

  mapfile -t CHECK_BITMAP_WORDS < <(jq -r '(.response // .).resumable_upload.bitmap.words[]? // empty' <<<"$response")
}

is_unit_uploaded() {
  local unit_id="$1"
  local word_index bit_index word

  word_index=$((unit_id / 16))
  bit_index=$((unit_id % 16))

  if (( word_index >= ${#CHECK_BITMAP_WORDS[@]} )); then
    return 1
  fi

  word="${CHECK_BITMAP_WORDS[$word_index]}"
  if [[ ! "$word" =~ ^[0-9]+$ ]]; then
    return 1
  fi

  if (( ((word >> bit_index) & 1) == 1 )); then
    return 0
  fi

  return 1
}

sleep_backoff() {
  local attempt="$1"
  sleep "$((RETRY_DELAY_SECONDS * attempt))"
}

upload_check_with_retry() {
  local filename="$1"
  local filesize="$2"
  local filehash="$3"
  local attempt response result message error_code

  for ((attempt = 1; attempt <= REQUEST_RETRY_MAX; attempt++)); do
    if ! response="$(
      curl -sS \
        -X POST \
        --data-urlencode "filename=$filename" \
        --data-urlencode "size=$filesize" \
        --data-urlencode "hash=$filehash" \
        --data-urlencode "filedrop_key=$FILEDROP_KEY" \
        --data-urlencode "resumable=yes" \
        --data-urlencode "response_format=json" \
        "$CHECK_URL"
    )"; then
      log "upload/check gagal (curl), percobaan $attempt/$REQUEST_RETRY_MAX."
      if (( attempt < REQUEST_RETRY_MAX )); then
        sleep_backoff "$attempt"
      fi
      continue
    fi

    if ! jq -e . >/dev/null 2>&1 <<<"$response"; then
      log "upload/check respons bukan JSON valid, percobaan $attempt/$REQUEST_RETRY_MAX."
      if (( attempt < REQUEST_RETRY_MAX )); then
        sleep_backoff "$attempt"
      fi
      continue
    fi

    result="$(jq -r '(.response // .).result // empty' <<<"$response")"
    if [[ "$result" == "Success" ]]; then
      printf '%s\n' "$response"
      return 0
    fi

    message="$(jq -r '(.response // .).message // empty' <<<"$response")"
    error_code="$(jq -r '(.response // .).error // empty' <<<"$response")"
    log "upload/check belum sukses. result=${result:-<kosong>} error=${error_code:-<kosong>} message=${message:-<kosong>} percobaan $attempt/$REQUEST_RETRY_MAX."
    if (( attempt < REQUEST_RETRY_MAX )); then
      sleep_backoff "$attempt"
    fi
  done

  return 1
}

upload_resumable_unit_with_retry() {
  local chunk_file="$1"
  local filename="$2"
  local filesize="$3"
  local filehash="$4"
  local unit_id="$5"
  local unit_bytes="$6"
  local unit_hash="$7"
  local encoded_filedrop_key="$8"
  local attempt response result code message upload_key
  local url="${RESUMABLE_URL}?filedrop_key=${encoded_filedrop_key}&response_format=json"

  for ((attempt = 1; attempt <= CHUNK_RETRY_MAX; attempt++)); do
    if ! response="$(
      curl -sS \
        -X POST \
        -H "x-filesize: $filesize" \
        -H "x-filehash: $filehash" \
        -H "x-unit-hash: $unit_hash" \
        -H "x-unit-id: $unit_id" \
        -H "x-unit-size: $unit_bytes" \
        -H "x-filename: $filename" \
        -F "file=@$chunk_file;filename=chunk;type=application/octet-stream" \
        "$url"
    )"; then
      log "Unit $unit_id gagal upload (curl), percobaan $attempt/$CHUNK_RETRY_MAX."
      if (( attempt < CHUNK_RETRY_MAX )); then
        sleep_backoff "$attempt"
      fi
      continue
    fi

    if ! jq -e . >/dev/null 2>&1 <<<"$response"; then
      log "Unit $unit_id respons bukan JSON valid, percobaan $attempt/$CHUNK_RETRY_MAX."
      if (( attempt < CHUNK_RETRY_MAX )); then
        sleep_backoff "$attempt"
      fi
      continue
    fi

    result="$(jq -r '(.response // .).result // empty' <<<"$response")"
    code="$(jq -r '(.response // .).doupload.result // empty' <<<"$response")"
    message="$(jq -r '(.response // .).doupload.description // (.response // .).message // empty' <<<"$response")"

    if [[ "$result" == "Success" && ( "$code" == "0" || -z "$code" ) ]]; then
      upload_key="$(jq -r '(.response // .).doupload.key // empty' <<<"$response")"
      if [[ -n "$upload_key" ]]; then
        LAST_UPLOAD_KEY="$upload_key"
      fi
      return 0
    fi

    log "Unit $unit_id gagal. result=${result:-<kosong>} code=${code:-<kosong>} message=${message:-<kosong>} percobaan $attempt/$CHUNK_RETRY_MAX."
    if (( attempt < CHUNK_RETRY_MAX )); then
      sleep_backoff "$attempt"
    fi
  done

  return 1
}

upload_instant_with_retry() {
  local filename="$1"
  local filesize="$2"
  local filehash="$3"
  local attempt response result message quickkey

  for ((attempt = 1; attempt <= REQUEST_RETRY_MAX; attempt++)); do
    if ! response="$(
      curl -sS \
        -X POST \
        --data-urlencode "filename=$filename" \
        --data-urlencode "size=$filesize" \
        --data-urlencode "hash=$filehash" \
        --data-urlencode "filedrop_key=$FILEDROP_KEY" \
        --data-urlencode "response_format=json" \
        "$INSTANT_URL"
    )"; then
      log "upload/instant gagal (curl), percobaan $attempt/$REQUEST_RETRY_MAX."
      if (( attempt < REQUEST_RETRY_MAX )); then
        sleep_backoff "$attempt"
      fi
      continue
    fi

    if ! jq -e . >/dev/null 2>&1 <<<"$response"; then
      log "upload/instant respons bukan JSON valid, percobaan $attempt/$REQUEST_RETRY_MAX."
      if (( attempt < REQUEST_RETRY_MAX )); then
        sleep_backoff "$attempt"
      fi
      continue
    fi

    result="$(jq -r '(.response // .).result // empty' <<<"$response")"
    message="$(jq -r '(.response // .).message // empty' <<<"$response")"
    quickkey="$(jq -r '(.response // .).quickkey // (.response // .).duplicate_quickkey // (.response // .).doupload.quickkey // empty' <<<"$response")"

    if [[ "$result" == "Success" && -n "$quickkey" ]]; then
      printf '%s\n' "$quickkey"
      return 0
    fi

    log "upload/instant belum memberi quickkey. result=${result:-<kosong>} message=${message:-<kosong>} percobaan $attempt/$REQUEST_RETRY_MAX."
    if (( attempt < REQUEST_RETRY_MAX )); then
      sleep_backoff "$attempt"
    fi
  done

  return 1
}

poll_quickkey_fallback() {
  local upload_key="$1"
  local attempt response result status quickkey fileerror message
  local poll_url="${POLL_URL}?key=$(url_encode "$upload_key")&response_format=json"

  for ((attempt = 1; attempt <= MAX_POLL_ATTEMPTS; attempt++)); do
    if ! response="$(curl -sS "$poll_url")"; then
      sleep "$POLL_INTERVAL_SECONDS"
      continue
    fi

    if ! jq -e . >/dev/null 2>&1 <<<"$response"; then
      sleep "$POLL_INTERVAL_SECONDS"
      continue
    fi

    result="$(jq -r '(.response // .).result // empty' <<<"$response")"
    message="$(jq -r '(.response // .).message // empty' <<<"$response")"
    status="$(jq -r '(.response // .).doupload.status // empty' <<<"$response")"
    quickkey="$(jq -r '(.response // .).doupload.quickkey // empty' <<<"$response")"
    fileerror="$(jq -r '(.response // .).doupload.fileerror // empty' <<<"$response")"

    if [[ -n "$quickkey" ]]; then
      printf '%s\n' "$quickkey"
      return 0
    fi

    if [[ "$result" != "Success" ]]; then
      log "poll fallback berhenti: result=${result:-<kosong>} message=${message:-<kosong>}."
      return 1
    fi

    if [[ -n "$fileerror" && "$fileerror" != "0" ]]; then
      log "poll fallback berhenti: fileerror=$fileerror."
      return 1
    fi

    if [[ "$status" == "99" ]]; then
      return 1
    fi

    sleep "$POLL_INTERVAL_SECONDS"
  done

  return 1
}

if [[ $# -lt 1 ]]; then
  cat >&2 <<'EOF'
Usage:
  bash scripts/filedrop.sh "<file-atau-wildcard>"

Contoh:
  bash scripts/filedrop.sh "*1.txt"
  bash scripts/filedrop.sh "./dist/*.zip"
EOF
  exit 1
fi

if [[ $# -gt 1 ]]; then
  {
    echo "Error: Input cocok ke lebih dari 1 file. Upload hanya boleh 1 file."
    printf ' - %s\n' "$@"
  } >&2
  exit 1
fi

require_cmd curl
require_cmd jq
require_cmd dd
require_cmd wc
require_cmd mktemp

if [[ -z "$FILEDROP_KEY" || "$FILEDROP_KEY" == "GANTI_DENGAN_FILEDROP_KEY" ]]; then
  die "Isi FILEDROP_KEY di dalam scripts/filedrop.sh terlebih dahulu."
fi

input_pattern="$1"
target_file="$(resolve_single_file "$input_pattern")"
filename="$(basename "$target_file")"
filesize="$(wc -c < "$target_file" | tr -d '[:space:]')"
filehash="$(calc_sha256 "$target_file")"
encoded_filedrop_key="$(url_encode "$FILEDROP_KEY")"

log "Target file: $target_file"
log "Ukuran     : $filesize bytes"
log "SHA256     : $filehash"
log "API        : $API_BASE_URL"

check_response="$(upload_check_with_retry "$filename" "$filesize" "$filehash")" || die "upload/check gagal setelah retry."
parse_check_response "$check_response"

if [[ "$CHECK_HASH_EXISTS" == "yes" && -n "$CHECK_DUPLICATE_QUICKKEY" ]]; then
  echo "Upload berhasil (sudah ada di MediaFire)."
  echo "File    : $target_file"
  echo "Quickkey: $CHECK_DUPLICATE_QUICKKEY"
  echo "URL     : https://www.mediafire.com/file/$CHECK_DUPLICATE_QUICKKEY"
  exit 0
fi

if [[ ! "$CHECK_UNIT_SIZE" =~ ^[0-9]+$ || "$CHECK_UNIT_SIZE" -le 0 ]]; then
  die "unit_size dari upload/check tidak valid: '${CHECK_UNIT_SIZE:-<kosong>}'."
fi

if [[ ! "$CHECK_NUMBER_OF_UNITS" =~ ^[0-9]+$ || "$CHECK_NUMBER_OF_UNITS" -le 0 ]]; then
  die "number_of_units dari upload/check tidak valid: '${CHECK_NUMBER_OF_UNITS:-<kosong>}'."
fi

unit_size="$CHECK_UNIT_SIZE"
number_of_units="$CHECK_NUMBER_OF_UNITS"

log "Resumable unit size : $unit_size bytes"
log "Total unit          : $number_of_units"

tmp_dir="$(mktemp -d)"
chunk_file="$tmp_dir/chunk.bin"

cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

round=1
while [[ "$CHECK_ALL_UNITS_READY" != "yes" ]]; do
  if (( round > MAX_RESUME_ROUNDS )); then
    die "Semua unit belum siap setelah $MAX_RESUME_ROUNDS ronde upload."
  fi

  log "Ronde upload ke-$round ..."
  uploaded_in_round=0

  for ((unit_id = 0; unit_id < number_of_units; unit_id++)); do
    if is_unit_uploaded "$unit_id"; then
      continue
    fi

    if ! dd if="$target_file" of="$chunk_file" bs="$unit_size" skip="$unit_id" count=1 status=none; then
      die "Gagal membaca chunk unit $unit_id."
    fi

    unit_bytes="$(wc -c < "$chunk_file" | tr -d '[:space:]')"
    if [[ ! "$unit_bytes" =~ ^[0-9]+$ || "$unit_bytes" -le 0 ]]; then
      die "Ukuran chunk unit $unit_id tidak valid: '${unit_bytes:-<kosong>}'."
    fi

    unit_hash="$(calc_sha256 "$chunk_file")"

    if ! upload_resumable_unit_with_retry "$chunk_file" "$filename" "$filesize" "$filehash" "$unit_id" "$unit_bytes" "$unit_hash" "$encoded_filedrop_key"; then
      die "Gagal upload unit $unit_id setelah retry."
    fi

    uploaded_in_round=$((uploaded_in_round + 1))
    if (( uploaded_in_round == 1 || uploaded_in_round % PROGRESS_EVERY_UNITS == 0 || unit_id + 1 == number_of_units )); then
      percent=$((((unit_id + 1) * 100) / number_of_units))
      log "Progress unit: $((unit_id + 1))/$number_of_units (${percent}%)"
    fi
  done

  check_response="$(upload_check_with_retry "$filename" "$filesize" "$filehash")" || die "upload/check ulang gagal."
  parse_check_response "$check_response"

  if [[ "$CHECK_ALL_UNITS_READY" != "yes" ]]; then
    remaining=0
    for ((unit_id = 0; unit_id < number_of_units; unit_id++)); do
      if ! is_unit_uploaded "$unit_id"; then
        remaining=$((remaining + 1))
      fi
    done
    log "Masih ada $remaining unit belum siap. Lanjut ronde berikutnya."
  fi

  round=$((round + 1))
done

quickkey=""
if quickkey="$(upload_instant_with_retry "$filename" "$filesize" "$filehash")"; then
  :
fi

if [[ -z "$quickkey" && -n "$CHECK_DUPLICATE_QUICKKEY" ]]; then
  quickkey="$CHECK_DUPLICATE_QUICKKEY"
fi

if [[ -z "$quickkey" && -n "$LAST_UPLOAD_KEY" ]]; then
  log "Mencoba poll fallback menggunakan upload key ..."
  quickkey="$(poll_quickkey_fallback "$LAST_UPLOAD_KEY" || true)"
fi

if [[ -z "$quickkey" ]]; then
  die "Upload chunk selesai, tapi quickkey tidak ditemukan. Coba jalankan lagi atau set API_VERSION=1.3."
fi

echo "Upload berhasil."
echo "File    : $target_file"
echo "Quickkey: $quickkey"
echo "URL     : https://www.mediafire.com/file/$quickkey"
