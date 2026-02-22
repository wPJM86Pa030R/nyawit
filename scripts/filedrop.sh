#!/usr/bin/env bash
set -euo pipefail

UPLOAD_SIMPLE_ENDPOINT="https://www.mediafire.com/api/upload/simple.php"
POLL_UPLOAD_ENDPOINT="https://www.mediafire.com/api/1.4/upload/poll_upload.php"
GET_LINKS_ENDPOINT="https://www.mediafire.com/api/1.4/file/get_links.php"
# Isi key FileDrop default di sini jika tidak ingin set via env/argumen.
DEFAULT_FILEDROP_KEY=""

usage() {
  cat <<'EOF'
MediaFire FileDrop uploader

Usage:
  filedrop.sh --file <path-or-glob> [--file <path-or-glob> ...] [options]
  filedrop.sh <path-or-glob> [more files/patterns ...] [options]

Contoh:
  FILEDROP_KEY=xxxx ./scripts/filedrop.sh "./dist/*.zip"
  FILEDROP_KEY=xxxx ./scripts/filedrop.sh dist/*.zip logs/*.txt
  FILEDROP_KEY=xxxx ./scripts/filedrop.sh --file "./build/*.apk" --action replace
  FILEDROP_KEY=xxxx ./scripts/filedrop.sh --recursive "/home/runner/uploads/"

Options:
  -f, --file <path-or-glob>  File/pattern yang di-upload (bisa diulang).
  -k, --filedrop-key <key>   FileDrop key MediaFire.
  -r, --recursive            Jika input folder, upload semua file di subfolder juga.
      --path <path>          Tujuan path di FileDrop (opsional).
      --action <mode>        Aksi duplikat: skip|keep|replace.
      --poll-interval <sec>  Jeda polling status upload. Default: 3.
      --timeout <sec>        Batas waktu polling per file. Default: 300.
  -h, --help                 Tampilkan bantuan.

Environment:
  DEFAULT_FILEDROP_KEY (di dalam file ini)
  FILEDROP_KEY
  MEDIAFIRE_FILEDROP_KEY
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

url_encode() {
  local s="$1"
  local out=""
  local i ch
  for (( i=0; i<${#s}; i++ )); do
    ch="${s:i:1}"
    case "$ch" in
      [a-zA-Z0-9.~_-]) out+="$ch" ;;
      *) printf -v out '%s%%%02X' "$out" "'$ch" ;;
    esac
  done
  printf '%s' "$out"
}

xml_first() {
  local tag="$1"
  sed -n "s:.*<${tag}>\\([^<]*\\)</${tag}>.*:\\1:p" | head -n1
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
  die "Butuh sha256sum atau shasum."
}

declare -a FILE_SPECS=()
declare -a UPLOAD_FILES=()
declare -A SEEN_FILES=()

FILEDROP_KEY="${FILEDROP_KEY:-${MEDIAFIRE_FILEDROP_KEY:-$DEFAULT_FILEDROP_KEY}}"
DEST_PATH=""
ACTION_ON_DUPLICATE=""
POLL_INTERVAL=3
TIMEOUT_SECONDS=300
RECURSIVE=0

add_upload_file() {
  local file="$1"
  if [[ -n "${SEEN_FILES["$file"]+x}" ]]; then
    return
  fi
  SEEN_FILES["$file"]=1
  UPLOAD_FILES+=("$file")
}

expand_spec() {
  local spec="$1"
  local matched=0
  local search_spec="$spec"
  local match=""

  if [[ -d "$search_spec" ]]; then
    if (( RECURSIVE == 1 )); then
      while IFS= read -r -d '' match; do
        [[ -f "$match" ]] || continue
        add_upload_file "$match"
        matched=1
      done < <(find "$search_spec" -type f -print0)

      if (( matched == 0 )); then
        return 1
      fi
      return 0
    fi

    search_spec="${search_spec%/}/*"
  fi

  if [[ -f "$search_spec" ]]; then
    add_upload_file "$search_spec"
    return 0
  fi

  while IFS= read -r match; do
    [[ -n "$match" ]] || continue
    [[ -f "$match" ]] || continue
    add_upload_file "$match"
    matched=1
  done < <(compgen -G "$search_spec" || true)

  if (( matched == 0 )); then
    return 1
  fi
}

upload_one_file() {
  local file="$1"
  local filename size hash
  local query upload_url
  local upload_resp upload_key
  local poll_resp status desc quickkey fileerror
  local links_resp normal_dl view_link
  local deadline

  [[ -f "$file" ]] || die "File tidak ditemukan: $file"
  [[ -s "$file" ]] || die "File kosong: $file"

  filename="$(basename "$file")"
  size="$(wc -c <"$file" | tr -d '[:space:]')"
  hash="$(file_sha256 "$file")"

  query="filedrop_key=$(url_encode "$FILEDROP_KEY")&response_format=xml"
  if [[ -n "$DEST_PATH" ]]; then
    query="${query}&path=$(url_encode "$DEST_PATH")"
  fi
  if [[ -n "$ACTION_ON_DUPLICATE" ]]; then
    query="${query}&action_on_duplicate=$(url_encode "$ACTION_ON_DUPLICATE")"
  fi
  upload_url="${UPLOAD_SIMPLE_ENDPOINT}?${query}"

  echo "=== Upload: $file ==="

  upload_resp="$(
    curl -sS --fail \
      -X POST "$upload_url" \
      -H "Content-Type: application/octet-stream" \
      -H "x-filename: ${filename}" \
      -H "x-filesize: ${size}" \
      -H "x-filehash: ${hash}" \
      --data-binary @"$file"
  )"

  upload_key="$(printf '%s' "$upload_resp" | xml_first key)"
  [[ -n "$upload_key" ]] || die "Gagal ambil upload key untuk '$file'."

  echo "upload_key=$upload_key"

  quickkey=""
  deadline=$((SECONDS + TIMEOUT_SECONDS))
  while :; do
    poll_resp="$(
      curl -sS --fail \
        "${POLL_UPLOAD_ENDPOINT}?key=$(url_encode "$upload_key")&response_format=xml"
    )"
    status="$(printf '%s' "$poll_resp" | xml_first status)"
    desc="$(printf '%s' "$poll_resp" | xml_first description)"
    quickkey="$(printf '%s' "$poll_resp" | xml_first quickkey)"
    fileerror="$(printf '%s' "$poll_resp" | xml_first fileerror)"

    echo "status=${status:-unknown} | ${desc:-}"

    if [[ -n "$fileerror" && "$fileerror" != "0" ]]; then
      die "Upload gagal untuk '$file' (fileerror=$fileerror)."
    fi

    if [[ "$status" == "99" && -n "$quickkey" ]]; then
      break
    fi

    if (( SECONDS >= deadline )); then
      die "Timeout menunggu upload selesai untuk '$file'."
    fi

    sleep "$POLL_INTERVAL"
  done

  links_resp="$(
    curl -sS --fail \
      "${GET_LINKS_ENDPOINT}?quick_key=$(url_encode "$quickkey")&response_format=xml"
  )"
  normal_dl="$(printf '%s' "$links_resp" | xml_first normal_download)"
  view_link="$(printf '%s' "$links_resp" | xml_first view)"

  echo "quickkey=$quickkey"
  [[ -n "$normal_dl" ]] && echo "normal_download=$normal_dl"
  [[ -n "$view_link" ]] && echo "view=$view_link"
  echo
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      FILE_SPECS+=("$2")
      shift 2
      ;;
    -k|--filedrop-key)
      [[ $# -ge 2 ]] || die "Nilai untuk $1 belum diisi."
      FILEDROP_KEY="$2"
      shift 2
      ;;
    -r|--recursive)
      RECURSIVE=1
      shift
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
      FILE_SPECS+=("$1")
      shift
      ;;
  esac
done

[[ ${#FILE_SPECS[@]} -gt 0 ]] || {
  usage
  die "File atau pattern belum ditentukan."
}

[[ -n "$FILEDROP_KEY" ]] || die "Isi DEFAULT_FILEDROP_KEY di scripts/filedrop.sh atau pakai --filedrop-key."

case "$ACTION_ON_DUPLICATE" in
  ""|skip|keep|replace) ;;
  *) die "--action harus: skip|keep|replace." ;;
esac

[[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || die "--poll-interval harus angka >= 0."
[[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || die "--timeout harus angka >= 0."

require_cmd curl
require_cmd sed
require_cmd head
require_cmd awk
require_cmd wc
require_cmd compgen
require_cmd find

for spec in "${FILE_SPECS[@]}"; do
  if ! expand_spec "$spec"; then
    die "Pattern tidak cocok file apa pun: $spec"
  fi
done

[[ ${#UPLOAD_FILES[@]} -gt 0 ]] || die "Tidak ada file yang bisa di-upload."

echo "Total file yang akan di-upload: ${#UPLOAD_FILES[@]}"
for upload_file in "${UPLOAD_FILES[@]}"; do
  upload_one_file "$upload_file"
done
