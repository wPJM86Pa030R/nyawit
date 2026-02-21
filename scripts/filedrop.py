#!/usr/bin/env python3
"""
MediaFire FileDrop uploader with wildcard and directory expansion.

Examples:
  python filedrop.py "/data/releases/*.zip"
  python filedrop.py "/data/releases"
  python filedrop.py "/data/**" --dry-run
"""

from __future__ import annotations

import argparse
import glob
import mimetypes
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

try:
    import requests
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "Module 'requests' belum terpasang. Jalankan: pip install requests"
    ) from exc


API_BASE = "https://www.mediafire.com/api/1.5"
UPLOAD_TIMEOUT = 60 * 30
GLOB_CHARS = ("*", "?", "[")


class MediaFireError(RuntimeError):
    """MediaFire API error."""


def has_wildcard(path_text: str) -> bool:
    return any(char in path_text for char in GLOB_CHARS)


def read_key_file(key_file: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not key_file.exists():
        return values

    for line in key_file.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip().strip("\"' ")

    return values


def compact_config(config: Dict[str, str]) -> Dict[str, str]:
    return {key: value for key, value in config.items() if value}


def get_setting(config: Dict[str, str], key: str, default: str = "") -> str:
    env_value = os.getenv(key)
    if env_value is not None and env_value.strip():
        return env_value.strip()
    return config.get(key, default)


def collect_directory_files(directory: Path, recursive: bool) -> List[Path]:
    iterator: Iterable[Path] = directory.rglob("*") if recursive else directory.glob("*")
    files = [path.resolve() for path in iterator if path.is_file()]
    files.sort()
    return files


def is_subpath(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def collapse_directories(paths: Sequence[Path], recursive: bool) -> List[Path]:
    unique_dirs = sorted({path.resolve() for path in paths}, key=lambda item: len(item.parts))
    if not recursive:
        return unique_dirs

    collapsed: List[Path] = []
    for candidate in unique_dirs:
        if any(is_subpath(candidate, existing) for existing in collapsed):
            continue
        collapsed.append(candidate)
    return collapsed


def resolve_sources(
    source_inputs: Sequence[str], recursive: bool
) -> Tuple[List[Path], List[str]]:
    resolved_files: List[Path] = []
    missing_inputs: List[str] = []
    seen: Set[str] = set()

    for raw_input in source_inputs:
        source_text = raw_input.strip()
        if not source_text:
            continue

        expanded = os.path.expanduser(os.path.expandvars(source_text))
        path_candidates: List[Path] = []

        if has_wildcard(expanded):
            matches = sorted(glob.glob(expanded, recursive=True))
            if not matches:
                missing_inputs.append(source_text)
                continue
            path_candidates = [Path(item) for item in matches]
        else:
            path_candidates = [Path(expanded)]

        found_any = False
        matched_dirs: List[Path] = []
        for candidate in path_candidates:
            target = candidate.resolve()
            if not target.exists():
                continue

            found_any = True
            if target.is_file():
                key = str(target)
                if key not in seen:
                    seen.add(key)
                    resolved_files.append(target)
                continue

            if target.is_dir():
                matched_dirs.append(target)

        for directory in collapse_directories(matched_dirs, recursive=recursive):
            for file_path in collect_directory_files(directory, recursive=recursive):
                key = str(file_path)
                if key not in seen:
                    seen.add(key)
                    resolved_files.append(file_path)

        if not found_any:
            missing_inputs.append(source_text)

    resolved_files.sort()
    return resolved_files, missing_inputs


def parse_api_response(payload: Dict) -> Dict:
    response = payload.get("response")
    if not isinstance(response, dict):
        raise MediaFireError("Format response MediaFire tidak valid.")
    return response


def extract_error_message(response: Dict, fallback: str) -> str:
    details = response.get("message") or response.get("error") or response.get("result")
    if details:
        return str(details)
    return fallback


def get_filedrop_key(config: Dict[str, str]) -> str:
    filedrop_key = get_setting(
        config,
        "MEDIAFIRE_FILEDROP_KEY",
        get_setting(config, "FILEDROP_KEY"),
    )
    if not filedrop_key:
        raise MediaFireError(
            "Konfigurasi MediaFire kurang: MEDIAFIRE_FILEDROP_KEY (atau FILEDROP_KEY)."
        )
    return filedrop_key


def extract_uploaded_link(response_data: Dict, file_name: str) -> str:
    doupload = response_data.get("doupload")
    quickkey = ""

    if isinstance(doupload, dict):
        quickkey = str(
            doupload.get("quickkey") or doupload.get("key") or doupload.get("hash") or ""
        )
        direct_link = doupload.get("links")
        if isinstance(direct_link, dict):
            normal_download = direct_link.get("normal_download")
            if normal_download:
                return str(normal_download)

    if not quickkey:
        quickkey = str(response_data.get("quickkey") or response_data.get("key") or "")

    if quickkey:
        safe_name = file_name.replace(" ", "%20")
        return f"https://www.mediafire.com/file/{quickkey}/{safe_name}/file"

    return ""


def upload_file(file_path: Path, filedrop_key: str) -> str:
    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    payload = {
        "filedrop_key": filedrop_key,
        "response_format": "json",
        "action_on_duplicate": "keep",
    }

    with file_path.open("rb") as stream:
        response = requests.post(
            f"{API_BASE}/upload/simple.php",
            data=payload,
            files={"file": (file_path.name, stream, mime_type)},
            timeout=UPLOAD_TIMEOUT,
        )
    response.raise_for_status()
    api_payload = response.json()
    data = parse_api_response(api_payload)

    result = str(data.get("result", "")).lower()
    nested = data.get("doupload")
    if isinstance(nested, dict):
        nested_result = str(nested.get("result", "")).lower()
        if nested_result:
            result = nested_result

    if result != "success":
        msg = extract_error_message(data, f"Upload gagal untuk {file_path.name}.")
        raise MediaFireError(msg)

    return extract_uploaded_link(data, file_path.name)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Upload file/folder ke MediaFire via FileDrop key. "
            "Source mendukung file tunggal, folder, atau wildcard."
        )
    )
    parser.add_argument(
        "sources",
        nargs="+",
        help="Path file/folder/wildcard. Contoh: /data/*.mp4 atau /data/folder",
    )
    parser.add_argument(
        "--key-file",
        default="",
        help="Path file key (default: filedrop.key di folder script).",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Jika source folder, hanya upload file level atas (tanpa subfolder).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Tampilkan file yang akan diupload tanpa upload.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    if args.key_file:
        config = compact_config(read_key_file(Path(args.key_file).expanduser().resolve()))
    else:
        script_key_file = script_dir / "filedrop.key"
        root_key_file = script_dir.parent / "filedrop.key"
        script_config = compact_config(read_key_file(script_key_file))
        root_config = compact_config(read_key_file(root_key_file))
        config = dict(root_config)
        config.update(script_config)

    files, missing = resolve_sources(args.sources, recursive=not args.no_recursive)
    if missing:
        print("Warning: source tidak ditemukan:", file=sys.stderr)
        for item in missing:
            print(f"  - {item}", file=sys.stderr)

    if not files:
        print("Tidak ada file valid untuk diupload.", file=sys.stderr)
        return 1

    print(f"Ditemukan {len(files)} file untuk diproses.")
    if args.dry_run:
        for path in files:
            print(f"[DRY-RUN] {path}")
        return 0

    try:
        filedrop_key = get_filedrop_key(config)
    except Exception as exc:
        print(f"Gagal membaca filedrop key: {exc}", file=sys.stderr)
        return 1

    success_count = 0
    failed: List[Tuple[Path, str]] = []

    for file_path in files:
        try:
            link = upload_file(file_path, filedrop_key)
            if link:
                print(f"[OK] {file_path} -> {link}")
            else:
                print(f"[OK] {file_path}")
            success_count += 1
        except Exception as exc:
            failed.append((file_path, str(exc)))
            print(f"[ERR] {file_path} -> {exc}", file=sys.stderr)

    print(f"Selesai. Berhasil: {success_count}, Gagal: {len(failed)}")
    return 0 if not failed else 2


if __name__ == "__main__":
    sys.exit(main())
