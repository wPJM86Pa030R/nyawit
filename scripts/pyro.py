import os
import re
import time
import asyncio
import shlex
import shutil
import datetime
import json
import glob
import tempfile
import secrets
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageIdInvalid
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_USER_ID_RAW = os.getenv("OWNER_USER_ID", "").strip()
ARIA2_BIN = os.getenv("ARIA2_BIN", "aria2c")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/home/runner/downloads")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/home/runner/uploads")
RCLONE_BIN = os.getenv("RCLONE_BIN", "rclone")
RCLONE_GDRIVE_REMOTE = os.getenv("RCLONE_GDRIVE_REMOTE", "").strip()
RCLONE_TERABOX_REMOTE = os.getenv("RCLONE_TERABOX_REMOTE", "terabox:Mirror").strip()
PROGRESS_INTERVAL = int(os.getenv("PROGRESS_INTERVAL", "5"))
PUBLIC_MODE = os.getenv("PUBLIC_MODE", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

try:
    ARIA2_BUTTON_TTL_SECONDS = max(60, int(os.getenv("ARIA2_BUTTON_TTL_SECONDS", "3600")))
except ValueError:
    ARIA2_BUTTON_TTL_SECONDS = 3600

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID dan API_HASH wajib diisi.")

OWNER_USER_ID = None
if OWNER_USER_ID_RAW:
    try:
        OWNER_USER_ID = int(OWNER_USER_ID_RAW)
    except ValueError:
        raise RuntimeError("OWNER_USER_ID harus angka jika diisi.")

BOT_MODE = bool(BOT_TOKEN)

api_id = int(API_ID)
download_root = Path(DOWNLOAD_DIR).expanduser().resolve()
download_root.mkdir(parents=True, exist_ok=True)
download_target = f"{download_root}{os.sep}"
upload_root = Path(UPLOAD_DIR).expanduser().resolve()
upload_root.mkdir(parents=True, exist_ok=True)

if BOT_MODE:
    app = Client(
        "manual_downloader_bot",
        api_id=api_id,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    )
elif SESSION_STRING:
    app = Client(
        "manual_downloader",
        api_id=api_id,
        api_hash=API_HASH,
        session_string=SESSION_STRING,
    )
else:
    app = Client("manual_downloader", api_id=api_id, api_hash=API_HASH)

# t.me/c/<internal_chat_id>/<msg_id> or t.me/c/<internal_chat_id>/<topic_id>/<msg_id>
PRIVATE_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/c/(?P<chat>\d+)/(?:(?P<topic>\d+)/)?(?P<msg>\d+)",
    re.IGNORECASE,
)

# t.me/<username>/<msg_id>
PUBLIC_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/(?P<chat>[a-zA-Z0-9_]{5,})/(?P<msg>\d+)",
    re.IGNORECASE,
)

# tg://openmessage?chat_id=<id>&message_id=<id>
OPENMESSAGE_RE = re.compile(
    r"tg://openmessage\?chat_id=(?P<chat>-?\d+)&message_id=(?P<msg>\d+)",
    re.IGNORECASE,
)

VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".mov",
    ".webm",
    ".avi",
    ".m4v",
    ".3gp",
}

LIST_MAX_ENTRIES = int(os.getenv("LIST_MAX_ENTRIES", "200"))
LIST_MAX_CHARS = int(os.getenv("LIST_MAX_CHARS", "3800"))
UPLOAD_CONTROL = {"task": None, "cancel_event": None}
DOWNLOAD_CONTROL = {
    "task": None,
    "lock": asyncio.Lock(),
    "counter": 0,
    "queue": [],
    "current": None,
    "history": [],
}
ARIA2_UPLOAD_JOBS: Dict[str, Dict[str, object]] = {}


def command_filter(name: str, allow_public: bool = False):
    if BOT_MODE:
        return filters.command(name, prefixes="/")
    if PUBLIC_MODE and allow_public:
        scope_filter = filters.all
    else:
        scope_filter = filters.me
    return filters.command(name, prefixes="/") & scope_filter


def format_bytes(value: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    unit = 0
    while size >= 1024 and unit < len(units) - 1:
        size /= 1024
        unit += 1
    return f"{size:.2f} {units[unit]}"


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}j {minutes:02d}m {sec:02d}s"
    if minutes:
        return f"{minutes:d}m {sec:02d}s"
    return f"{sec:d}s"


def progress_bar(percent: float, width: int = 20) -> str:
    safe_percent = max(0.0, min(100.0, percent))
    filled = int(round((safe_percent / 100.0) * width))
    filled = max(0, min(width, filled))
    return "#" * filled + "-" * (width - filled)


def parse_telegram_link(text: Optional[str]) -> Optional[Tuple[object, int]]:
    if not text:
        return None

    private_match = PRIVATE_LINK_RE.search(text)
    if private_match:
        chat_id = int(f"-100{private_match.group('chat')}")
        message_id = int(private_match.group("msg"))
        return chat_id, message_id

    public_match = PUBLIC_LINK_RE.search(text)
    if public_match:
        username = public_match.group("chat")
        if username.lower() == "c":
            return None
        message_id = int(public_match.group("msg"))
        return username, message_id

    openmessage_match = OPENMESSAGE_RE.search(text)
    if openmessage_match:
        chat_id_raw = int(openmessage_match.group("chat"))
        message_id = int(openmessage_match.group("msg"))
        return chat_id_raw, message_id

    return None


def has_downloadable_media(message) -> bool:
    return any(
        (
            message.document,
            message.video,
            message.audio,
            message.photo,
            message.animation,
            message.voice,
            message.video_note,
        )
    )


def media_label(message) -> str:
    if message.document:
        return f"document: {message.document.file_name or 'unknown'}"
    if message.video:
        return f"video: {message.video.file_name or 'unknown'}"
    if message.audio:
        return f"audio: {message.audio.file_name or 'unknown'}"
    if message.photo:
        return "photo"
    if message.animation:
        return f"animation: {message.animation.file_name or 'unknown'}"
    if message.voice:
        return "voice"
    if message.video_note:
        return "video_note"
    return "media"


async def progress_callback(current, total, status_message, media_name, state):
    now = time.time()
    last_tick = state["last_tick"]
    is_done = total > 0 and current >= total

    if not is_done and (now - last_tick) < PROGRESS_INTERVAL:
        return

    elapsed = max(now - state["started_at"], 0.001)
    speed = current / elapsed
    eta = int((total - current) / speed) if speed > 0 and total > 0 else 0
    percent = (current * 100 / total) if total > 0 else 0
    bar = progress_bar(percent)

    control = state.get("download_control")
    request_id = state.get("download_request_id")
    if control and request_id is not None:
        current_state = control.get("current")
        if current_state and current_state.get("id") == request_id:
            current_state["current_bytes"] = int(current)
            current_state["total_bytes"] = int(total) if total else 0
            current_state["percent"] = float(percent)
            current_state["speed"] = float(speed)
            current_state["eta"] = int(eta)
            current_state["elapsed"] = int(elapsed)
            current_state["updated_at"] = now

    text = (
        "Download berjalan\n"
        f"File: {media_name}\n"
        f"Progress: [{bar}] {percent:.2f}%\n"
        f"Size: {format_bytes(current)} / {format_bytes(total)}\n"
        f"Speed: {format_bytes(speed)}/s\n"
        f"Elapsed: {format_duration(int(elapsed))}\n"
        f"ETA: {format_duration(eta)}"
    )

    print(
        f"[DL] {media_name} | {percent:.2f}% | "
        f"{format_bytes(current)}/{format_bytes(total)} | "
        f"{format_bytes(speed)}/s | ETA {format_duration(eta)}"
    )

    try:
        await status_message.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception:
        pass

    state["last_tick"] = now


async def upload_progress_callback(current, total, status_message, media_name, state):
    cancel_event = state.get("cancel_event")
    if cancel_event and cancel_event.is_set():
        raise asyncio.CancelledError("Upload dibatalkan oleh pengguna.")

    now = time.time()
    last_tick = state["last_tick"]
    is_done = total > 0 and current >= total

    if not is_done and (now - last_tick) < PROGRESS_INTERVAL:
        return

    elapsed = max(now - state["started_at"], 0.001)
    speed = current / elapsed
    eta = int((total - current) / speed) if speed > 0 and total > 0 else 0
    percent = (current * 100 / total) if total > 0 else 0

    text = (
        "Upload berjalan\n"
        f"File: {media_name}\n"
        f"Progress: {percent:.2f}%\n"
        f"Size: {format_bytes(current)} / {format_bytes(total)}\n"
        f"Speed: {format_bytes(speed)}/s\n"
        f"ETA: {format_duration(eta)}"
    )

    print(
        f"[UP] {media_name} | {percent:.2f}% | "
        f"{format_bytes(current)}/{format_bytes(total)} | "
        f"{format_bytes(speed)}/s | ETA {format_duration(eta)}"
    )

    try:
        await status_message.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception:
        pass

    state["last_tick"] = now


def parse_chat_target(raw_target: str):
    value = raw_target.strip()
    if not value or value.lower() == "me":
        return "me"
    if value.startswith("@"):
        value = value[1:]
    if value.lstrip("-").isdigit():
        return int(value)
    return value


def command_args(message):
    raw = (message.text or message.caption or "").strip()
    if not raw:
        return []
    try:
        parts = shlex.split(raw)
    except ValueError:
        parts = raw.split()
    if len(parts) <= 1:
        return []
    return parts[1:]


def parse_disk_command_target(command_name: str, args: List[str]) -> Tuple[Optional[str], Optional[str]]:
    path_parts = []
    for arg in args:
        if arg in {"-h", "--human-readable"}:
            continue
        if arg.startswith("-"):
            return None, (
                f"Opsi tidak dikenal: `{arg}`\n"
                f"Format: `/{command_name} [-h] [path]`"
            )
        path_parts.append(arg)
    return " ".join(path_parts).strip() or ".", None


def parse_aria2_command_args(
    args: List[str],
) -> Tuple[Optional[List[str]], Optional[str], Optional[Path], Optional[str]]:
    urls: List[str] = []
    output_name: Optional[str] = None
    target_dir: Path = download_root

    i = 0
    while i < len(args):
        arg = args[i]

        if arg in {"--out", "-o"}:
            i += 1
            if i >= len(args):
                return None, None, None, "Nilai --out tidak boleh kosong."
            output_name = args[i].strip()
            if not output_name:
                return None, None, None, "Nilai --out tidak boleh kosong."
            if Path(output_name).name != output_name:
                return None, None, None, "Nilai --out harus nama file, bukan path."
        elif arg.startswith("--out="):
            output_name = arg.split("=", 1)[1].strip()
            if not output_name:
                return None, None, None, "Nilai --out tidak boleh kosong."
            if Path(output_name).name != output_name:
                return None, None, None, "Nilai --out harus nama file, bukan path."
        elif arg in {"--dir", "-d"}:
            i += 1
            if i >= len(args):
                return None, None, None, "Nilai --dir tidak boleh kosong."
            try:
                target_dir = local_path_from_text(args[i])
            except Exception as e:
                return None, None, None, f"Path --dir tidak valid: `{e}`"
        elif arg.startswith("--dir="):
            dir_value = arg.split("=", 1)[1].strip()
            if not dir_value:
                return None, None, None, "Nilai --dir tidak boleh kosong."
            try:
                target_dir = local_path_from_text(dir_value)
            except Exception as e:
                return None, None, None, f"Path --dir tidak valid: `{e}`"
        elif arg.startswith("-"):
            return None, None, None, (
                f"Opsi tidak dikenal: `{arg}`\n"
                "Format: `/aria2 <url|magnet> [--out nama_file] [--dir path]`\n"
                "Atau reply file/link Telegram lalu kirim `/aria2`."
            )
        else:
            urls.append(arg)

        i += 1

    if not urls:
        return None, None, None, (
            "Format aria2:\n"
            "`/aria2 <url|magnet>`\n"
            "`/aria2 <url> --out nama_file.ext`\n"
            "`/aria2 <url|magnet> --dir /home/runner/downloads/`\n"
            "`/aria2` sambil reply file/link Telegram\n"
            "Alias: `/a2`"
        )

    if output_name and len(urls) > 1:
        return None, None, None, "--out hanya boleh dipakai jika URL sumber satu."

    return urls, output_name, target_dir, None


ARIA2_SIZE_RE = re.compile(
    r"(?P<done>\d+(?:\.\d+)?[KMGTP]?i?B)/(?P<total>\d+(?:\.\d+)?[KMGTP]?i?B)\((?P<percent>\d{1,3})%\)",
    re.IGNORECASE,
)
ARIA2_PERCENT_RE = re.compile(r"\((?P<percent>\d{1,3})%\)")
ARIA2_SPEED_RE = re.compile(r"(?:DL|SPD):(?P<speed>[^\s\]]+)", re.IGNORECASE)
ARIA2_ETA_RE = re.compile(r"ETA:(?P<eta>[^\s\]]+)", re.IGNORECASE)


def update_aria2_progress_from_line(progress_state: Dict[str, object], line: str) -> None:
    cleaned = line.strip()
    if not cleaned:
        return

    changed = False

    size_match = ARIA2_SIZE_RE.search(cleaned)
    if size_match:
        progress_state["size_done"] = size_match.group("done")
        progress_state["size_total"] = size_match.group("total")
        progress_state["percent"] = float(size_match.group("percent"))
        changed = True
    else:
        percent_match = ARIA2_PERCENT_RE.search(cleaned)
        if percent_match:
            progress_state["percent"] = float(percent_match.group("percent"))
            changed = True

    speed_match = ARIA2_SPEED_RE.search(cleaned)
    if speed_match:
        speed = speed_match.group("speed")
        if speed and not speed.endswith("/s"):
            speed = f"{speed}/s"
        progress_state["speed"] = speed
        changed = True

    eta_match = ARIA2_ETA_RE.search(cleaned)
    if eta_match:
        progress_state["eta"] = eta_match.group("eta")
        changed = True

    if changed:
        progress_state["line"] = cleaned[:240]
        progress_state["updated_at"] = time.time()


async def collect_aria2_stream(
    stream,
    chunks: List[str],
    progress_state: Dict[str, object],
    tail_key: str,
) -> None:
    if not stream:
        return

    while True:
        chunk = await stream.read(1024)
        if not chunk:
            break

        text = chunk.decode("utf-8", errors="ignore")
        chunks.append(text)
        if len(chunks) > 200:
            del chunks[:100]

        pending = str(progress_state.get(tail_key, "")) + text
        parts = re.split(r"[\r\n]", pending)
        progress_state[tail_key] = parts.pop() if parts else ""
        for part in parts:
            update_aria2_progress_from_line(progress_state, part)

    leftover = str(progress_state.get(tail_key, "")).strip()
    if leftover:
        update_aria2_progress_from_line(progress_state, leftover)
    progress_state[tail_key] = ""


def snapshot_directory_file_state(root: Path) -> Dict[str, Tuple[int, int]]:
    snapshot: Dict[str, Tuple[int, int]] = {}
    try:
        candidates = [root] if root.is_file() else root.rglob("*")
    except Exception:
        return snapshot

    for path in candidates:
        try:
            if not path.is_file():
                continue
            if path.name.endswith(".aria2"):
                continue
            resolved = path.resolve()
            stat_info = resolved.stat()
            snapshot[str(resolved)] = (int(stat_info.st_size), int(stat_info.st_mtime_ns))
        except Exception:
            continue
    return snapshot


def detect_aria2_downloaded_files(
    target_dir: Path,
    before_snapshot: Dict[str, Tuple[int, int]],
    started_at: float,
    output_name: Optional[str] = None,
) -> List[Path]:
    found_files: List[Path] = []
    seen_paths = set()

    if output_name:
        preferred_path = (target_dir / output_name).resolve()
        if preferred_path.exists() and preferred_path.is_file():
            key = str(preferred_path)
            seen_paths.add(key)
            found_files.append(preferred_path)

    try:
        candidates = [target_dir] if target_dir.is_file() else target_dir.rglob("*")
    except Exception:
        return found_files

    for path in candidates:
        try:
            if not path.is_file() or path.name.endswith(".aria2"):
                continue

            resolved = path.resolve()
            key = str(resolved)
            stat_info = resolved.stat()
            current_size = int(stat_info.st_size)
            current_mtime_ns = int(stat_info.st_mtime_ns)
            current_mtime = float(stat_info.st_mtime)
            previous = before_snapshot.get(key)

            changed = False
            if previous is None:
                changed = current_mtime >= (started_at - 2.0)
            else:
                previous_size, previous_mtime_ns = previous
                if current_size != previous_size or current_mtime_ns > previous_mtime_ns:
                    changed = current_mtime >= (started_at - 2.0)

            if changed and key not in seen_paths:
                seen_paths.add(key)
                found_files.append(resolved)
        except Exception:
            continue

    def sort_key(item: Path):
        try:
            mtime = item.stat().st_mtime if item.exists() else 0.0
        except Exception:
            mtime = 0.0
        return (mtime, item.name.lower())

    found_files.sort(key=sort_key)
    return found_files


def cleanup_expired_aria2_upload_jobs() -> None:
    now = time.time()
    expired_tokens = []
    for token, payload in ARIA2_UPLOAD_JOBS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        ARIA2_UPLOAD_JOBS.pop(token, None)


def normalize_existing_file_paths(paths: List[Path]) -> List[Path]:
    normalized: List[Path] = []
    seen = set()
    for item in paths:
        try:
            resolved = item.resolve()
        except Exception:
            resolved = item

        key = str(resolved)
        if key in seen:
            continue
        if resolved.exists() and resolved.is_file():
            seen.add(key)
            normalized.append(resolved)
    return normalized


def register_aria2_upload_job(
    requester_id: Optional[int],
    chat_id: int,
    files: List[Path],
    target_dir: Optional[Path] = None,
) -> Optional[str]:
    valid_files = normalize_existing_file_paths(files)
    if not valid_files:
        return None

    cleanup_expired_aria2_upload_jobs()
    token = secrets.token_hex(4)
    base_dir = target_dir or valid_files[0].parent
    try:
        base_dir_text = str(base_dir.resolve())
    except Exception:
        base_dir_text = str(base_dir)

    ARIA2_UPLOAD_JOBS[token] = {
        "requester_id": requester_id,
        "chat_id": chat_id,
        "files": [str(path) for path in valid_files],
        "target_dir": base_dir_text,
        "last_action": None,
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def build_aria2_upload_keyboard(token: str, include_retry: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Upload Telegram", callback_data=f"a2up|{token}|tg"),
        ],
        [
            InlineKeyboardButton("Rclone GDrive", callback_data=f"a2up|{token}|gd"),
            InlineKeyboardButton("Rclone Terabox", callback_data=f"a2up|{token}|tb"),
        ],
    ]
    if include_retry:
        rows.append(
            [
                InlineKeyboardButton("Retry Terakhir", callback_data=f"a2up|{token}|retry"),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("Lewati", callback_data=f"a2up|{token}|skip"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def resolve_aria2_job_files(job_payload: Dict[str, object]) -> List[Path]:
    files_raw = job_payload.get("files")
    if not isinstance(files_raw, list):
        return []
    return normalize_existing_file_paths([Path(str(raw)) for raw in files_raw])


def build_upload_summary(
    summary_title: str,
    target_text: str,
    total_files: int,
    success_lines: List[str],
    failed_lines: List[str],
) -> str:
    summary_lines = [
        summary_title,
        f"Tujuan: `{target_text}`",
        f"Total file: `{total_files}`",
        f"Berhasil: `{len(success_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]

    if success_lines:
        summary_lines.append("")
        summary_lines.append("Daftar berhasil:")
        summary_lines.extend(success_lines[:20])
        if len(success_lines) > 20:
            summary_lines.append(f"... {len(success_lines) - 20} file lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Daftar gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} file lain.")

    return trim_output("\n".join(summary_lines))


def build_rclone_destination(remote_base: str, file_name: str) -> str:
    base = remote_base.strip()
    if base.endswith(":") or base.endswith("/"):
        return f"{base}{file_name}"
    return f"{base}/{file_name}"


def summarize_process_error(stdout_raw: bytes, stderr_raw: bytes, return_code: int) -> str:
    stderr_text = stderr_raw.decode("utf-8", errors="ignore").strip()
    stdout_text = stdout_raw.decode("utf-8", errors="ignore").strip()
    merged = stderr_text or stdout_text
    if not merged:
        return f"return code {return_code}"

    lines = [line.strip() for line in merged.splitlines() if line.strip()]
    if not lines:
        return f"return code {return_code}"
    return trim_output(lines[-1])[:200]


async def upload_files_to_telegram_target(
    client: Client,
    command_message,
    status_message,
    source_paths: List[Path],
    target_chat,
    cancel_event: asyncio.Event,
) -> Tuple[object, List[str], List[str], bool, List[Path]]:
    target = target_label(target_chat)
    total_files = len(source_paths)
    success_lines: List[str] = []
    failed_lines: List[str] = []
    failed_paths: List[Path] = []
    cancelled = False

    for index, source_path in enumerate(source_paths, start=1):
        if cancel_event.is_set():
            cancelled = True
            failed_paths.extend(source_paths[index - 1 :])
            break

        media_name = source_path.name
        state = {"started_at": time.time(), "last_tick": 0.0, "cancel_event": cancel_event}

        status_message = await update_status_message(
            command_message,
            status_message,
            "Upload berjalan\n"
            f"File: `{index}/{total_files}`\n"
            f"Nama: `{media_name}`\n"
            f"Sumber: `{source_path}`\n"
            f"Tujuan: `{target}`"
        )

        try:
            suffix = source_path.suffix.lower()
            if suffix in VIDEO_EXTENSIONS:
                video_kwargs = {
                    "chat_id": target_chat,
                    "video": str(source_path),
                    "caption": f"`{media_name}`",
                    "supports_streaming": True,
                    "progress": upload_progress_callback,
                    "progress_args": (status_message, media_name, state),
                }
                video_metadata = await probe_video_metadata(source_path)
                for key in ("duration", "width", "height"):
                    value = video_metadata.get(key)
                    if value:
                        video_kwargs[key] = value

                if "duration" not in video_kwargs:
                    print(
                        f"[WARN] Durasi video tidak terdeteksi untuk {source_path}. "
                        "Telegram bisa menampilkan durasi 0:00."
                    )

                thumb_path = await generate_video_thumbnail(source_path)
                if thumb_path:
                    video_kwargs["thumb"] = str(thumb_path)
                else:
                    print(
                        f"[WARN] Thumbnail tidak berhasil dibuat untuk {source_path.name}. "
                        "Telegram mungkin menampilkan preview kosong."
                    )

                try:
                    sent_message = await client.send_video(**video_kwargs)
                finally:
                    if thumb_path:
                        remove_file_quietly(thumb_path)
            else:
                sent_message = await client.send_document(
                    chat_id=target_chat,
                    document=str(source_path),
                    caption=f"`{media_name}`",
                    progress=upload_progress_callback,
                    progress_args=(status_message, media_name, state),
                )
        except asyncio.CancelledError:
            cancel_event.set()
            cancelled = True
            failed_lines.append(f"- `{media_name}` -> dibatalkan")
            failed_paths.append(source_path)
            if index < total_files:
                failed_paths.extend(source_paths[index:])
            break
        except Exception as e:
            failed_lines.append(f"- `{media_name}` -> {e}")
            failed_paths.append(source_path)
            continue

        if sent_message.chat and sent_message.chat.username:
            ref = f"https://t.me/{sent_message.chat.username}/{sent_message.id}"
        else:
            ref = f"{sent_message.chat.id}/{sent_message.id}"
        delete_error = remove_local_file_after_upload(source_path)
        if delete_error:
            success_lines.append(
                f"- `{media_name}` -> `{ref}` (upload OK, hapus lokal gagal: `{delete_error}`)"
            )
        else:
            success_lines.append(f"- `{media_name}` -> `{ref}` (lokal dihapus)")

    if cancel_event.is_set():
        cancelled = True

    return status_message, success_lines, failed_lines, cancelled, normalize_existing_file_paths(failed_paths)


async def upload_files_via_rclone(
    command_message,
    status_message,
    source_paths: List[Path],
    remote_base: str,
    remote_label: str,
) -> Tuple[object, List[str], List[str], List[Path]]:
    success_lines: List[str] = []
    failed_lines: List[str] = []
    failed_paths: List[Path] = []
    total_files = len(source_paths)

    for index, source_path in enumerate(source_paths, start=1):
        destination = build_rclone_destination(remote_base, source_path.name)
        status_message = await update_status_message(
            command_message,
            status_message,
            "Rclone upload berjalan\n"
            f"File: `{index}/{total_files}`\n"
            f"Nama: `{source_path.name}`\n"
            f"Sumber: `{source_path}`\n"
            f"Tujuan: `{destination}`\n"
            f"Remote: `{remote_label}`"
        )

        command = [
            RCLONE_BIN,
            "copyto",
            str(source_path),
            destination,
            "--stats=1s",
            "--stats-one-line",
            "--retries=2",
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            failed_lines.append(
                f"- `{source_path.name}` -> binary `{RCLONE_BIN}` tidak ditemukan."
            )
            failed_paths.extend(source_paths[index - 1 :])
            break
        except Exception as e:
            failed_lines.append(f"- `{source_path.name}` -> gagal menjalankan rclone: {e}")
            failed_paths.append(source_path)
            continue

        stdout_raw, stderr_raw = await process.communicate()
        if process.returncode == 0:
            delete_error = remove_local_file_after_upload(source_path)
            if delete_error:
                success_lines.append(
                    f"- `{source_path.name}` -> `{destination}` "
                    f"(upload OK, hapus lokal gagal: `{delete_error}`)"
                )
            else:
                success_lines.append(f"- `{source_path.name}` -> `{destination}` (lokal dihapus)")
        else:
            reason = summarize_process_error(stdout_raw, stderr_raw, process.returncode)
            failed_lines.append(f"- `{source_path.name}` -> {reason}")
            failed_paths.append(source_path)

    return status_message, success_lines, failed_lines, normalize_existing_file_paths(failed_paths)


def requester_label(message) -> str:
    user = getattr(message, "from_user", None)
    if not user:
        return "unknown"
    if user.username:
        return f"@{user.username}"
    full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
    if full_name:
        return f"{full_name} ({user.id})"
    return str(user.id)


def is_saved_messages_only_violation(message) -> bool:
    if not message.from_user:
        return True
    return message.chat.id != message.from_user.id


def is_owner_in_bot_mode(message) -> bool:
    if not BOT_MODE:
        return False
    if not OWNER_USER_ID or not message.from_user:
        return False
    return message.from_user.id == OWNER_USER_ID


async def require_private_command_access(message, command_name: str) -> bool:
    if BOT_MODE:
        if not OWNER_USER_ID:
            await open_status_message(
                message,
                "OWNER_USER_ID belum diatur. Set env OWNER_USER_ID agar command private aktif di mode bot.",
            )
            return False
        if not is_owner_in_bot_mode(message):
            await open_status_message(
                message,
                f"Perintah /{command_name} hanya untuk owner bot.",
            )
            return False
        return True

    if is_saved_messages_only_violation(message):
        await open_status_message(message, f"Gunakan /{command_name} hanya di Saved Messages.")
        return False
    return True


async def require_public_command_access(message, command_name: str) -> bool:
    if BOT_MODE:
        if PUBLIC_MODE:
            return True
        return await require_private_command_access(message, command_name)

    if PUBLIC_MODE:
        return True
    if is_saved_messages_only_violation(message):
        await open_status_message(message, f"Gunakan /{command_name} hanya di Saved Messages.")
        return False
    return True


async def open_status_message(message, text: str, reply_markup=None):
    if BOT_MODE:
        return await message.reply_text(text, reply_markup=reply_markup)

    if not PUBLIC_MODE or getattr(message, "outgoing", False):
        try:
            await message.edit_text(text, reply_markup=reply_markup)
            return message
        except Exception:
            pass
    return await message.reply_text(text, reply_markup=reply_markup)


async def update_status_message(command_message, status_message, text: str, reply_markup=None):
    try:
        await status_message.edit_text(text, reply_markup=reply_markup)
        return status_message
    except Exception:
        pass

    try:
        return await command_message.reply_text(text, reply_markup=reply_markup)
    except Exception:
        return status_message


async def append_download_history(entry: Dict[str, object]) -> None:
    async with DOWNLOAD_CONTROL["lock"]:
        history = DOWNLOAD_CONTROL["history"]
        history.insert(0, entry)
        del history[20:]


async def start_download_worker_if_needed(client: Client) -> None:
    async with DOWNLOAD_CONTROL["lock"]:
        current_task = DOWNLOAD_CONTROL.get("task")
        if current_task and not current_task.done():
            return
        DOWNLOAD_CONTROL["task"] = asyncio.create_task(download_queue_worker(client))


async def enqueue_download_request(
    client: Client,
    command_message,
    status_message,
    target_message,
    media_name: str,
    enable_upload_buttons: bool = False,
) -> Tuple[int, int, int]:
    now = time.time()
    async with DOWNLOAD_CONTROL["lock"]:
        DOWNLOAD_CONTROL["counter"] += 1
        request_id = DOWNLOAD_CONTROL["counter"]
        queue_item = {
            "id": request_id,
            "enqueued_at": now,
            "command_message": command_message,
            "status_message": status_message,
            "target_message": target_message,
            "media_name": media_name,
            "requester": requester_label(command_message),
            "chat_id": command_message.chat.id,
            "enable_upload_buttons": bool(enable_upload_buttons),
        }
        DOWNLOAD_CONTROL["queue"].append(queue_item)
        queue_position = len(DOWNLOAD_CONTROL["queue"])
        has_active = DOWNLOAD_CONTROL.get("current") is not None
        overall_position = queue_position + (1 if has_active else 0)

    await start_download_worker_if_needed(client)
    return request_id, queue_position, overall_position


async def snapshot_download_state() -> Dict[str, object]:
    async with DOWNLOAD_CONTROL["lock"]:
        current = DOWNLOAD_CONTROL.get("current")
        queue = DOWNLOAD_CONTROL.get("queue", [])
        history = DOWNLOAD_CONTROL.get("history", [])
        return {
            "current": dict(current) if current else None,
            "queue": [dict(item) for item in queue],
            "history": [dict(item) for item in history[:5]],
        }


async def download_queue_worker(client: Client):
    while True:
        async with DOWNLOAD_CONTROL["lock"]:
            if not DOWNLOAD_CONTROL["queue"]:
                DOWNLOAD_CONTROL["task"] = None
                DOWNLOAD_CONTROL["current"] = None
                return

            item = DOWNLOAD_CONTROL["queue"].pop(0)
            started_at = time.time()
            DOWNLOAD_CONTROL["current"] = {
                "id": item["id"],
                "media_name": item["media_name"],
                "requester": item["requester"],
                "chat_id": item["chat_id"],
                "started_at": started_at,
                "current_bytes": 0,
                "total_bytes": 0,
                "percent": 0.0,
                "speed": 0.0,
                "eta": 0,
                "elapsed": 0,
                "updated_at": started_at,
            }
            remaining_queue = len(DOWNLOAD_CONTROL["queue"])

        command_message = item["command_message"]
        status_message = item["status_message"]
        media_name = item["media_name"]
        request_id = item["id"]
        state = {
            "started_at": time.time(),
            "last_tick": 0.0,
            "download_control": DOWNLOAD_CONTROL,
            "download_request_id": request_id,
        }

        status_message = await update_status_message(
            command_message,
            status_message,
            "Memulai download dari antrian\n"
            f"ID: `#{request_id}`\n"
            f"File: {media_name}\n"
            f"Sisa antrian setelah ini: `{remaining_queue}`",
        )

        download_started_at = time.time()
        try:
            file_path = await item["target_message"].download(
                file_name=download_target,
                progress=progress_callback,
                progress_args=(status_message, media_name, state),
            )
        except Exception as e:
            await update_status_message(
                command_message,
                status_message,
                "Download gagal.\n"
                f"ID: `#{request_id}`\n"
                f"File: {media_name}\n"
                f"Error: `{e}`",
            )
            await append_download_history(
                {
                    "id": request_id,
                    "media_name": media_name,
                    "requester": item["requester"],
                    "status": "failed",
                    "duration": int(max(0, time.time() - download_started_at)),
                    "error": str(e),
                    "finished_at": int(time.time()),
                }
            )
        else:
            if not file_path:
                await update_status_message(
                    command_message,
                    status_message,
                    "Download gagal: path file kosong.\n"
                    f"ID: `#{request_id}`\n"
                    f"File: {media_name}",
                )
                await append_download_history(
                    {
                        "id": request_id,
                        "media_name": media_name,
                        "requester": item["requester"],
                        "status": "failed",
                        "duration": int(max(0, time.time() - download_started_at)),
                        "error": "path file kosong",
                        "finished_at": int(time.time()),
                    }
                )
            else:
                try:
                    downloaded_path = Path(str(file_path)).resolve()
                except Exception:
                    downloaded_path = Path(str(file_path))

                if item.get("enable_upload_buttons"):
                    token = register_aria2_upload_job(
                        requester_id=getattr(command_message.from_user, "id", None),
                        chat_id=item.get("chat_id", command_message.chat.id),
                        files=[downloaded_path],
                        target_dir=downloaded_path.parent,
                    )
                    if token:
                        status_message = await update_status_message(
                            command_message,
                            status_message,
                            "Download selesai.\n"
                            f"ID: `#{request_id}`\n"
                            f"Lokasi: `{downloaded_path}`\n\n"
                            "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox.\n"
                            f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
                            reply_markup=build_aria2_upload_keyboard(token),
                        )
                    else:
                        status_message = await update_status_message(
                            command_message,
                            status_message,
                            "Download selesai.\n"
                            f"ID: `#{request_id}`\n"
                            f"Lokasi: `{downloaded_path}`\n"
                            "Catatan: tombol upload tidak tersedia karena file tidak terdeteksi.",
                        )
                else:
                    status_message = await update_status_message(
                        command_message,
                        status_message,
                        "Download selesai.\n"
                        f"ID: `#{request_id}`\n"
                        f"Lokasi: `{downloaded_path}`",
                    )
                await append_download_history(
                    {
                        "id": request_id,
                        "media_name": media_name,
                        "requester": item["requester"],
                        "status": "done",
                        "duration": int(max(0, time.time() - download_started_at)),
                        "path": str(downloaded_path),
                        "finished_at": int(time.time()),
                    }
                )
        finally:
            async with DOWNLOAD_CONTROL["lock"]:
                current = DOWNLOAD_CONTROL.get("current")
                if current and current.get("id") == request_id:
                    DOWNLOAD_CONTROL["current"] = None


def local_path_from_text(path_text: str) -> Path:
    raw = path_text.strip()
    if raw.startswith("file://"):
        raw = raw[7:]
    path = Path(os.path.expandvars(raw)).expanduser()
    return path.resolve()


def has_wildcard(path_text: str) -> bool:
    return any(char in path_text for char in ("*", "?", "["))


def resolve_path_candidates(path_text: str) -> Tuple[List[Path], Optional[str]]:
    raw = path_text.strip()
    if not raw:
        return [], "Path kosong."

    if raw.startswith("file://"):
        raw = raw[7:]

    expanded = os.path.expandvars(raw)
    candidate = os.path.expanduser(expanded)

    if has_wildcard(candidate):
        raw_matches = sorted(glob.glob(candidate, recursive=True))
        if not raw_matches:
            return [], f"Wildcard tidak cocok: `{path_text}`"

        matched_paths: List[Path] = []
        seen_paths = set()
        for item in raw_matches:
            resolved = Path(item).resolve()
            key = str(resolved)
            if key not in seen_paths:
                seen_paths.add(key)
                matched_paths.append(resolved)
        return matched_paths, None

    try:
        resolved = Path(candidate).resolve()
    except Exception as e:
        return [], f"Path tidak valid `{path_text}`: {e}"

    return [resolved], None


def path_exists_or_symlink(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def is_root_path(path: Path) -> bool:
    try:
        return path.parent == path
    except Exception:
        return False


def resolve_upload_sources(path_text: str) -> Tuple[List[Path], Optional[str]]:
    raw = path_text.strip()
    if not raw:
        return [], "Path upload kosong."

    if raw.startswith("file://"):
        raw = raw[7:]

    expanded = os.path.expandvars(raw)
    is_absolute_or_home = expanded.startswith("~") or Path(expanded).is_absolute()
    candidates = [expanded]
    if not is_absolute_or_home:
        candidates.append(str(upload_root / expanded))

    matched_files: List[Path] = []
    seen_paths = set()
    first_directory = None
    has_pattern = has_wildcard(expanded)

    for candidate in candidates:
        candidate_expanded = os.path.expanduser(candidate)
        if has_wildcard(candidate_expanded):
            raw_matches = sorted(glob.glob(candidate_expanded, recursive=True))
            for item in raw_matches:
                resolved = Path(item).resolve()
                if resolved.is_file():
                    key = str(resolved)
                    if key not in seen_paths:
                        seen_paths.add(key)
                        matched_files.append(resolved)
        else:
            resolved = Path(candidate_expanded).resolve()
            if resolved.exists():
                if resolved.is_file():
                    key = str(resolved)
                    if key not in seen_paths:
                        seen_paths.add(key)
                        matched_files.append(resolved)
                elif first_directory is None:
                    first_directory = resolved

    if matched_files:
        return matched_files, None

    if first_directory:
        return [], f"Path harus file, bukan folder:\n`{first_directory}`"

    if has_pattern:
        return [], (
            "File tidak ditemukan untuk wildcard.\n"
            f"Pola: `{path_text}`\n"
            f"Default folder upload: `{upload_root}`"
        )

    if not is_absolute_or_home:
        fallback_path = (upload_root / expanded).resolve()
        return [], (
            "File tidak ditemukan.\n"
            f"Input: `{path_text}`\n"
            f"Cek juga: `{fallback_path}`"
        )

    return [], f"File tidak ditemukan:\n`{Path(os.path.expanduser(expanded)).resolve()}`"


def target_label(chat_id_or_username) -> str:
    if chat_id_or_username == "me":
        return "Saved Messages"
    return str(chat_id_or_username)


def short_size(size_bytes: int) -> str:
    return format_bytes(float(size_bytes))


def format_entry_line(path: Path) -> str:
    try:
        stat_info = path.stat()
        size_text = short_size(stat_info.st_size)
        mtime_text = datetime.datetime.fromtimestamp(stat_info.st_mtime).strftime(
            "%Y-%m-%d %H:%M"
        )
    except Exception:
        size_text = "?"
        mtime_text = "?"

    if path.is_dir():
        kind = "DIR"
        name = f"{path.name}/"
    elif path.is_symlink():
        kind = "LNK"
        name = path.name
    else:
        kind = "FIL"
        name = path.name

    return f"{kind:>3}  {size_text:>10}  {mtime_text}  {name}"


def list_directory_lines(target_path: Path, show_all: bool):
    entries = []
    for item in target_path.iterdir():
        if not show_all and item.name.startswith("."):
            continue
        entries.append(item)

    entries.sort(key=lambda p: (not p.is_dir(), p.name.lower()))
    lines = [format_entry_line(item) for item in entries[:LIST_MAX_ENTRIES]]
    truncated = len(entries) > LIST_MAX_ENTRIES
    return lines, len(entries), truncated


def trim_output(text: str) -> str:
    if len(text) <= LIST_MAX_CHARS:
        return text
    return text[: LIST_MAX_CHARS - 40] + "\n... output dipotong ..."


def compute_path_usage(target_path: Path) -> Tuple[int, int, int, int]:
    total_bytes = 0
    file_count = 0
    dir_count = 0
    error_count = 0

    stack = [target_path]
    while stack:
        current = stack.pop()
        try:
            if current.is_symlink():
                total_bytes += current.lstat().st_size
                file_count += 1
                continue

            if current.is_file():
                total_bytes += current.stat().st_size
                file_count += 1
                continue

            if current.is_dir():
                dir_count += 1
                try:
                    for item in current.iterdir():
                        stack.append(item)
                except Exception:
                    error_count += 1
                continue
        except Exception:
            error_count += 1

    return total_bytes, file_count, dir_count, error_count


def remove_file_quietly(path: Optional[Path]) -> None:
    if not path:
        return
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def remove_local_file_after_upload(path: Path) -> Optional[str]:
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path

    try:
        if not resolved.exists():
            return "file lokal tidak ditemukan saat proses hapus"
        if not resolved.is_file():
            return "path lokal bukan file"
        resolved.unlink()
        return None
    except Exception as e:
        return str(e)


def parse_positive_int(raw_value) -> Optional[int]:
    try:
        value = int(float(raw_value))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def parse_positive_seconds(raw_value) -> Optional[int]:
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return max(1, int(round(value)))


def find_child_atom_range(file_obj, start_offset: int, end_offset: int, atom_name: bytes):
    cursor = start_offset
    while cursor + 8 <= end_offset:
        file_obj.seek(cursor)
        header = file_obj.read(8)
        if len(header) < 8:
            return None

        size = int.from_bytes(header[:4], "big", signed=False)
        current_name = header[4:8]
        header_size = 8

        if size == 1:
            extended_size = file_obj.read(8)
            if len(extended_size) < 8:
                return None
            size = int.from_bytes(extended_size, "big", signed=False)
            header_size = 16
        elif size == 0:
            size = end_offset - cursor

        if size < header_size:
            return None

        atom_data_start = cursor + header_size
        atom_end = cursor + size
        if atom_end > end_offset:
            return None

        if current_name == atom_name:
            return atom_data_start, atom_end

        cursor = atom_end

    return None


def mp4_duration_fallback(path: Path) -> Optional[int]:
    try:
        file_size = path.stat().st_size
        if file_size <= 8:
            return None

        with path.open("rb") as file_obj:
            moov_range = find_child_atom_range(file_obj, 0, file_size, b"moov")
            if not moov_range:
                return None

            mvhd_range = find_child_atom_range(file_obj, moov_range[0], moov_range[1], b"mvhd")
            if not mvhd_range:
                return None

            file_obj.seek(mvhd_range[0])
            version_raw = file_obj.read(1)
            if not version_raw:
                return None

            version = version_raw[0]
            file_obj.read(3)  # flags

            if version == 1:
                file_obj.read(16)  # creation + modification time
                timescale_raw = file_obj.read(4)
                duration_raw = file_obj.read(8)
            else:
                file_obj.read(8)  # creation + modification time
                timescale_raw = file_obj.read(4)
                duration_raw = file_obj.read(4)

            if len(timescale_raw) != 4 or len(duration_raw) not in (4, 8):
                return None

            timescale = int.from_bytes(timescale_raw, "big", signed=False)
            duration_units = int.from_bytes(duration_raw, "big", signed=False)
            if timescale <= 0 or duration_units <= 0:
                return None

            return max(1, int(round(duration_units / timescale)))
    except Exception:
        return None


async def probe_video_metadata(path: Path) -> Dict[str, int]:
    metadata: Dict[str, int] = {}
    ffprobe_bin = os.getenv("FFPROBE_BIN", "ffprobe")
    command = [
        ffprobe_bin,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_streams",
        "-show_format",
        str(path),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        process = None
    except Exception as e:
        print(f"[WARN] Gagal menjalankan ffprobe untuk {path.name}: {e}")
        process = None

    if process:
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            try:
                payload = json.loads(stdout.decode("utf-8", errors="ignore"))
                streams = payload.get("streams") or []
                video_stream = next(
                    (item for item in streams if item.get("codec_type") == "video"),
                    None,
                )
                if video_stream:
                    width = parse_positive_int(video_stream.get("width"))
                    height = parse_positive_int(video_stream.get("height"))
                    duration = parse_positive_seconds(video_stream.get("duration"))
                    if width:
                        metadata["width"] = width
                    if height:
                        metadata["height"] = height
                    if duration:
                        metadata["duration"] = duration

                if "duration" not in metadata:
                    format_info = payload.get("format") or {}
                    duration = parse_positive_seconds(format_info.get("duration"))
                    if duration:
                        metadata["duration"] = duration
            except json.JSONDecodeError:
                pass
        else:
            error_text = stderr.decode("utf-8", errors="ignore").strip()
            if error_text:
                print(f"[WARN] ffprobe gagal untuk {path.name}: {error_text}")

    if "duration" not in metadata and path.suffix.lower() == ".mp4":
        duration = mp4_duration_fallback(path)
        if duration:
            metadata["duration"] = duration

    return metadata


async def generate_video_thumbnail(path: Path) -> Optional[Path]:
    ffmpeg_bin = os.getenv("FFMPEG_BIN", "ffmpeg")
    thumb_second_raw = os.getenv("THUMBNAIL_SECOND", "1")
    try:
        thumb_second = max(0.0, float(thumb_second_raw))
    except (TypeError, ValueError):
        thumb_second = 1.0

    with tempfile.NamedTemporaryFile(prefix="pyro_thumb_", suffix=".jpg", delete=False) as tmp_file:
        thumb_path = Path(tmp_file.name)

    command = [
        ffmpeg_bin,
        "-y",
        "-ss",
        f"{thumb_second:.3f}",
        "-i",
        str(path),
        "-frames:v",
        "1",
        "-vf",
        "thumbnail,scale=320:-2",
        "-q:v",
        "5",
        str(thumb_path),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        remove_file_quietly(thumb_path)
        print("[WARN] ffmpeg tidak ditemukan, thumbnail otomatis dilewati.")
        return None
    except Exception as e:
        remove_file_quietly(thumb_path)
        print(f"[WARN] Gagal menjalankan ffmpeg untuk thumbnail {path.name}: {e}")
        return None

    _, stderr = await process.communicate()
    if process.returncode != 0:
        remove_file_quietly(thumb_path)
        error_text = stderr.decode("utf-8", errors="ignore").strip()
        if error_text:
            print(f"[WARN] Gagal membuat thumbnail {path.name}: {error_text}")
        return None

    try:
        if not thumb_path.exists() or thumb_path.stat().st_size <= 0:
            remove_file_quietly(thumb_path)
            return None
    except Exception:
        remove_file_quietly(thumb_path)
        return None

    return thumb_path


async def resolve_target_message(client: Client, replied_message):
    if has_downloadable_media(replied_message):
        return replied_message, None

    source_text = replied_message.text or replied_message.caption or ""
    parsed = parse_telegram_link(source_text)
    if not parsed:
        return None, "Pesan yang dibalas tidak berisi media atau link Telegram yang valid."

    chat_id_or_username, message_id = parsed
    try:
        target_message = await client.get_messages(chat_id_or_username, message_id)
    except MessageIdInvalid:
        return None, "ID pesan pada link tidak valid."
    except Exception as e:
        return None, f"Gagal mengambil pesan dari link: {e}"

    if not target_message or target_message.empty:
        return None, "Pesan dari link tidak ditemukan atau akun tidak punya akses."

    if not has_downloadable_media(target_message):
        return None, "Pesan dari link ditemukan, tapi tidak berisi file/video."

    return target_message, None


# Manual only, no auto-download.
@app.on_message(command_filter("d1", allow_public=True))
async def download_command(client: Client, message):
    if not await require_public_command_access(message, "d1"):
        return

    if not message.reply_to_message:
        await open_status_message(
            message,
            "Balas pesan yang berisi file/video atau link Telegram, lalu kirim /d1."
        )
        return

    status_message = await open_status_message(message, "Memeriksa pesan yang dibalas...")
    target_message, error_message = await resolve_target_message(
        client, message.reply_to_message
    )
    if error_message:
        await update_status_message(message, status_message, error_message)
        return

    name = media_label(target_message)
    request_id, queue_position, overall_position = await enqueue_download_request(
        client=client,
        command_message=message,
        status_message=status_message,
        target_message=target_message,
        media_name=name,
    )

    await update_status_message(
        message,
        status_message,
        "Permintaan download masuk antrian.\n"
        f"ID: `#{request_id}`\n"
        f"File: {name}\n"
        f"Posisi antrian: `{queue_position}`\n"
        f"Posisi total (termasuk yang aktif): `{overall_position}`\n"
        "Gunakan `/dstatus` untuk lihat detail."
    )


@app.on_message(command_filter(["dstatus", "dqueue"], allow_public=True))
async def download_status_command(client: Client, message):
    del client

    if not await require_public_command_access(message, "dstatus"):
        return

    snapshot = await snapshot_download_state()
    current = snapshot["current"]
    queue = snapshot["queue"]
    history = snapshot["history"]
    now = time.time()

    lines = ["Status download:"]

    if current:
        percent = float(current.get("percent", 0.0))
        bar = progress_bar(percent)
        current_bytes = int(current.get("current_bytes", 0))
        total_bytes = int(current.get("total_bytes", 0))
        speed = float(current.get("speed", 0.0))
        eta = int(current.get("eta", 0))
        started_at = float(current.get("started_at", now))
        elapsed = max(0, int(now - started_at))

        lines.extend(
            [
                "",
                f"Aktif: `#{current.get('id')}`",
                f"File: `{current.get('media_name', 'unknown')}`",
                f"Requester: `{current.get('requester', 'unknown')}`",
                f"Progress: [{bar}] {percent:.2f}%",
                (
                    f"Size: `{format_bytes(float(current_bytes))}` / "
                    f"`{format_bytes(float(total_bytes))}`"
                    if total_bytes > 0
                    else f"Size: `{format_bytes(float(current_bytes))}` / `unknown`"
                ),
                f"Speed: `{format_bytes(speed)}/s`",
                f"Elapsed: `{format_duration(elapsed)}`",
                f"ETA: `{format_duration(eta)}`",
            ]
        )
    else:
        lines.extend(["", "Aktif: tidak ada download berjalan."])

    lines.extend(["", f"Antrian pending: `{len(queue)}`"])
    if queue:
        lines.append("Daftar antrian:")
        for index, item in enumerate(queue[:10], start=1):
            wait_seconds = max(0, int(now - float(item.get("enqueued_at", now))))
            lines.append(
                f"{index}. `#{item.get('id')}` `{item.get('media_name', 'unknown')}` | "
                f"by `{item.get('requester', 'unknown')}` | "
                f"tunggu `{format_duration(wait_seconds)}`"
            )
        if len(queue) > 10:
            lines.append(f"... {len(queue) - 10} item lain.")

    if history:
        lines.extend(["", "Riwayat terakhir:"])
        for item in history[:5]:
            status = "selesai" if item.get("status") == "done" else "gagal"
            duration = format_duration(int(item.get("duration", 0)))
            base = f"- `#{item.get('id')}` `{item.get('media_name', 'unknown')}` -> {status} ({duration})"
            if item.get("error"):
                base += f" | `{item.get('error')}`"
            lines.append(base)

    await open_status_message(message, trim_output("\n".join(lines)))


@app.on_message(command_filter(["aria2", "a2"], allow_public=True))
async def aria2_download_command(client: Client, message):
    if not await require_public_command_access(message, "aria2"):
        return

    args = command_args(message)

    if not args and message.reply_to_message:
        status_message = await open_status_message(
            message,
            "Memeriksa reply file/link Telegram...",
        )
        target_message, error_message = await resolve_target_message(
            client, message.reply_to_message
        )
        if error_message:
            await update_status_message(message, status_message, error_message)
            return

        name = media_label(target_message)
        request_id, queue_position, overall_position = await enqueue_download_request(
            client=client,
            command_message=message,
            status_message=status_message,
            target_message=target_message,
            media_name=name,
            enable_upload_buttons=True,
        )
        await update_status_message(
            message,
            status_message,
            "Sumber Telegram terdeteksi.\n"
            "Permintaan download masuk antrian (mode Telegram).\n"
            f"ID: `#{request_id}`\n"
            f"File: {name}\n"
            f"Posisi antrian: `{queue_position}`\n"
            f"Posisi total (termasuk yang aktif): `{overall_position}`\n"
            "Gunakan `/dstatus` untuk lihat detail.\n"
            "Setelah download selesai, tombol upload akan muncul.",
        )
        return

    urls, output_name, target_dir, parse_error = parse_aria2_command_args(args)
    if parse_error:
        await open_status_message(message, parse_error)
        return

    if not target_dir:
        await open_status_message(message, "Folder target tidak valid.")
        return

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        await open_status_message(message, f"Gagal membuat folder target: `{e}`")
        return

    before_snapshot = snapshot_directory_file_state(target_dir)

    command = [
        ARIA2_BIN,
        "--dir",
        str(target_dir),
        "--continue=true",
        "--allow-overwrite=true",
        "--auto-file-renaming=true",
        "--summary-interval=1",
        "--download-result=full",
        "--show-console-readout=true",
        "--console-log-level=warn",
        "--file-allocation=none",
    ]
    if output_name:
        command.extend(["--out", output_name])
    command.extend(urls or [])

    status_message = await open_status_message(
        message,
        "Memulai download via aria2...\n"
        f"Target folder: `{target_dir}`\n"
        f"Sumber: `{len(urls or [])}` item",
    )

    started_at = time.time()
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        await update_status_message(
            message,
            status_message,
            "aria2c tidak ditemukan.\n"
            f"Set env `ARIA2_BIN` (saat ini: `{ARIA2_BIN}`) ke binary aria2 yang valid.",
        )
        return
    except Exception as e:
        await update_status_message(message, status_message, f"Gagal menjalankan aria2: `{e}`")
        return

    progress_state: Dict[str, object] = {
        "percent": None,
        "size_done": None,
        "size_total": None,
        "speed": None,
        "eta": None,
        "line": None,
        "updated_at": None,
        "stdout_tail": "",
        "stderr_tail": "",
    }
    stdout_chunks: List[str] = []
    stderr_chunks: List[str] = []
    stdout_task = asyncio.create_task(
        collect_aria2_stream(process.stdout, stdout_chunks, progress_state, "stdout_tail")
    )
    stderr_task = asyncio.create_task(
        collect_aria2_stream(process.stderr, stderr_chunks, progress_state, "stderr_tail")
    )

    last_tick = 0.0
    while True:
        try:
            await asyncio.wait_for(process.wait(), timeout=1.0)
            break
        except asyncio.TimeoutError:
            now = time.time()
            if (now - last_tick) >= PROGRESS_INTERVAL:
                elapsed = format_duration(int(now - started_at))
                percent = progress_state.get("percent")
                if percent is not None:
                    percent_float = float(percent)
                    bar = progress_bar(percent_float)
                    size_done = progress_state.get("size_done") or "unknown"
                    size_total = progress_state.get("size_total") or "unknown"
                    speed = progress_state.get("speed") or "unknown"
                    eta = progress_state.get("eta") or "unknown"
                    updated_at = progress_state.get("updated_at")
                    update_age = (
                        format_duration(int(max(0, now - float(updated_at))))
                        if updated_at
                        else "unknown"
                    )
                    progress_lines = [
                        "Aria2 sedang berjalan...",
                        f"PID: `{process.pid}`",
                        f"Target folder: `{target_dir}`",
                        f"Progress: [{bar}] `{percent_float:.2f}%`",
                        f"Size: `{size_done}` / `{size_total}`",
                        f"Speed: `{speed}`",
                        f"ETA: `{eta}`",
                        f"Elapsed: `{elapsed}`",
                        f"Update terakhir: `{update_age}` lalu",
                    ]
                    raw_line = progress_state.get("line")
                    if raw_line:
                        progress_lines.append("")
                        progress_lines.append("Raw:")
                        progress_lines.append(f"`{raw_line}`")
                    status_message = await update_status_message(
                        message,
                        status_message,
                        "\n".join(progress_lines),
                    )
                else:
                    status_message = await update_status_message(
                        message,
                        status_message,
                        "Aria2 sedang berjalan...\n"
                        f"PID: `{process.pid}`\n"
                        f"Target folder: `{target_dir}`\n"
                        f"Elapsed: `{elapsed}`\n"
                        "Menunggu data progress dari aria2...",
                    )
                last_tick = now

    await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
    stdout_text = "".join(stdout_chunks).strip()
    stderr_text = "".join(stderr_chunks).strip()
    elapsed_text = format_duration(int(max(0, time.time() - started_at)))
    stdout_tail = "\n".join(stdout_text.splitlines()[-12:]) if stdout_text else ""
    stderr_tail = "\n".join(stderr_text.splitlines()[-12:]) if stderr_text else ""

    if process.returncode == 0:
        downloaded_files = detect_aria2_downloaded_files(
            target_dir=target_dir,
            before_snapshot=before_snapshot,
            started_at=started_at,
            output_name=output_name,
        )
        summary_lines = [
            "Aria2 selesai.",
            f"Target folder: `{target_dir}`",
            f"Durasi: `{elapsed_text}`",
            f"Return code: `{process.returncode}`",
        ]
        if output_name:
            summary_lines.append(f"Output name: `{output_name}`")
        if downloaded_files:
            token = register_aria2_upload_job(
                requester_id=getattr(message.from_user, "id", None),
                chat_id=message.chat.id,
                files=downloaded_files,
                target_dir=target_dir,
            )
            if token:
                summary_lines.append(f"File terdeteksi: `{len(downloaded_files)}`")
                summary_lines.append("")
                summary_lines.append(
                    "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox."
                )
                summary_lines.append(
                    f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`."
                )
                if len(downloaded_files) <= 5:
                    summary_lines.append("")
                    summary_lines.append("Daftar file:")
                    summary_lines.extend(f"- `{item.name}`" for item in downloaded_files)
                await update_status_message(
                    message,
                    status_message,
                    trim_output("\n".join(summary_lines)),
                    reply_markup=build_aria2_upload_keyboard(token),
                )
                return
        summary_lines.append("File baru tidak terdeteksi otomatis untuk tombol upload.")
        if stdout_tail:
            summary_lines.append("")
            summary_lines.append("Ringkasan aria2 (stdout):")
            summary_lines.append("```text")
            summary_lines.append(stdout_tail)
            summary_lines.append("```")
        await update_status_message(message, status_message, trim_output("\n".join(summary_lines)))
        return

    failed_lines = [
        "Aria2 gagal.",
        f"Target folder: `{target_dir}`",
        f"Durasi: `{elapsed_text}`",
        f"Return code: `{process.returncode}`",
    ]
    if stderr_tail:
        failed_lines.append("")
        failed_lines.append("Error aria2 (stderr):")
        failed_lines.append("```text")
        failed_lines.append(stderr_tail)
        failed_lines.append("```")
    elif stdout_tail:
        failed_lines.append("")
        failed_lines.append("Output aria2 (stdout):")
        failed_lines.append("```text")
        failed_lines.append(stdout_tail)
        failed_lines.append("```")
    await update_status_message(message, status_message, trim_output("\n".join(failed_lines)))


@app.on_callback_query(filters.regex(r"^a2up\|"))
async def aria2_upload_choice_callback(client: Client, callback_query):
    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, action = parts
    cleanup_expired_aria2_upload_jobs()
    job_payload = ARIA2_UPLOAD_JOBS.get(token)
    if not job_payload:
        await callback_query.answer("Tombol upload sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = job_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /aria2.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    source_paths = resolve_aria2_job_files(job_payload)
    if not source_paths:
        ARIA2_UPLOAD_JOBS.pop(token, None)
        await callback_query.answer("File hasil aria2 tidak ditemukan lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Aksi upload dibatalkan: file hasil aria2 sudah tidak tersedia.",
            reply_markup=None,
        )
        return

    if action == "skip":
        ARIA2_UPLOAD_JOBS.pop(token, None)
        await callback_query.answer("Upload lanjutan dilewati.")
        base_text = (status_message.text or "").strip()
        final_text = (
            f"{base_text}\n\nUpload lanjutan: dilewati."
            if base_text
            else "Upload lanjutan: dilewati."
        )
        await update_status_message(
            status_message,
            status_message,
            trim_output(final_text),
            reply_markup=None,
        )
        return

    if action == "retry":
        last_action = str(job_payload.get("last_action") or "").strip()
        if last_action not in {"tg", "gd", "tb"}:
            await callback_query.answer(
                "Belum ada aksi upload sebelumnya untuk di-retry.",
                show_alert=True,
            )
            return
        action = last_action

    if action == "tg":
        running_task = UPLOAD_CONTROL.get("task")
        if running_task and not running_task.done():
            await callback_query.answer(
                "Masih ada upload Telegram aktif. Gunakan /ucancel dulu.",
                show_alert=True,
            )
            return

        await callback_query.answer("Memulai upload ke Telegram...")

        job_payload["last_action"] = "tg"
        cancel_event = asyncio.Event()
        UPLOAD_CONTROL["task"] = asyncio.current_task()
        UPLOAD_CONTROL["cancel_event"] = cancel_event

        target_chat = job_payload.get("chat_id", status_message.chat.id)
        target = target_label(target_chat)
        try:
            status_message = await update_status_message(
                status_message,
                status_message,
                "Memulai upload hasil aria2 ke Telegram...\n"
                f"Total file: `{len(source_paths)}`\n"
                f"Tujuan: `{target}`",
                reply_markup=None,
            )
            (
                status_message,
                success_lines,
                failed_lines,
                cancelled,
                failed_paths,
            ) = await upload_files_to_telegram_target(
                client=client,
                command_message=status_message,
                status_message=status_message,
                source_paths=source_paths,
                target_chat=target_chat,
                cancel_event=cancel_event,
            )
            summary_title = (
                "Upload hasil aria2 ke Telegram dibatalkan."
                if cancelled
                else "Upload hasil aria2 ke Telegram selesai."
            )
            summary_text = build_upload_summary(
                summary_title=summary_title,
                target_text=target,
                total_files=len(source_paths),
                success_lines=success_lines,
                failed_lines=failed_lines,
            )
            failed_paths = normalize_existing_file_paths(failed_paths)
            if failed_paths:
                job_payload["files"] = [str(path) for path in failed_paths]
                job_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
                summary_text = trim_output(
                    summary_text
                    + "\n\nRetry tersedia untuk file gagal.\n"
                    "Klik `Retry Terakhir` atau pilih tujuan upload lagi."
                )
                reply_markup = build_aria2_upload_keyboard(token, include_retry=True)
            else:
                ARIA2_UPLOAD_JOBS.pop(token, None)
                reply_markup = None
            await update_status_message(
                status_message,
                status_message,
                summary_text,
                reply_markup=reply_markup,
            )
        finally:
            current_task = asyncio.current_task()
            if UPLOAD_CONTROL.get("task") is current_task:
                UPLOAD_CONTROL["task"] = None
                UPLOAD_CONTROL["cancel_event"] = None
        return

    if action in {"gd", "tb"}:
        if action == "gd":
            remote_base = RCLONE_GDRIVE_REMOTE
            remote_label = "Google Drive"
        else:
            remote_base = RCLONE_TERABOX_REMOTE
            remote_label = "Terabox"

        if not remote_base:
            await callback_query.answer(
                f"Remote rclone {remote_label} belum diatur di env.",
                show_alert=True,
            )
            return

        await callback_query.answer(f"Memulai upload rclone {remote_label}...")

        job_payload["last_action"] = action
        status_message = await update_status_message(
            status_message,
            status_message,
            "Memulai upload hasil aria2 via rclone...\n"
            f"Total file: `{len(source_paths)}`\n"
            f"Remote: `{remote_base}`",
            reply_markup=None,
        )
        status_message, success_lines, failed_lines, failed_paths = await upload_files_via_rclone(
            command_message=status_message,
            status_message=status_message,
            source_paths=source_paths,
            remote_base=remote_base,
            remote_label=remote_label,
        )
        summary_text = build_upload_summary(
            summary_title=f"Upload hasil aria2 via rclone {remote_label} selesai.",
            target_text=remote_base,
            total_files=len(source_paths),
            success_lines=success_lines,
            failed_lines=failed_lines,
        )
        failed_paths = normalize_existing_file_paths(failed_paths)
        if failed_paths:
            job_payload["files"] = [str(path) for path in failed_paths]
            job_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
            summary_text = trim_output(
                summary_text
                + "\n\nRetry tersedia untuk file gagal.\n"
                "Klik `Retry Terakhir` atau pilih tujuan upload lagi."
            )
            reply_markup = build_aria2_upload_keyboard(token, include_retry=True)
        else:
            ARIA2_UPLOAD_JOBS.pop(token, None)
            reply_markup = None
        await update_status_message(
            status_message,
            status_message,
            summary_text,
            reply_markup=reply_markup,
        )
        return

    await callback_query.answer("Aksi tombol tidak dikenal.", show_alert=True)


@app.on_message(command_filter("u1"))
async def upload_command(client: Client, message):
    if not await require_private_command_access(message, "u1"):
        return

    running_task = UPLOAD_CONTROL.get("task")
    if running_task and not running_task.done():
        await open_status_message(
            message,
            "Masih ada upload aktif.\n"
            "Gunakan `/ucancel` untuk membatalkan upload yang sedang berjalan."
        )
        return

    args = command_args(message)
    source_path_text = ""
    target_chat = "me"

    if "--to" in args:
        idx = args.index("--to")
        source_path_text = " ".join(args[:idx]).strip()
        target_text = " ".join(args[idx + 1 :]).strip()
        if not target_text:
            await open_status_message(
                message,
                "Format target tidak valid.\n"
                "Contoh: `/u1 /home/runner/uploads/file.mp4 --to @username`"
            )
            return
        target_chat = parse_chat_target(target_text)
    else:
        source_path_text = " ".join(args).strip()

    if not source_path_text and message.reply_to_message:
        source_path_text = (
            message.reply_to_message.text or message.reply_to_message.caption or ""
        ).strip()

    if not source_path_text:
        await open_status_message(
            message,
            "Format upload:\n"
            "`/u1 /home/runner/uploads/file.mp4`\n"
            "`/u1 *.txt`\n"
            "`/u1 /home/runner/uploads/*.mp4 --to @username`\n"
            "Atau reply pesan berisi path file lalu kirim `/u1`."
        )
        return

    source_paths, resolve_error = resolve_upload_sources(source_path_text)
    if resolve_error:
        await open_status_message(message, resolve_error)
        return

    cancel_event = asyncio.Event()
    UPLOAD_CONTROL["task"] = asyncio.current_task()
    UPLOAD_CONTROL["cancel_event"] = cancel_event

    if target_chat == "me":
        target_chat = message.chat.id

    target = target_label(target_chat)
    total_files = len(source_paths)

    status_message = await open_status_message(
        message,
        "Memulai upload dari storage VPS\n"
        f"Input: `{source_path_text}`\n"
        f"Ditemukan: `{total_files}` file\n"
        f"Default folder upload: `{upload_root}`\n"
        f"Tujuan: `{target}`"
    )

    success_lines = []
    failed_lines = []
    cancelled = False

    for index, source_path in enumerate(source_paths, start=1):
        if cancel_event.is_set():
            cancelled = True
            break

        media_name = source_path.name
        state = {"started_at": time.time(), "last_tick": 0.0, "cancel_event": cancel_event}

        status_message = await update_status_message(
            message,
            status_message,
            "Upload berjalan\n"
            f"File: `{index}/{total_files}`\n"
            f"Nama: `{media_name}`\n"
            f"Sumber: `{source_path}`\n"
            f"Tujuan: `{target}`"
        )

        try:
            suffix = source_path.suffix.lower()
            if suffix in VIDEO_EXTENSIONS:
                video_kwargs = {
                    "chat_id": target_chat,
                    "video": str(source_path),
                    "caption": f"`{media_name}`",
                    "supports_streaming": True,
                    "progress": upload_progress_callback,
                    "progress_args": (status_message, media_name, state),
                }
                video_metadata = await probe_video_metadata(source_path)
                for key in ("duration", "width", "height"):
                    value = video_metadata.get(key)
                    if value:
                        video_kwargs[key] = value

                if "duration" not in video_kwargs:
                    print(
                        f"[WARN] Durasi video tidak terdeteksi untuk {source_path}. "
                        "Telegram bisa menampilkan durasi 0:00."
                    )

                thumb_path = await generate_video_thumbnail(source_path)
                if thumb_path:
                    video_kwargs["thumb"] = str(thumb_path)
                else:
                    print(
                        f"[WARN] Thumbnail tidak berhasil dibuat untuk {source_path.name}. "
                        "Telegram mungkin menampilkan preview kosong."
                    )

                try:
                    sent_message = await client.send_video(**video_kwargs)
                finally:
                    if thumb_path:
                        remove_file_quietly(thumb_path)
            else:
                sent_message = await client.send_document(
                    chat_id=target_chat,
                    document=str(source_path),
                    caption=f"`{media_name}`",
                    progress=upload_progress_callback,
                    progress_args=(status_message, media_name, state),
                )
        except asyncio.CancelledError:
            cancel_event.set()
            cancelled = True
            failed_lines.append(f"- `{media_name}` -> dibatalkan")
            break
        except Exception as e:
            failed_lines.append(f"- `{media_name}` -> {e}")
            continue

        if sent_message.chat and sent_message.chat.username:
            ref = f"https://t.me/{sent_message.chat.username}/{sent_message.id}"
        else:
            ref = f"{sent_message.chat.id}/{sent_message.id}"
        delete_error = remove_local_file_after_upload(source_path)
        if delete_error:
            success_lines.append(
                f"- `{media_name}` -> `{ref}` (upload OK, hapus lokal gagal: `{delete_error}`)"
            )
        else:
            success_lines.append(f"- `{media_name}` -> `{ref}` (lokal dihapus)")

    if cancel_event.is_set():
        cancelled = True

    summary_title = "Upload dibatalkan." if cancelled else "Upload selesai."
    summary_lines = [
        summary_title,
        f"Tujuan: `{target}`",
        f"Total file: `{total_files}`",
        f"Berhasil: `{len(success_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]

    if success_lines:
        summary_lines.append("")
        summary_lines.append("Daftar berhasil:")
        summary_lines.extend(success_lines[:20])
        if len(success_lines) > 20:
            summary_lines.append(f"... {len(success_lines) - 20} file lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Daftar gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} file lain.")

    try:
        await update_status_message(message, status_message, trim_output("\n".join(summary_lines)))
    finally:
        current_task = asyncio.current_task()
        if UPLOAD_CONTROL.get("task") is current_task:
            UPLOAD_CONTROL["task"] = None
            UPLOAD_CONTROL["cancel_event"] = None


@app.on_message(command_filter("ucancel"))
async def cancel_upload_command(client: Client, message):
    del client

    if not await require_private_command_access(message, "ucancel"):
        return

    running_task = UPLOAD_CONTROL.get("task")
    cancel_event = UPLOAD_CONTROL.get("cancel_event")

    if not running_task or running_task.done() or not cancel_event:
        UPLOAD_CONTROL["task"] = None
        UPLOAD_CONTROL["cancel_event"] = None
        await open_status_message(message, "Tidak ada upload yang sedang berjalan.")
        return

    cancel_event.set()
    await open_status_message(
        message,
        "Permintaan cancel diterima.\n"
        "Menunggu proses upload berhenti..."
    )


@app.on_message(command_filter("ls"))
async def list_command(client: Client, message):
    del client

    if not await require_private_command_access(message, "ls"):
        return

    args = command_args(message)
    show_all = False
    path_parts = []
    for arg in args:
        if arg == "-a":
            show_all = True
        else:
            path_parts.append(arg)

    path_text = " ".join(path_parts).strip() or "."
    try:
        target_path = local_path_from_text(path_text)
    except Exception as e:
        await open_status_message(message, f"Path tidak valid: `{e}`")
        return

    if not target_path.exists():
        await open_status_message(message, f"Path tidak ditemukan:\n`{target_path}`")
        return

    if target_path.is_file():
        output = (
            f"Path: `{target_path}`\n"
            "Type: file\n\n"
            "```text\n"
            f"{format_entry_line(target_path)}\n"
            "```"
        )
        await open_status_message(message, trim_output(output))
        return

    try:
        lines, total_entries, truncated = list_directory_lines(target_path, show_all)
    except PermissionError:
        await open_status_message(message, f"Akses ditolak:\n`{target_path}`")
        return
    except Exception as e:
        await open_status_message(message, f"Gagal membaca direktori: `{e}`")
        return

    if not lines:
        output = (
            f"Path: `{target_path}`\n"
            f"Total: `{total_entries}`\n\n"
            "(kosong)"
        )
        await open_status_message(message, output)
        return

    note = ""
    if truncated:
        note = f"\nCatatan: ditampilkan {LIST_MAX_ENTRIES} dari {total_entries} item."

    output = (
        f"Path: `{target_path}`\n"
        f"Total: `{total_entries}`{note}\n\n"
        "```text\n"
        + "\n".join(lines)
        + "\n```"
    )
    await open_status_message(message, trim_output(output))


@app.on_message(command_filter("du", allow_public=True))
async def disk_usage_command(client: Client, message):
    del client

    if not await require_public_command_access(message, "du"):
        return

    args = command_args(message)
    path_text, parse_error = parse_disk_command_target("du", args)
    if parse_error:
        await open_status_message(message, parse_error)
        return

    try:
        target_path = local_path_from_text(path_text)
    except Exception as e:
        await open_status_message(message, f"Path tidak valid: `{e}`")
        return

    if not path_exists_or_symlink(target_path):
        await open_status_message(message, f"Path tidak ditemukan:\n`{target_path}`")
        return

    total_bytes, file_count, dir_count, error_count = compute_path_usage(target_path)

    summary_lines = [
        "du selesai.",
        f"Path: `{target_path}`",
        f"Total ukuran: `{format_bytes(float(total_bytes))}`",
        f"Jumlah file: `{file_count}`",
        f"Jumlah folder: `{dir_count}`",
    ]
    if error_count:
        summary_lines.append(f"Catatan: `{error_count}` item gagal diakses.")

    await open_status_message(message, "\n".join(summary_lines))


@app.on_message(command_filter("df", allow_public=True))
async def disk_free_command(client: Client, message):
    del client

    if not await require_public_command_access(message, "df"):
        return

    args = command_args(message)
    path_text, parse_error = parse_disk_command_target("df", args)
    if parse_error:
        await open_status_message(message, parse_error)
        return

    try:
        target_path = local_path_from_text(path_text)
    except Exception as e:
        await open_status_message(message, f"Path tidak valid: `{e}`")
        return

    if not path_exists_or_symlink(target_path):
        await open_status_message(message, f"Path tidak ditemukan:\n`{target_path}`")
        return

    try:
        usage = shutil.disk_usage(str(target_path))
    except Exception as e:
        await open_status_message(message, f"Gagal membaca disk usage: `{e}`")
        return

    total_bytes = usage.total
    used_bytes = usage.used
    free_bytes = usage.free
    used_percent = (used_bytes * 100 / total_bytes) if total_bytes > 0 else 0.0

    summary = (
        "df selesai.\n"
        f"Path: `{target_path}`\n"
        f"Total: `{format_bytes(float(total_bytes))}`\n"
        f"Terpakai: `{format_bytes(float(used_bytes))}` ({used_percent:.2f}%)\n"
        f"Sisa: `{format_bytes(float(free_bytes))}`"
    )
    await open_status_message(message, summary)


@app.on_message(command_filter("mkdir"))
async def mkdir_command(client: Client, message):
    del client

    if not await require_private_command_access(message, "mkdir"):
        return

    args = command_args(message)
    if not args:
        await open_status_message(
            message,
            "Format:\n"
            "`/mkdir /home/runner/new-folder`\n"
            "`/mkdir \"./folder dengan spasi\"`"
        )
        return

    created_lines = []
    existed_lines = []
    failed_lines = []

    for raw_path in args:
        try:
            target_path = local_path_from_text(raw_path)
        except Exception as e:
            failed_lines.append(f"- `{raw_path}` -> path tidak valid: {e}")
            continue

        if target_path.exists():
            if target_path.is_dir():
                existed_lines.append(f"- `{target_path}`")
            else:
                failed_lines.append(f"- `{target_path}` -> sudah ada sebagai file")
            continue

        try:
            target_path.mkdir(parents=True, exist_ok=False)
            created_lines.append(f"- `{target_path}`")
        except Exception as e:
            failed_lines.append(f"- `{target_path}` -> {e}")

    summary_lines = [
        "mkdir selesai.",
        f"Dibuat: `{len(created_lines)}`",
        f"Sudah ada: `{len(existed_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]

    if created_lines:
        summary_lines.append("")
        summary_lines.append("Folder dibuat:")
        summary_lines.extend(created_lines[:20])
        if len(created_lines) > 20:
            summary_lines.append(f"... {len(created_lines) - 20} folder lain.")

    if existed_lines:
        summary_lines.append("")
        summary_lines.append("Folder sudah ada:")
        summary_lines.extend(existed_lines[:20])
        if len(existed_lines) > 20:
            summary_lines.append(f"... {len(existed_lines) - 20} folder lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} item lain.")

    await open_status_message(message, trim_output("\n".join(summary_lines)))


@app.on_message(command_filter("rm"))
async def remove_command(client: Client, message):
    del client

    if not await require_private_command_access(message, "rm"):
        return

    args = command_args(message)
    if not args:
        await open_status_message(
            message,
            "Format:\n"
            "`/rm /home/runner/uploads/file.txt`\n"
            "`/rm /home/runner/uploads/tmp-folder`\n"
            "`/rm *.tmp`"
        )
        return

    removed_file_lines = []
    removed_dir_lines = []
    failed_lines = []

    for raw_path in args:
        targets, resolve_error = resolve_path_candidates(raw_path)
        if resolve_error:
            failed_lines.append(f"- `{raw_path}` -> {resolve_error}")
            continue

        for target_path in targets:
            if is_root_path(target_path):
                failed_lines.append(f"- `{target_path}` -> menolak hapus root path")
                continue

            if not path_exists_or_symlink(target_path):
                failed_lines.append(f"- `{target_path}` -> tidak ditemukan")
                continue

            try:
                if target_path.is_dir() and not target_path.is_symlink():
                    shutil.rmtree(target_path)
                    removed_dir_lines.append(f"- `{target_path}`")
                else:
                    target_path.unlink()
                    removed_file_lines.append(f"- `{target_path}`")
            except Exception as e:
                failed_lines.append(f"- `{target_path}` -> {e}")

    summary_lines = [
        "rm selesai.",
        f"File dihapus: `{len(removed_file_lines)}`",
        f"Folder dihapus: `{len(removed_dir_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]

    if removed_file_lines:
        summary_lines.append("")
        summary_lines.append("File dihapus:")
        summary_lines.extend(removed_file_lines[:20])
        if len(removed_file_lines) > 20:
            summary_lines.append(f"... {len(removed_file_lines) - 20} file lain.")

    if removed_dir_lines:
        summary_lines.append("")
        summary_lines.append("Folder dihapus:")
        summary_lines.extend(removed_dir_lines[:20])
        if len(removed_dir_lines) > 20:
            summary_lines.append(f"... {len(removed_dir_lines) - 20} folder lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} item lain.")

    await open_status_message(message, trim_output("\n".join(summary_lines)))


@app.on_message(command_filter("copy"))
async def copy_command(client: Client, message):
    del client

    if not await require_private_command_access(message, "copy"):
        return

    args = command_args(message)
    if len(args) != 2:
        await open_status_message(
            message,
            "Format:\n"
            "`/copy /home/runner/uploads/a.txt /home/runner/backup/a.txt`\n"
            "`/copy *.mp4 /home/runner/backup/videos/`\n"
            "Catatan: gunakan tanda kutip jika path mengandung spasi."
        )
        return

    source_text, destination_text = args
    source_paths, source_error = resolve_path_candidates(source_text)
    if source_error:
        await open_status_message(message, source_error)
        return

    existing_sources = [item for item in source_paths if path_exists_or_symlink(item)]
    if not existing_sources:
        await open_status_message(message, f"Source tidak ditemukan:\n`{source_text}`")
        return

    try:
        destination_path = local_path_from_text(destination_text)
    except Exception as e:
        await open_status_message(message, f"Path tujuan tidak valid: `{e}`")
        return

    if len(existing_sources) > 1 and (not destination_path.exists() or not destination_path.is_dir()):
        await open_status_message(
            message,
            "Jika source lebih dari satu, tujuan wajib folder yang sudah ada.\n"
            f"Tujuan: `{destination_path}`"
        )
        return

    success_lines = []
    failed_lines = []

    for source_path in existing_sources:
        if destination_path.exists() and destination_path.is_dir():
            target_path = destination_path / source_path.name
        else:
            target_path = destination_path

        if path_exists_or_symlink(target_path):
            failed_lines.append(f"- `{source_path}` -> target sudah ada: `{target_path}`")
            continue

        if not target_path.parent.exists():
            failed_lines.append(
                f"- `{source_path}` -> folder tujuan tidak ditemukan: `{target_path.parent}`"
            )
            continue

        try:
            if source_path.is_dir() and not source_path.is_symlink():
                shutil.copytree(source_path, target_path)
            else:
                shutil.copy2(source_path, target_path)
            success_lines.append(f"- `{source_path}` -> `{target_path}`")
        except Exception as e:
            failed_lines.append(f"- `{source_path}` -> {e}")

    summary_lines = [
        "copy selesai.",
        f"Berhasil: `{len(success_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]

    if success_lines:
        summary_lines.append("")
        summary_lines.append("Daftar berhasil:")
        summary_lines.extend(success_lines[:20])
        if len(success_lines) > 20:
            summary_lines.append(f"... {len(success_lines) - 20} item lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Daftar gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} item lain.")

    await open_status_message(message, trim_output("\n".join(summary_lines)))


@app.on_message(command_filter("mv"))
async def move_command(client: Client, message):
    del client

    if not await require_private_command_access(message, "mv"):
        return

    args = command_args(message)
    if len(args) != 2:
        await open_status_message(
            message,
            "Format:\n"
            "`/mv /home/runner/uploads/a.txt /home/runner/archive/a.txt`\n"
            "`/mv *.log /home/runner/archive/`\n"
            "Catatan: gunakan tanda kutip jika path mengandung spasi."
        )
        return

    source_text, destination_text = args
    source_paths, source_error = resolve_path_candidates(source_text)
    if source_error:
        await open_status_message(message, source_error)
        return

    existing_sources = [item for item in source_paths if path_exists_or_symlink(item)]
    if not existing_sources:
        await open_status_message(message, f"Source tidak ditemukan:\n`{source_text}`")
        return

    try:
        destination_path = local_path_from_text(destination_text)
    except Exception as e:
        await open_status_message(message, f"Path tujuan tidak valid: `{e}`")
        return

    if len(existing_sources) > 1 and (not destination_path.exists() or not destination_path.is_dir()):
        await open_status_message(
            message,
            "Jika source lebih dari satu, tujuan wajib folder yang sudah ada.\n"
            f"Tujuan: `{destination_path}`"
        )
        return

    success_lines = []
    failed_lines = []

    for source_path in existing_sources:
        if destination_path.exists() and destination_path.is_dir():
            target_path = destination_path / source_path.name
        else:
            target_path = destination_path

        if source_path == target_path:
            failed_lines.append(f"- `{source_path}` -> source dan target sama")
            continue

        if path_exists_or_symlink(target_path):
            failed_lines.append(f"- `{source_path}` -> target sudah ada: `{target_path}`")
            continue

        if not target_path.parent.exists():
            failed_lines.append(
                f"- `{source_path}` -> folder tujuan tidak ditemukan: `{target_path.parent}`"
            )
            continue

        try:
            shutil.move(str(source_path), str(target_path))
            success_lines.append(f"- `{source_path}` -> `{target_path}`")
        except Exception as e:
            failed_lines.append(f"- `{source_path}` -> {e}")

    summary_lines = [
        "mv selesai.",
        f"Berhasil: `{len(success_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]

    if success_lines:
        summary_lines.append("")
        summary_lines.append("Daftar berhasil:")
        summary_lines.extend(success_lines[:20])
        if len(success_lines) > 20:
            summary_lines.append(f"... {len(success_lines) - 20} item lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Daftar gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} item lain.")

    await open_status_message(message, trim_output("\n".join(summary_lines)))


if __name__ == "__main__":
    runtime_mode = "BOT TOKEN" if BOT_MODE else "USERBOT SESSION"
    print(f"Manual downloader aktif. Mode: {runtime_mode}")
    if BOT_MODE:
        print(f"Owner ID: {OWNER_USER_ID if OWNER_USER_ID else '(belum diatur)'}")
    print(f"Mode command: {'PUBLIC' if PUBLIC_MODE else 'PRIVATE'}")
    print("Langkah pakai:")
    if BOT_MODE and PUBLIC_MODE:
        print("1. Di chat/group, semua member bisa pakai: /d1 /dstatus /dqueue /du /df /aria2 (/a2).")
        print("2. Untuk /d1: balas file/video ATAU link t.me lalu kirim /d1.")
        print("3. Cek antrian download: /dstatus atau /dqueue.")
        print("4. Command lain hanya owner (OWNER_USER_ID): /u1 /ucancel /ls /mkdir /rm /copy /mv.")
    elif BOT_MODE:
        print("1. Semua command hanya owner (OWNER_USER_ID).")
        print("2. Public command nonaktif. Aktifkan PUBLIC_MODE=1 untuk membuka /d1 /dstatus /dqueue /du /df /aria2.")
    elif PUBLIC_MODE:
        print("1. Di chat/group, semua member bisa pakai: /d1 /dstatus /dqueue /du /df /aria2 (/a2).")
        print("2. Untuk /d1: balas file/video ATAU link t.me lalu kirim /d1.")
        print("3. Cek antrian download: /dstatus atau /dqueue.")
        print("4. Command lain tetap khusus Saved Messages (owner): /u1 /ucancel /ls /mkdir /rm /copy /mv.")
    else:
        print("1. Forward file/video ATAU kirim link t.me ke Saved Messages.")
        print("2. Reply pesan tersebut dengan /d1.")
        print("3. Cek antrian download: /dstatus atau /dqueue.")
        print("4. Upload file lokal: /u1 /path/file, /u1 *.txt, atau /u1 /path/*.mp4 --to @username")
    print("- Download external via aria2: /aria2 <url|magnet> (default folder DOWNLOAD_DIR)")
    print("- /aria2 juga bisa dipakai sambil reply file/link Telegram")
    print("- Setelah /aria2 selesai, bot menampilkan tombol upload: Telegram / rclone GDrive / rclone Terabox")
    print("- Jika upload gagal, gunakan tombol `Retry Terakhir` atau pilih tujuan upload lagi")
    print(
        f"- Remote rclone: GDrive=`{RCLONE_GDRIVE_REMOTE or '(belum diatur)'}`, "
        f"Terabox=`{RCLONE_TERABOX_REMOTE or '(belum diatur)'}`"
    )
    print("- Cek isi direktori: /ls /path  (opsional: /ls -a /path)")
    print("- Cek disk: /du [-h] [path] dan /df [-h] [path]")
    print("- Batalkan upload yang sedang berjalan: /ucancel")
    print("- Manajemen file: /mkdir <path>, /rm <path>, /copy <source> <target>, /mv <source> <target>")
    print(f"- File download disimpan ke: {download_root}")
    print(f"- Default folder upload: {upload_root}")
    app.run()
