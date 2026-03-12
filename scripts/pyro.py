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
PKILL_ADMIN_IDS_RAW = os.getenv("PKILL_ADMIN_IDS", "").strip()
ARIA2_BIN = os.getenv("ARIA2_BIN", "aria2c")
GALLERY_DL_BIN = os.getenv("GALLERY_DL_BIN", "gallery-dl")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/home/runner/downloads")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/home/runner/downloads")
EXTRACT_DIR = os.getenv("EXTRACT_DIR", "/home/runner/downloads")
RCLONE_BIN = os.getenv("RCLONE_BIN", "rclone")
RCLONE_GDRIVE_REMOTE = os.getenv("RCLONE_GDRIVE_REMOTE", "").strip()
RCLONE_TERABOX_REMOTE = os.getenv("RCLONE_TERABOX_REMOTE", "terabox:Mirror").strip()
RCLONE_DROPBOX_REMOTE = os.getenv("RCLONE_DROPBOX_REMOTE", "").strip()
PROGRESS_INTERVAL = int(os.getenv("PROGRESS_INTERVAL", "5"))
try:
    RCLONE_COMMAND_TIMEOUT_SECONDS = max(
        30, int(os.getenv("RCLONE_COMMAND_TIMEOUT_SECONDS", "900"))
    )
except ValueError:
    RCLONE_COMMAND_TIMEOUT_SECONDS = 900
try:
    EXTRACT_COMMAND_TIMEOUT_SECONDS = max(
        30, int(os.getenv("EXTRACT_COMMAND_TIMEOUT_SECONDS", "900"))
    )
except ValueError:
    EXTRACT_COMMAND_TIMEOUT_SECONDS = 900
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

PKILL_ADMIN_USER_IDS = set()
if PKILL_ADMIN_IDS_RAW:
    for raw_id in re.split(r"[,\s;]+", PKILL_ADMIN_IDS_RAW):
        cleaned = raw_id.strip()
        if not cleaned:
            continue
        if not cleaned.lstrip("-").isdigit():
            raise RuntimeError(
                f"PKILL_ADMIN_IDS tidak valid: `{cleaned}`. Isi dengan user ID angka, pisahkan koma/spasi."
            )
        PKILL_ADMIN_USER_IDS.add(int(cleaned))

BOT_MODE = bool(BOT_TOKEN)

api_id = int(API_ID)
download_root = Path(DOWNLOAD_DIR).expanduser().resolve()
download_root.mkdir(parents=True, exist_ok=True)
download_target = f"{download_root}{os.sep}"
upload_root = Path(UPLOAD_DIR).expanduser().resolve()
upload_root.mkdir(parents=True, exist_ok=True)
extract_root = Path(EXTRACT_DIR).expanduser().resolve()
extract_root.mkdir(parents=True, exist_ok=True)

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

EXTERNAL_LINK_RE = re.compile(
    r"(magnet:\?[^\s]+|https?://[^\s<>\"'`]+|ftp://[^\s<>\"'`]+)",
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
RCLONE_PAGE_BODY_CHARS = max(500, LIST_MAX_CHARS - 1000)
U1_PICKER_PAGE_SIZE = 20
U1_PICKER_BUTTONS_PER_ROW = 5
EXTRACT_PICKER_PAGE_SIZE = 10
EXTRACT_PICKER_BUTTONS_PER_ROW = 5
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
ARIA2_PENDING_UPLOAD_CHOICES: Dict[str, Dict[str, object]] = {}
U1_FILE_PICK_SESSIONS: Dict[str, Dict[str, object]] = {}
U1_FOLDER_MODE_SESSIONS: Dict[str, Dict[str, object]] = {}
RCLONE_OUTPUT_SESSIONS: Dict[str, Dict[str, object]] = {}
EXTRACT_PICK_SESSIONS: Dict[str, Dict[str, object]] = {}
EXTRACT_FILE_PICK_SESSIONS: Dict[str, Dict[str, object]] = {}
EXTRACT_DELETE_CONFIRM_SESSIONS: Dict[str, Dict[str, object]] = {}

EXTRACT_MODES = ("unrar", "unzip", "untar", "un7z")


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


def extract_non_telegram_links(text: Optional[str]) -> List[str]:
    if not text:
        return []

    found: List[str] = []
    seen = set()
    for raw_link in EXTERNAL_LINK_RE.findall(text):
        link = raw_link.strip().rstrip(".,;:!?)\"'`]>")
        if not link:
            continue
        if parse_telegram_link(link):
            continue
        key = link.strip()
        if key in seen:
            continue
        seen.add(key)
        found.append(key)
    return found


def is_direct_url(value: str) -> bool:
    if not value:
        return False
    cleaned = value.strip()
    return cleaned.lower().startswith(("http://", "https://", "ftp://"))


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
    del command_name

    options_ended = False
    path_parts = []
    for arg in args:
        if not options_ended and arg == "--":
            options_ended = True
            continue
        if not options_ended and arg.startswith("-"):
            # Kompatibilitas: terima opsi apa pun seperti command shell, lalu abaikan.
            # Contoh: /du -h, /df --si, dst.
            continue
        path_parts.append(arg)
    return " ".join(path_parts).strip() or ".", None


def parse_aria2_command_args(
    args: List[str],
    fallback_urls: Optional[List[str]] = None,
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
                "Atau reply link direct (http/https/ftp/magnet) lalu kirim `/aria2`."
            )
        else:
            urls.append(arg)

        i += 1

    if not urls and fallback_urls:
        urls.extend(item.strip() for item in fallback_urls if item and item.strip())

    if not urls:
        return None, None, None, (
            "Format aria2:\n"
            "`/aria2 <url|magnet>`\n"
            "`/aria2 <url> --out nama_file.ext`\n"
            "`/aria2 <url|magnet> --dir /home/runner/downloads/`\n"
            "`/aria2` sambil reply link direct (http/https/ftp/magnet)\n"
            "Alias: `/a2`"
        )

    if output_name and len(urls) > 1:
        return None, None, None, "--out hanya boleh dipakai jika URL sumber satu."

    return urls, output_name, target_dir, None


def parse_gallery_dl_command_args(
    args: List[str],
    fallback_urls: Optional[List[str]] = None,
) -> Tuple[Optional[List[str]], Optional[Path], Optional[List[str]], Optional[str]]:
    def is_directory_option(option_text: str) -> bool:
        key, _, _ = option_text.partition("=")
        return key.strip().lower() == "directory"

    command_args: List[str] = []
    urls: List[str] = []
    target_dir: Path = download_root
    dest_explicit = False
    directory_option_explicit = False

    i = 0
    while i < len(args):
        arg = args[i]

        if arg in {"-d", "--dest"}:
            i += 1
            if i >= len(args):
                return None, None, None, "Nilai -d/--dest tidak boleh kosong."
            raw_path = args[i].strip()
            if not raw_path:
                return None, None, None, "Nilai -d/--dest tidak boleh kosong."
            try:
                target_dir = local_path_from_text(raw_path)
            except Exception as e:
                return None, None, None, f"Path -d/--dest tidak valid: `{e}`"
            command_args.extend([arg, str(target_dir)])
            dest_explicit = True
        elif arg.startswith("--dest="):
            raw_path = arg.split("=", 1)[1].strip()
            if not raw_path:
                return None, None, None, "Nilai --dest tidak boleh kosong."
            try:
                target_dir = local_path_from_text(raw_path)
            except Exception as e:
                return None, None, None, f"Path --dest tidak valid: `{e}`"
            command_args.append(f"--dest={target_dir}")
            dest_explicit = True
        elif arg in {"-o", "--option"}:
            i += 1
            if i >= len(args):
                return None, None, None, "Nilai -o/--option tidak boleh kosong."
            option_text = args[i].strip()
            if not option_text:
                return None, None, None, "Nilai -o/--option tidak boleh kosong."
            if is_directory_option(option_text):
                directory_option_explicit = True
            command_args.extend([arg, option_text])
        elif arg.startswith("--option="):
            option_text = arg.split("=", 1)[1].strip()
            if not option_text:
                return None, None, None, "Nilai --option tidak boleh kosong."
            if is_directory_option(option_text):
                directory_option_explicit = True
            command_args.append(arg)
        else:
            command_args.append(arg)
            if is_direct_url(arg):
                urls.append(arg)

        i += 1

    if not urls and fallback_urls:
        seen = {item.strip() for item in urls}
        for item in fallback_urls:
            cleaned = item.strip()
            if not cleaned or cleaned in seen or not is_direct_url(cleaned):
                continue
            seen.add(cleaned)
            urls.append(cleaned)
            command_args.append(cleaned)

    if not dest_explicit and not directory_option_explicit:
        command_args = ["-d", str(target_dir), "-o", 'directory=""', *command_args]
    elif not dest_explicit:
        command_args = ["-d", str(target_dir), *command_args]
    elif not directory_option_explicit:
        command_args = ["-o", 'directory=""', *command_args]

    if not urls:
        return None, None, None, (
            "Format gallery-dl:\n"
            "`/gdl <url>`\n"
            "`/gdl -d /home/runner/downloads <url>`\n"
            "`/gdl -o directory=\"\" <url>`\n"
            "`/gdl` sambil reply link direct (http/https/ftp)\n"
            "Default otomatis: `-d /home/runner/downloads -o directory=\"\"`\n"
            "Alias kompatibilitas: `/gallerydl`"
        )

    return command_args, target_dir, urls, None


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


async def collect_process_stream(
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
            cleaned = part.strip()
            if not cleaned:
                continue
            progress_state["line"] = cleaned
            progress_state["updated_at"] = time.time()

    leftover = str(progress_state.get(tail_key, "")).strip()
    if leftover:
        progress_state["line"] = leftover
        progress_state["updated_at"] = time.time()
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
            previous = before_snapshot.get(key)

            changed = False
            if previous is None:
                changed = True
            else:
                previous_size, previous_mtime_ns = previous
                if current_size != previous_size or current_mtime_ns > previous_mtime_ns:
                    changed = True

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
    expired_tokens: List[str] = []
    for token, payload in ARIA2_UPLOAD_JOBS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        ARIA2_UPLOAD_JOBS.pop(token, None)

    pending_expired_tokens: List[str] = []
    for token, payload in ARIA2_PENDING_UPLOAD_CHOICES.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            pending_expired_tokens.append(token)
    for token in pending_expired_tokens:
        ARIA2_PENDING_UPLOAD_CHOICES.pop(token, None)

    # Keep other button-based sessions in sync with the same TTL cleanup cycle.
    cleanup_expired_u1_file_pick_sessions()
    cleanup_expired_u1_folder_mode_sessions()
    cleanup_expired_rclone_output_sessions()
    cleanup_expired_extract_pick_sessions()
    cleanup_expired_extract_file_pick_sessions()
    cleanup_expired_extract_delete_confirm_sessions()


def register_pending_upload_choice(
    requester_id: Optional[int],
    chat_id: int,
    source_label: str = "aria2",
) -> str:
    cleanup_expired_aria2_upload_jobs()
    token = secrets.token_hex(4)
    while token in ARIA2_UPLOAD_JOBS or token in ARIA2_PENDING_UPLOAD_CHOICES:
        token = secrets.token_hex(4)

    ARIA2_PENDING_UPLOAD_CHOICES[token] = {
        "requester_id": requester_id,
        "chat_id": chat_id,
        "source_label": source_label,
        "selected_action": None,
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def normalize_existing_paths(paths: List[Path], include_dirs: bool = False) -> List[Path]:
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
        if resolved.exists() and (resolved.is_file() or (include_dirs and resolved.is_dir())):
            seen.add(key)
            normalized.append(resolved)
    return normalized


def normalize_existing_file_paths(paths: List[Path]) -> List[Path]:
    return normalize_existing_paths(paths, include_dirs=False)


def register_aria2_upload_job(
    requester_id: Optional[int],
    requester_name: str,
    chat_id: int,
    files: List[Path],
    target_dir: Optional[Path] = None,
    source_label: str = "aria2",
    token: Optional[str] = None,
) -> Optional[str]:
    valid_files = normalize_existing_file_paths(files)
    if not valid_files:
        return None

    cleanup_expired_aria2_upload_jobs()
    if token and token in ARIA2_UPLOAD_JOBS:
        return None
    resolved_token = token or secrets.token_hex(4)
    while (
        (resolved_token in ARIA2_UPLOAD_JOBS or resolved_token in ARIA2_PENDING_UPLOAD_CHOICES)
        and resolved_token != token
    ):
        resolved_token = secrets.token_hex(4)
    base_dir = target_dir or valid_files[0].parent
    try:
        base_dir_text = str(base_dir.resolve())
    except Exception:
        base_dir_text = str(base_dir)

    ARIA2_UPLOAD_JOBS[resolved_token] = {
        "requester_id": requester_id,
        "requester_name": requester_name.strip(),
        "chat_id": chat_id,
        "files": [str(path) for path in valid_files],
        "target_dir": base_dir_text,
        "source_label": source_label,
        "last_action": None,
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    ARIA2_PENDING_UPLOAD_CHOICES.pop(resolved_token, None)
    return resolved_token


def build_aria2_upload_keyboard(token: str, include_retry: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Upload Telegram", callback_data=f"a2up|{token}|tg"),
        ],
        [
            InlineKeyboardButton("Rclone GDrive", callback_data=f"a2up|{token}|gd"),
            InlineKeyboardButton("Rclone Terabox", callback_data=f"a2up|{token}|tb"),
        ],
        [
            InlineKeyboardButton("Rclone Dropbox", callback_data=f"a2up|{token}|db"),
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


def cleanup_expired_u1_file_pick_sessions() -> None:
    now = time.time()
    expired_tokens: List[str] = []
    for token, payload in U1_FILE_PICK_SESSIONS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        U1_FILE_PICK_SESSIONS.pop(token, None)


def cleanup_expired_u1_folder_mode_sessions() -> None:
    now = time.time()
    expired_tokens: List[str] = []
    for token, payload in U1_FOLDER_MODE_SESSIONS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        U1_FOLDER_MODE_SESSIONS.pop(token, None)


def register_u1_file_pick_session(
    requester_id: Optional[int],
    command_chat_id: int,
    target_chat,
    source_input: str,
    files: List[Path],
) -> Optional[str]:
    valid_files = normalize_existing_paths(files, include_dirs=True)
    if not valid_files:
        return None

    cleanup_expired_u1_file_pick_sessions()
    token = secrets.token_hex(4)
    while (
        token in U1_FILE_PICK_SESSIONS
        or token in U1_FOLDER_MODE_SESSIONS
        or token in ARIA2_UPLOAD_JOBS
        or token in ARIA2_PENDING_UPLOAD_CHOICES
    ):
        token = secrets.token_hex(4)

    U1_FILE_PICK_SESSIONS[token] = {
        "requester_id": requester_id,
        "command_chat_id": command_chat_id,
        "target_chat": target_chat,
        "source_input": source_input.strip(),
        "files": [str(path) for path in valid_files],
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def resolve_u1_file_pick_paths(payload: Dict[str, object]) -> List[Path]:
    files_raw = payload.get("files")
    if not isinstance(files_raw, list):
        return []
    return normalize_existing_paths([Path(str(item)) for item in files_raw], include_dirs=True)


def register_u1_folder_mode_session(
    requester_id: Optional[int],
    target_chat,
    folder_path: Path,
    source_input: str,
) -> Optional[str]:
    if not folder_path.exists() or not folder_path.is_dir():
        return None

    cleanup_expired_u1_folder_mode_sessions()
    token = secrets.token_hex(4)
    while (
        token in U1_FOLDER_MODE_SESSIONS
        or token in U1_FILE_PICK_SESSIONS
        or token in ARIA2_UPLOAD_JOBS
        or token in ARIA2_PENDING_UPLOAD_CHOICES
    ):
        token = secrets.token_hex(4)

    U1_FOLDER_MODE_SESSIONS[token] = {
        "requester_id": requester_id,
        "target_chat": target_chat,
        "folder_path": str(folder_path),
        "source_input": source_input.strip(),
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def split_text_into_pages(text: str, max_chars: int) -> List[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    if not normalized.strip():
        return []

    safe_max = max(200, int(max_chars))
    pages: List[str] = []
    current = ""

    for line in normalized.split("\n"):
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= safe_max:
            current = candidate
            continue

        if current:
            pages.append(current)
            current = ""

        remaining = line
        while len(remaining) > safe_max:
            pages.append(remaining[:safe_max])
            remaining = remaining[safe_max:]
        current = remaining

    if current:
        pages.append(current)

    return pages


def cleanup_expired_rclone_output_sessions() -> None:
    now = time.time()
    expired_tokens: List[str] = []
    for token, payload in RCLONE_OUTPUT_SESSIONS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        RCLONE_OUTPUT_SESSIONS.pop(token, None)


def cleanup_expired_extract_pick_sessions() -> None:
    now = time.time()
    expired_tokens: List[str] = []
    for token, payload in EXTRACT_PICK_SESSIONS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        EXTRACT_PICK_SESSIONS.pop(token, None)


def cleanup_expired_extract_file_pick_sessions() -> None:
    now = time.time()
    expired_tokens: List[str] = []
    for token, payload in EXTRACT_FILE_PICK_SESSIONS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        EXTRACT_FILE_PICK_SESSIONS.pop(token, None)


def cleanup_expired_extract_delete_confirm_sessions() -> None:
    now = time.time()
    expired_tokens: List[str] = []
    for token, payload in EXTRACT_DELETE_CONFIRM_SESSIONS.items():
        expires_at = float(payload.get("expires_at", 0.0) or 0.0)
        if expires_at and expires_at <= now:
            expired_tokens.append(token)
    for token in expired_tokens:
        EXTRACT_DELETE_CONFIRM_SESSIONS.pop(token, None)


def register_extract_pick_session(
    requester_id: Optional[int],
    source_inputs: List[str],
    target_dir: Path,
) -> Optional[str]:
    normalized_inputs = [str(item).strip() for item in source_inputs if str(item).strip()]
    if not normalized_inputs:
        return None

    cleanup_expired_extract_pick_sessions()
    token = secrets.token_hex(4)
    while (
        token in EXTRACT_PICK_SESSIONS
        or token in EXTRACT_FILE_PICK_SESSIONS
        or token in EXTRACT_DELETE_CONFIRM_SESSIONS
        or token in RCLONE_OUTPUT_SESSIONS
        or token in U1_FILE_PICK_SESSIONS
        or token in U1_FOLDER_MODE_SESSIONS
        or token in ARIA2_UPLOAD_JOBS
        or token in ARIA2_PENDING_UPLOAD_CHOICES
    ):
        token = secrets.token_hex(4)

    EXTRACT_PICK_SESSIONS[token] = {
        "requester_id": requester_id,
        "source_inputs": normalized_inputs,
        "target_dir": str(target_dir),
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def register_extract_file_pick_session(
    requester_id: Optional[int],
    extract_mode: str,
    target_dir: Path,
    files: List[Path],
) -> Optional[str]:
    valid_files = normalize_existing_file_paths(files)
    if not valid_files:
        return None

    cleanup_expired_extract_file_pick_sessions()
    token = secrets.token_hex(4)
    while (
        token in EXTRACT_FILE_PICK_SESSIONS
        or token in EXTRACT_PICK_SESSIONS
        or token in EXTRACT_DELETE_CONFIRM_SESSIONS
        or token in RCLONE_OUTPUT_SESSIONS
        or token in U1_FILE_PICK_SESSIONS
        or token in U1_FOLDER_MODE_SESSIONS
        or token in ARIA2_UPLOAD_JOBS
        or token in ARIA2_PENDING_UPLOAD_CHOICES
    ):
        token = secrets.token_hex(4)

    EXTRACT_FILE_PICK_SESSIONS[token] = {
        "requester_id": requester_id,
        "extract_mode": str(extract_mode).strip().lower(),
        "target_dir": str(target_dir),
        "files": [str(item) for item in valid_files],
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def resolve_extract_file_pick_paths(payload: Dict[str, object]) -> List[Path]:
    files_raw = payload.get("files")
    if not isinstance(files_raw, list):
        return []
    return normalize_existing_file_paths([Path(str(item)) for item in files_raw])


def register_extract_delete_confirm_session(
    requester_id: Optional[int],
    archive_paths: List[Path],
    summary_text: str,
) -> Optional[str]:
    valid_files = normalize_existing_file_paths(archive_paths)
    if not valid_files:
        return None

    cleanup_expired_extract_delete_confirm_sessions()
    token = secrets.token_hex(4)
    while (
        token in EXTRACT_DELETE_CONFIRM_SESSIONS
        or token in EXTRACT_FILE_PICK_SESSIONS
        or token in EXTRACT_PICK_SESSIONS
        or token in RCLONE_OUTPUT_SESSIONS
        or token in U1_FILE_PICK_SESSIONS
        or token in U1_FOLDER_MODE_SESSIONS
        or token in ARIA2_UPLOAD_JOBS
        or token in ARIA2_PENDING_UPLOAD_CHOICES
    ):
        token = secrets.token_hex(4)

    EXTRACT_DELETE_CONFIRM_SESSIONS[token] = {
        "requester_id": requester_id,
        "archive_paths": [str(item) for item in valid_files],
        "summary_text": str(summary_text).strip(),
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def resolve_extract_delete_confirm_paths(payload: Dict[str, object]) -> List[Path]:
    files_raw = payload.get("archive_paths")
    if not isinstance(files_raw, list):
        return []
    return normalize_existing_file_paths([Path(str(item)) for item in files_raw])


def register_rclone_output_session(
    requester_id: Optional[int],
    summary_lines: List[str],
    output_pages: List[str],
) -> Optional[str]:
    pages = [str(page) for page in output_pages if str(page).strip()]
    if not pages:
        return None

    cleanup_expired_rclone_output_sessions()
    token = secrets.token_hex(4)
    while (
        token in RCLONE_OUTPUT_SESSIONS
        or token in U1_FILE_PICK_SESSIONS
        or token in U1_FOLDER_MODE_SESSIONS
        or token in EXTRACT_PICK_SESSIONS
        or token in EXTRACT_FILE_PICK_SESSIONS
        or token in EXTRACT_DELETE_CONFIRM_SESSIONS
        or token in ARIA2_UPLOAD_JOBS
        or token in ARIA2_PENDING_UPLOAD_CHOICES
    ):
        token = secrets.token_hex(4)

    RCLONE_OUTPUT_SESSIONS[token] = {
        "requester_id": requester_id,
        "summary_lines": [str(line) for line in summary_lines],
        "pages": pages,
        "created_at": time.time(),
        "expires_at": time.time() + ARIA2_BUTTON_TTL_SECONDS,
    }
    return token


def build_rclone_output_keyboard(
    token: str,
    total_pages: int,
    page: int = 0,
) -> Optional[InlineKeyboardMarkup]:
    safe_total = max(1, int(total_pages))
    if safe_total <= 1:
        return None

    safe_page = max(0, min(int(page), safe_total - 1))
    nav_row: List[InlineKeyboardButton] = []
    if safe_page > 0:
        nav_row.append(
            InlineKeyboardButton("Prev", callback_data=f"rclpg|{token}|{safe_page - 1}")
        )
    if safe_page < (safe_total - 1):
        nav_row.append(
            InlineKeyboardButton("Next", callback_data=f"rclpg|{token}|{safe_page + 1}")
        )

    if not nav_row:
        return None
    return InlineKeyboardMarkup([nav_row])


def build_rclone_output_text(
    summary_lines: List[str],
    output_pages: List[str],
    page: int = 0,
) -> str:
    lines = [str(line) for line in summary_lines]
    if not output_pages:
        lines.extend(["", "Tidak ada output command."])
        return "\n".join(lines)

    safe_total = max(1, len(output_pages))
    safe_page = max(0, min(int(page), safe_total - 1))
    lines.extend(
        [
            "",
            f"Output halaman `{safe_page + 1}/{safe_total}`:",
            "```text",
            output_pages[safe_page],
            "```",
        ]
    )
    return "\n".join(lines)


def build_u1_file_picker_keyboard(
    token: str,
    total_files: int,
    page: int = 0,
) -> InlineKeyboardMarkup:
    safe_total = max(0, int(total_files))
    if safe_total <= 0:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("Batal", callback_data=f"u1pick|{token}|cancel")]]
        )

    page_count = max(1, (safe_total + U1_PICKER_PAGE_SIZE - 1) // U1_PICKER_PAGE_SIZE)
    safe_page = max(0, min(int(page), page_count - 1))
    start = safe_page * U1_PICKER_PAGE_SIZE
    end = min(safe_total, start + U1_PICKER_PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for file_index in range(start, end):
        current_row.append(
            InlineKeyboardButton(
                str(file_index + 1),
                callback_data=f"u1pick|{token}|{file_index}",
            )
        )
        if len(current_row) >= U1_PICKER_BUTTONS_PER_ROW:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)

    nav_row: List[InlineKeyboardButton] = []
    if safe_page > 0:
        nav_row.append(
            InlineKeyboardButton("Prev", callback_data=f"u1page|{token}|{safe_page - 1}")
        )
    if safe_page < (page_count - 1):
        nav_row.append(
            InlineKeyboardButton("Next", callback_data=f"u1page|{token}|{safe_page + 1}")
        )
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("Batal", callback_data=f"u1pick|{token}|cancel")])
    return InlineKeyboardMarkup(rows)


def build_u1_file_picker_text(
    source_input: str,
    target_chat,
    source_paths: List[Path],
    page: int = 0,
) -> str:
    total_files = len(source_paths)
    if total_files <= 0:
        return "Tidak ada path valid untuk dipilih."

    page_count = max(1, (total_files + U1_PICKER_PAGE_SIZE - 1) // U1_PICKER_PAGE_SIZE)
    safe_page = max(0, min(int(page), page_count - 1))
    start = safe_page * U1_PICKER_PAGE_SIZE
    end = min(total_files, start + U1_PICKER_PAGE_SIZE)

    lines = [
        "Pilih file/folder untuk /u1.",
        f"Input: `{source_input}`",
        f"Total item: `{total_files}`",
        f"Tujuan upload Telegram: `{target_label(target_chat)}`",
        "",
        "Daftar item:",
    ]
    for index, source_path in enumerate(source_paths[start:end], start=start + 1):
        name_text = f"{source_path.name}/" if source_path.is_dir() else source_path.name
        kind_text = "DIR" if source_path.is_dir() else "FILE"
        lines.append(f"{index}. `{name_text}` ({kind_text})")

    if page_count > 1:
        lines.extend(["", f"Halaman: `{safe_page + 1}/{page_count}`"])

    lines.extend(
        [
            "",
            f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
            "Klik angka untuk pilih file/folder. Jika pilih folder, bot akan minta mode upload folder.",
        ]
    )
    return trim_output("\n".join(lines))


def build_u1_folder_mode_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("1. Semua + folder", callback_data=f"u1fmode|{token}|all_with_folder")],
            [InlineKeyboardButton("2. Semua tanpa folder", callback_data=f"u1fmode|{token}|all_no_folder")],
            [InlineKeyboardButton("3. Single file", callback_data=f"u1fmode|{token}|single_file")],
            [InlineKeyboardButton("Batal", callback_data=f"u1fmode|{token}|cancel")],
        ]
    )


def u1_folder_mode_label(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized == "all_with_folder":
        return "Upload semua file beserta folder"
    if normalized == "all_no_folder":
        return "Upload semua file kecuali folder"
    if normalized == "single_file":
        return "Upload single file"
    return normalized or "unknown"


def collect_folder_files(folder_path: Path, recursive: bool) -> Tuple[List[Path], Optional[str]]:
    if not folder_path.exists():
        return [], f"Folder tidak ditemukan: `{folder_path}`"
    if not folder_path.is_dir():
        return [], f"Path bukan folder: `{folder_path}`"

    collected: List[Path] = []
    seen = set()
    try:
        iterator = folder_path.rglob("*") if recursive else folder_path.iterdir()
        for item in iterator:
            try:
                resolved = item.resolve()
            except Exception:
                resolved = item
            if not resolved.exists() or not resolved.is_file():
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            collected.append(resolved)
    except Exception as e:
        return [], str(e)

    collected.sort(key=lambda p: str(p).lower())
    return collected, None


def extract_mode_label(mode: str) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized == "unrar":
        return "unrar x"
    if normalized == "unzip":
        return "unzip"
    if normalized == "untar":
        return "tar -xzf"
    if normalized == "un7z":
        return "7z x"
    return normalized or "unknown"


def build_extract_mode_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("unrar", callback_data=f"xpick|{token}|unrar"),
                InlineKeyboardButton("unzip", callback_data=f"xpick|{token}|unzip"),
            ],
            [
                InlineKeyboardButton("untar", callback_data=f"xpick|{token}|untar"),
                InlineKeyboardButton("un7z", callback_data=f"xpick|{token}|un7z"),
            ],
            [
                InlineKeyboardButton("Batal", callback_data=f"xpick|{token}|cancel"),
            ],
        ]
    )


def build_extract_file_picker_keyboard(
    token: str,
    total_files: int,
    page: int = 0,
) -> InlineKeyboardMarkup:
    safe_total = max(0, int(total_files))
    if safe_total <= 0:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("Batal", callback_data=f"xfile|{token}|cancel")]]
        )

    page_count = max(1, (safe_total + EXTRACT_PICKER_PAGE_SIZE - 1) // EXTRACT_PICKER_PAGE_SIZE)
    safe_page = max(0, min(int(page), page_count - 1))
    start = safe_page * EXTRACT_PICKER_PAGE_SIZE
    end = min(safe_total, start + EXTRACT_PICKER_PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for file_index in range(start, end):
        current_row.append(
            InlineKeyboardButton(
                str(file_index + 1),
                callback_data=f"xfile|{token}|{file_index}",
            )
        )
        if len(current_row) >= EXTRACT_PICKER_BUTTONS_PER_ROW:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)

    nav_row: List[InlineKeyboardButton] = []
    if safe_page > 0:
        nav_row.append(
            InlineKeyboardButton("Prev", callback_data=f"xpage|{token}|{safe_page - 1}")
        )
    if safe_page < (page_count - 1):
        nav_row.append(
            InlineKeyboardButton("Next", callback_data=f"xpage|{token}|{safe_page + 1}")
        )
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton("Batal", callback_data=f"xfile|{token}|cancel")])
    return InlineKeyboardMarkup(rows)


def build_extract_file_picker_text(
    extract_mode: str,
    target_dir: Path,
    source_paths: List[Path],
    page: int = 0,
) -> str:
    total_files = len(source_paths)
    if total_files <= 0:
        return "Tidak ada arsip valid untuk dipilih."

    page_count = max(1, (total_files + EXTRACT_PICKER_PAGE_SIZE - 1) // EXTRACT_PICKER_PAGE_SIZE)
    safe_page = max(0, min(int(page), page_count - 1))
    start = safe_page * EXTRACT_PICKER_PAGE_SIZE
    end = min(total_files, start + EXTRACT_PICKER_PAGE_SIZE)

    lines = [
        "Pilih arsip untuk /extract.",
        f"Mode: `{extract_mode_label(extract_mode)}`",
        f"Target folder: `{target_dir}`",
        f"Total arsip cocok: `{total_files}`",
        "",
        "Daftar arsip:",
    ]
    for index, source_path in enumerate(source_paths[start:end], start=start + 1):
        lines.append(f"{index}. `{source_path.name}`")

    if page_count > 1:
        lines.extend(["", f"Halaman: `{safe_page + 1}/{page_count}`"])

    lines.extend(
        [
            "",
            f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
            "Klik angka untuk konfirmasi file yang akan diekstrak.",
        ]
    )
    return trim_output("\n".join(lines))


def build_extract_delete_confirm_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Ya, hapus arsip", callback_data=f"xdel|{token}|yes")],
            [InlineKeyboardButton("Tidak, simpan arsip", callback_data=f"xdel|{token}|no")],
        ]
    )


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


def sanitize_mention_name(raw_name: str) -> str:
    cleaned = (raw_name or "").replace("\r", " ").replace("\n", " ").strip()
    if not cleaned:
        return "user"
    # Hindari karakter yang bisa memutus format markdown mention.
    cleaned = cleaned.replace("[", "").replace("]", "").replace("`", "")
    return cleaned[:64]


def mention_user_by_id(user_id: Optional[int], display_name: Optional[str] = None) -> str:
    if isinstance(user_id, int):
        label = sanitize_mention_name(display_name or "user")
        return f"[{label}](tg://user?id={user_id})"
    if display_name:
        return f"`{sanitize_mention_name(display_name)}`"
    return "`unknown`"


async def send_upload_result_notification(
    status_message,
    source_title: str,
    destination_text: str,
    executor_user_id: Optional[int],
    executor_display_name: Optional[str],
    success_count: int,
    failed_count: int,
    cancelled: bool = False,
) -> None:
    if cancelled:
        result_text = "dibatalkan"
    elif failed_count <= 0:
        result_text = "sukses"
    elif success_count > 0:
        result_text = "gagal sebagian"
    else:
        result_text = "gagal"

    lines = [
        "Notifikasi upload:",
        f"Sumber: `{source_title}`",
        f"Tujuan: `{destination_text}`",
        f"Hasil: `{result_text}` (berhasil `{success_count}`, gagal `{failed_count}`)",
        f"Eksekutor: {mention_user_by_id(executor_user_id, executor_display_name)}",
    ]
    try:
        await status_message.reply_text(trim_output("\n".join(lines)), disable_web_page_preview=True)
    except Exception:
        pass


async def send_download_result_notification(
    status_message,
    source_label: str,
    requester_id: Optional[int],
    requester_name: str,
    request_id: int,
    media_name: str,
    downloaded_path: Optional[Path] = None,
    error_text: str = "",
) -> None:
    is_success = downloaded_path is not None and not error_text
    status_text = "sukses" if is_success else "gagal"
    lines = [
        "Notifikasi download:",
        f"Sumber: `/{source_label}`",
        f"ID: `#{request_id}`",
        f"File: `{media_name}`",
        f"Hasil: `{status_text}`",
        f"Eksekutor: {mention_user_by_id(requester_id, requester_name)}",
    ]
    if downloaded_path is not None:
        safe_path_text = str(downloaded_path).replace('"', "")
        lines.append(f"Lokasi: `{downloaded_path}`")
        lines.append(f"Lanjut upload manual: `/u1 \"{safe_path_text}\"`")
    if error_text:
        lines.append(f"Error: `{trim_output(error_text)}`")
    try:
        await status_message.reply_text(trim_output("\n".join(lines)), disable_web_page_preview=True)
    except Exception:
        pass


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


async def execute_upload_action_for_token(
    client: Client,
    status_message,
    token: str,
    action: str,
    source_paths: Optional[List[Path]] = None,
    callback_query=None,
) -> Tuple[object, bool]:
    cleanup_expired_aria2_upload_jobs()
    job_payload = ARIA2_UPLOAD_JOBS.get(token)
    if not job_payload:
        if callback_query:
            await callback_query.answer("Tombol upload sudah kedaluwarsa.", show_alert=True)
        return status_message, False

    if source_paths is None:
        source_paths = resolve_aria2_job_files(job_payload)
    else:
        source_paths = normalize_existing_file_paths(source_paths)

    source_label = str(job_payload.get("source_label") or "aria2").strip() or "aria2"
    source_title = source_label
    actor = getattr(callback_query, "from_user", None) if callback_query else None
    executor_user_id = (
        actor.id
        if actor and isinstance(getattr(actor, "id", None), int)
        else (
            job_payload.get("requester_id")
            if isinstance(job_payload.get("requester_id"), int)
            else None
        )
    )
    executor_display_name = (
        telegram_user_display_name(actor)
        if actor
        else str(job_payload.get("requester_name") or "").strip()
    )

    if not source_paths:
        ARIA2_UPLOAD_JOBS.pop(token, None)
        if callback_query:
            await callback_query.answer(f"File hasil {source_title} tidak ditemukan lagi.", show_alert=True)
        status_message = await update_status_message(
            status_message,
            status_message,
            f"Aksi upload dibatalkan: file hasil {source_title} sudah tidak tersedia.",
            reply_markup=None,
        )
        return status_message, False

    if action == "tg":
        running_task = UPLOAD_CONTROL.get("task")
        if running_task and not running_task.done():
            if callback_query:
                await callback_query.answer(
                    "Masih ada upload Telegram aktif. Gunakan /ucancel dulu.",
                    show_alert=True,
                )
            else:
                status_message = await update_status_message(
                    status_message,
                    status_message,
                    "Upload otomatis ke Telegram ditunda karena masih ada upload aktif.\n"
                    "Klik tombol upload setelah upload aktif selesai.",
                    reply_markup=build_aria2_upload_keyboard(token),
                )
            return status_message, False

        if callback_query:
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
                f"Memulai upload hasil {source_title} ke Telegram...\n"
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
                f"Upload hasil {source_title} ke Telegram dibatalkan."
                if cancelled
                else f"Upload hasil {source_title} ke Telegram selesai."
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
            status_message = await update_status_message(
                status_message,
                status_message,
                summary_text,
                reply_markup=reply_markup,
            )
            await send_upload_result_notification(
                status_message=status_message,
                source_title=source_title,
                destination_text=target,
                executor_user_id=executor_user_id,
                executor_display_name=executor_display_name,
                success_count=len(success_lines),
                failed_count=len(failed_lines),
                cancelled=cancelled,
            )
        finally:
            current_task = asyncio.current_task()
            if UPLOAD_CONTROL.get("task") is current_task:
                UPLOAD_CONTROL["task"] = None
                UPLOAD_CONTROL["cancel_event"] = None
        return status_message, True

    if action in {"gd", "tb", "db"}:
        if action == "gd":
            remote_base = RCLONE_GDRIVE_REMOTE
            remote_label = "Google Drive"
        elif action == "tb":
            remote_base = RCLONE_TERABOX_REMOTE
            remote_label = "Terabox"
        else:
            remote_base = RCLONE_DROPBOX_REMOTE
            remote_label = "Dropbox"

        if not remote_base:
            if callback_query:
                await callback_query.answer(
                    f"Remote rclone {remote_label} belum diatur di env.",
                    show_alert=True,
                )
            else:
                status_message = await update_status_message(
                    status_message,
                    status_message,
                    f"Upload otomatis via rclone {remote_label} batal: remote belum diatur di env.\n"
                    "Pilih tujuan upload lain dari tombol.",
                    reply_markup=build_aria2_upload_keyboard(token),
                )
            return status_message, False

        if callback_query:
            await callback_query.answer(f"Memulai upload rclone {remote_label}...")

        job_payload["last_action"] = action
        status_message = await update_status_message(
            status_message,
            status_message,
            f"Memulai upload hasil {source_title} via rclone...\n"
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
            summary_title=f"Upload hasil {source_title} via rclone {remote_label} selesai.",
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
        status_message = await update_status_message(
            status_message,
            status_message,
            summary_text,
            reply_markup=reply_markup,
        )
        await send_upload_result_notification(
            status_message=status_message,
            source_title=source_title,
            destination_text=remote_base,
            executor_user_id=executor_user_id,
            executor_display_name=executor_display_name,
            success_count=len(success_lines),
            failed_count=len(failed_lines),
            cancelled=False,
        )
        return status_message, True

    if callback_query:
        await callback_query.answer("Aksi tombol tidak dikenal.", show_alert=True)
    return status_message, False


def telegram_user_display_name(user) -> str:
    if not user:
        return ""
    full_name = " ".join(
        part.strip()
        for part in [getattr(user, "first_name", ""), getattr(user, "last_name", "")]
        if part and str(part).strip()
    ).strip()
    if full_name:
        return full_name
    username = str(getattr(user, "username", "") or "").strip()
    if username:
        return username
    user_id = getattr(user, "id", None)
    return str(user_id) if isinstance(user_id, int) else ""


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


async def require_whitelist_admin_access(message, command_name: str) -> bool:
    actor = getattr(message, "from_user", None)
    if not actor:
        await open_status_message(
            message,
            f"Perintah /{command_name} hanya untuk admin whitelist (user ID).",
        )
        return False

    if not PKILL_ADMIN_USER_IDS:
        await open_status_message(
            message,
            "Whitelist admin command kosong.\n"
            "Set env `PKILL_ADMIN_IDS` dengan daftar user ID (pisahkan koma/spasi).",
        )
        return False

    if actor.id in PKILL_ADMIN_USER_IDS:
        return True

    await open_status_message(
        message,
        f"Kamu tidak ada di whitelist admin `/{command_name}`.",
    )
    return False


def is_whitelist_admin_user(message) -> bool:
    actor = getattr(message, "from_user", None)
    if not actor:
        return False
    return actor.id in PKILL_ADMIN_USER_IDS


async def require_owner_or_whitelist_access(message, command_name: str) -> bool:
    if is_whitelist_admin_user(message):
        return True

    if BOT_MODE:
        if not OWNER_USER_ID:
            await open_status_message(
                message,
                "OWNER_USER_ID belum diatur.\n"
                f"Perintah /{command_name} butuh owner bot atau user whitelist `PKILL_ADMIN_IDS`.",
            )
            return False
        if is_owner_in_bot_mode(message):
            return True
        await open_status_message(
            message,
            f"Perintah /{command_name} hanya untuk owner bot atau user whitelist `PKILL_ADMIN_IDS`.",
        )
        return False

    if is_saved_messages_only_violation(message):
        await open_status_message(
            message,
            f"Gunakan /{command_name} hanya di Saved Messages, atau pakai akun yang ada di whitelist `PKILL_ADMIN_IDS`.",
        )
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
    selected_upload_action: str = "",
    upload_token: Optional[str] = None,
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
            "requester_id": getattr(getattr(command_message, "from_user", None), "id", None),
            "requester_name": telegram_user_display_name(
                getattr(command_message, "from_user", None)
            ),
            "chat_id": command_message.chat.id,
            "enable_upload_buttons": bool(enable_upload_buttons),
            "selected_upload_action": selected_upload_action.strip(),
            "upload_token": upload_token.strip() if isinstance(upload_token, str) else None,
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
            status_message = await update_status_message(
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
            await send_download_result_notification(
                status_message=status_message,
                source_label="d1",
                requester_id=item.get("requester_id")
                if isinstance(item.get("requester_id"), int)
                else None,
                requester_name=str(item.get("requester_name") or "").strip(),
                request_id=request_id,
                media_name=media_name,
                downloaded_path=None,
                error_text=str(e),
            )
        else:
            if not file_path:
                status_message = await update_status_message(
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
                await send_download_result_notification(
                    status_message=status_message,
                    source_label="d1",
                    requester_id=item.get("requester_id")
                    if isinstance(item.get("requester_id"), int)
                    else None,
                    requester_name=str(item.get("requester_name") or "").strip(),
                    request_id=request_id,
                    media_name=media_name,
                    downloaded_path=None,
                    error_text="path file kosong",
                )
            else:
                try:
                    downloaded_path = Path(str(file_path)).resolve()
                except Exception:
                    downloaded_path = Path(str(file_path))

                if item.get("enable_upload_buttons"):
                    selected_upload_action = str(item.get("selected_upload_action") or "").strip()
                    token = register_aria2_upload_job(
                        requester_id=getattr(command_message.from_user, "id", None),
                        requester_name=telegram_user_display_name(
                            getattr(command_message, "from_user", None)
                        ),
                        chat_id=item.get("chat_id", command_message.chat.id),
                        files=[downloaded_path],
                        target_dir=downloaded_path.parent,
                        source_label="d1",
                        token=item.get("upload_token"),
                    )
                    if token:
                        summary_lines = [
                            "Download selesai.",
                            f"ID: `#{request_id}`",
                            f"Lokasi: `{downloaded_path}`",
                            "",
                            f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
                        ]
                        if selected_upload_action in {"tg", "gd", "tb", "db"}:
                            summary_lines.append("")
                            summary_lines.append(
                                f"Pilihan upload otomatis: `{selected_upload_action}`. Upload akan langsung dijalankan."
                            )
                            status_message = await update_status_message(
                                command_message,
                                status_message,
                                trim_output("\n".join(summary_lines)),
                                reply_markup=None,
                            )
                            status_message, _ = await execute_upload_action_for_token(
                                client=client,
                                status_message=status_message,
                                token=token,
                                action=selected_upload_action,
                                source_paths=[downloaded_path],
                            )
                        elif selected_upload_action == "skip":
                            summary_lines.append("")
                            summary_lines.append("Upload lanjutan: dilewati (dipilih sebelumnya).")
                            status_message = await update_status_message(
                                command_message,
                                status_message,
                                trim_output("\n".join(summary_lines)),
                                reply_markup=None,
                            )
                            ARIA2_UPLOAD_JOBS.pop(token, None)
                        else:
                            summary_lines.append(
                                "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox / rclone Dropbox."
                            )
                            status_message = await update_status_message(
                                command_message,
                                status_message,
                                trim_output("\n".join(summary_lines)),
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
                await send_download_result_notification(
                    status_message=status_message,
                    source_label="d1",
                    requester_id=item.get("requester_id")
                    if isinstance(item.get("requester_id"), int)
                    else None,
                    requester_name=str(item.get("requester_name") or "").strip(),
                    request_id=request_id,
                    media_name=media_name,
                    downloaded_path=downloaded_path,
                    error_text="",
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

    matched_items: List[Path] = []
    seen_paths = set()
    has_pattern = has_wildcard(expanded)

    for candidate in candidates:
        candidate_expanded = os.path.expanduser(candidate)
        if has_wildcard(candidate_expanded):
            raw_matches = sorted(glob.glob(candidate_expanded, recursive=True))
            for item in raw_matches:
                resolved = Path(item).resolve()
                if resolved.exists():
                    key = str(resolved)
                    if key not in seen_paths:
                        seen_paths.add(key)
                        matched_items.append(resolved)
        else:
            resolved = Path(candidate_expanded).resolve()
            if resolved.exists():
                key = str(resolved)
                if key not in seen_paths:
                    seen_paths.add(key)
                    matched_items.append(resolved)

    if matched_items:
        matched_items.sort(key=lambda p: (not p.is_dir(), p.name.lower()))
        return matched_items, None

    if has_pattern:
        return [], (
            "Path tidak ditemukan untuk wildcard.\n"
            f"Pola: `{path_text}`\n"
            f"Default folder upload: `{upload_root}`"
        )

    if not is_absolute_or_home:
        fallback_path = (upload_root / expanded).resolve()
        return [], (
            "Path tidak ditemukan.\n"
            f"Input: `{path_text}`\n"
            f"Cek juga: `{fallback_path}`"
        )

    return [], f"Path tidak ditemukan:\n`{Path(os.path.expanduser(expanded)).resolve()}`"


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


def build_extract_command(
    archive_path: Path,
    target_dir: Path,
    forced_mode: Optional[str] = None,
) -> Tuple[str, List[str]]:
    archive_name_lower = archive_path.name.lower()
    target_dir_text = str(target_dir)
    selected_mode = str(forced_mode or "").strip().lower()
    if selected_mode == "untar" or (
        not selected_mode and archive_name_lower.endswith((".tar.gz", ".tgz"))
    ):
        return "tar", ["tar", "-xzf", str(archive_path), "-C", target_dir_text]
    if selected_mode == "unzip" or (not selected_mode and archive_name_lower.endswith(".zip")):
        return "unzip", ["unzip", "-o", str(archive_path), "-d", target_dir_text]
    if selected_mode == "unrar" or (not selected_mode and archive_name_lower.endswith(".rar")):
        return "unrar", ["unrar", "x", "-o+", "-y", str(archive_path), f"{target_dir_text}{os.sep}"]
    return "7z", ["7z", "x", str(archive_path), f"-o{target_dir_text}", "-y"]


def archive_matches_extract_mode(archive_path: Path, mode: str) -> bool:
    archive_name_lower = archive_path.name.lower()
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "unrar":
        return archive_name_lower.endswith(".rar")
    if normalized_mode == "unzip":
        return archive_name_lower.endswith(".zip")
    if normalized_mode == "untar":
        return archive_name_lower.endswith((".tar.gz", ".tgz"))
    if normalized_mode == "un7z":
        return archive_name_lower.endswith(".7z")
    return True


def resolve_extract_candidates(source_inputs: List[str]) -> Tuple[List[Path], List[str]]:
    candidate_paths: List[Path] = []
    failed_lines: List[str] = []
    seen_paths = set()
    for raw_path in source_inputs:
        targets, resolve_error = resolve_path_candidates(raw_path)
        if resolve_error:
            failed_lines.append(f"- `{raw_path}` -> {resolve_error}")
            continue
        for target_path in targets:
            key = str(target_path)
            if key in seen_paths:
                continue
            seen_paths.add(key)
            candidate_paths.append(target_path)
    return candidate_paths, failed_lines


def compact_process_output(stderr_text: str, stdout_text: str, max_chars: int = 220) -> str:
    raw_output = (stderr_text or stdout_text).strip()
    if not raw_output:
        return "tanpa output"
    compact_text = re.sub(r"\s+", " ", " ".join(raw_output.splitlines())).strip()
    compact_text = compact_text.replace("`", "'")
    if len(compact_text) > max_chars:
        return compact_text[: max_chars - 3] + "..."
    return compact_text


async def execute_extract_operation(
    command_message,
    status_message,
    source_inputs: List[str],
    extract_mode: str,
    target_dir: Path,
    requester_id: Optional[int] = None,
) -> None:
    normalized_mode = str(extract_mode or "").strip().lower()
    if normalized_mode not in EXTRACT_MODES:
        await update_status_message(
            command_message,
            status_message,
            f"Mode extract tidak valid: `{extract_mode}`",
            reply_markup=None,
        )
        return

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        await update_status_message(
            command_message,
            status_message,
            "Gagal menyiapkan folder ekstrak.\n"
            f"Target: `{target_dir}`\n"
            f"Error: `{e}`",
            reply_markup=None,
        )
        return

    candidate_paths, failed_lines = resolve_extract_candidates(source_inputs)
    matching_candidates = []
    skipped_lines = []
    mode_display = extract_mode_label(normalized_mode)
    for candidate_path in candidate_paths:
        if archive_matches_extract_mode(candidate_path, normalized_mode):
            matching_candidates.append(candidate_path)
        else:
            skipped_lines.append(
                f"- `{candidate_path}` -> tidak cocok mode `{mode_display}`"
            )

    success_lines = []
    success_archive_paths: List[Path] = []
    for archive_path in matching_candidates:
        if not path_exists_or_symlink(archive_path):
            failed_lines.append(f"- `{archive_path}` -> path tidak ditemukan")
            continue
        if archive_path.is_dir():
            failed_lines.append(f"- `{archive_path}` -> path harus file arsip, bukan folder")
            continue

        extractor_name, command = build_extract_command(
            archive_path,
            target_dir,
            forced_mode=normalized_mode,
        )
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            failed_lines.append(
                f"- `{archive_path}` -> command `{extractor_name}` tidak ditemukan di sistem"
            )
            continue
        except Exception as e:
            failed_lines.append(f"- `{archive_path}` -> gagal menjalankan `{extractor_name}`: {e}")
            continue

        timed_out = False
        try:
            stdout_raw, stderr_raw = await asyncio.wait_for(
                process.communicate(),
                timeout=EXTRACT_COMMAND_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            timed_out = True
            process.kill()
            stdout_raw, stderr_raw = await process.communicate()

        stdout_text = stdout_raw.decode("utf-8", errors="ignore")
        stderr_text = stderr_raw.decode("utf-8", errors="ignore")
        if not timed_out and process.returncode == 0:
            success_lines.append(
                f"- `{archive_path.name}` -> `{target_dir}` (via `{extractor_name}`)"
            )
            success_archive_paths.append(archive_path)
            continue

        reason = "timeout" if timed_out else f"exit code {process.returncode}"
        error_excerpt = compact_process_output(stderr_text, stdout_text)
        failed_lines.append(
            f"- `{archive_path}` -> `{extractor_name}` gagal ({reason}): {error_excerpt}"
        )

    summary_lines = [
        "extract selesai.",
        f"Mode: `{mode_display}`",
        f"Target folder: `{target_dir}`",
        f"Input sumber: `{len(source_inputs)}`",
        f"Kandidat: `{len(candidate_paths)}`",
        f"Diproses: `{len(matching_candidates)}`",
        f"Berhasil: `{len(success_lines)}`",
        f"Gagal: `{len(failed_lines)}`",
    ]
    if skipped_lines:
        summary_lines.append(f"Diskip (tidak cocok mode): `{len(skipped_lines)}`")

    if success_lines:
        summary_lines.append("")
        summary_lines.append("Berhasil:")
        summary_lines.extend(success_lines[:20])
        if len(success_lines) > 20:
            summary_lines.append(f"... {len(success_lines) - 20} arsip lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Gagal:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} item lain.")

    if skipped_lines:
        summary_lines.append("")
        summary_lines.append("Diskip:")
        summary_lines.extend(skipped_lines[:10])
        if len(skipped_lines) > 10:
            summary_lines.append(f"... {len(skipped_lines) - 10} item lain.")

    summary_text = trim_output("\n".join(summary_lines))
    summary_markup = None

    if success_archive_paths:
        effective_requester_id = requester_id
        if not isinstance(effective_requester_id, int):
            fallback_requester = getattr(getattr(command_message, "from_user", None), "id", None)
            effective_requester_id = fallback_requester if isinstance(fallback_requester, int) else None

        delete_token = register_extract_delete_confirm_session(
            requester_id=effective_requester_id,
            archive_paths=success_archive_paths,
            summary_text=summary_text,
        )
        if delete_token:
            summary_text = trim_output(
                f"{summary_text}\n\n"
                f"Arsip berhasil diekstrak: `{len(success_archive_paths)}`.\n"
                "Hapus file arsip sumber?\n"
                f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`."
            )
            summary_markup = build_extract_delete_confirm_keyboard(delete_token)

    await update_status_message(
        command_message,
        status_message,
        summary_text,
        reply_markup=summary_markup,
    )


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
    if not parse_telegram_link(source_text):
        return None, "Pesan yang dibalas tidak berisi media atau link Telegram yang valid."

    return await resolve_target_message_from_link_text(client, source_text)


async def resolve_target_message_from_link_text(client: Client, source_text: str):
    parsed = parse_telegram_link(source_text)
    if not parsed:
        return None, "Link Telegram tidak valid. Format: `/d1 <link_telegram>`."

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


def help_public_access_label() -> str:
    if BOT_MODE:
        return "semua user chat" if PUBLIC_MODE else "owner bot"
    return "semua chat" if PUBLIC_MODE else "Saved Messages (akun sendiri)"


def help_admin_access_label() -> str:
    if BOT_MODE:
        return "owner bot + whitelist PKILL_ADMIN_IDS"
    return "Saved Messages + whitelist PKILL_ADMIN_IDS"


def help_private_access_label() -> str:
    if BOT_MODE:
        return "owner bot"
    return "Saved Messages (akun sendiri)"


def build_help_topic_text(topic: str) -> Optional[str]:
    normalized_topic = str(topic or "").strip().lower().lstrip("/")
    if not normalized_topic:
        return None

    if normalized_topic in {"extract", "x", "unrar", "unzip", "untar", "un7z"}:
        return trim_output(
            "\n".join(
                [
                    "Bantuan `/extract`",
                    f"Akses: `{help_admin_access_label()}`",
                    f"Target ekstrak default: `{extract_root}`",
                    "Format:",
                    "`/extract`",
                    "`/extract /home/runner/downloads/*.zip`",
                    "`/extract /home/runner/downloads/sample.rar`",
                    "",
                    "Alur:",
                    "1. Pilih mode extract (`unrar`, `unzip`, `untar`, `un7z`).",
                    "2. Pilih file via tombol angka (maks 10 file/halaman, Prev/Next).",
                    "3. Setelah ekstrak sukses, bot tanya apakah file arsip sumber ingin dihapus.",
                ]
            )
        )

    if normalized_topic in {"aria2", "a2"}:
        return trim_output(
            "\n".join(
                [
                    "Bantuan `/aria2`",
                    f"Akses: `{help_public_access_label()}`",
                    f"Folder download default: `{download_root}`",
                    "Format:",
                    "`/aria2 <url|magnet>`",
                    "`/aria2 -d /home/runner/downloads <url|magnet>`",
                    "`/aria2 -o nama_file.ext <url>`",
                    "",
                    "Catatan:",
                    "- Bisa dipakai sambil reply pesan yang berisi link direct (http/https/ftp).",
                    "- Setelah kirim command, pilih tujuan upload dulu (Telegram/rclone/Lewati), baru download jalan.",
                ]
            )
        )

    if normalized_topic in {"gdl", "gallerydl", "gallery-dl"}:
        return trim_output(
            "\n".join(
                [
                    "Bantuan `/gdl`",
                    f"Akses: `{help_public_access_label()}`",
                    f"Folder download default: `{download_root}`",
                    "Format:",
                    "`/gdl <url>`",
                    "`/gdl -d /home/runner/downloads -o directory=\"\" <url>`",
                    "",
                    "Catatan:",
                    "- Setelah kirim command, pilih tujuan upload dulu (Telegram/rclone/Lewati), baru download jalan.",
                    "- Alias command: `/gallerydl`.",
                ]
            )
        )

    if normalized_topic in {"u1", "upload"}:
        return trim_output(
            "\n".join(
                [
                    "Bantuan `/u1`",
                    f"Akses: `{help_public_access_label()}`",
                    f"Folder default: `{upload_root}`",
                    "Format:",
                    "`/u1`",
                    "`/u1 /home/runner/downloads/file.mp4`",
                    "`/u1 /home/runner/downloads/folder`",
                    "`/u1 /home/runner/downloads/*.mkv --to @username`",
                    "",
                    "Catatan:",
                    "- Jika pilih folder, tersedia 3 mode: semua+folder, semua tanpa folder, atau single file.",
                    "- Untuk mode single file, picker memakai tombol angka + pagination.",
                ]
            )
        )

    return None


def build_general_help_text() -> str:
    lines = [
        "Daftar command bot",
        f"Mode: `{'BOT' if BOT_MODE else 'USERBOT'}` | PUBLIC_MODE: `{'ON' if PUBLIC_MODE else 'OFF'}`",
        "",
        "Informasi command:",
        "- `/start` tampilkan ringkasan command.",
        "- `/help` tampilkan daftar command + topik bantuan.",
        "- `/help extract` detail command extract.",
        "- `/help aria2` detail command aria2.",
        "- `/help gdl` detail command gallery-dl.",
        "- `/help u1` detail command upload lokal.",
        "",
        f"Command public (`{help_public_access_label()}`):",
        "- `/d1`, `/dstatus`, `/dqueue`, `/aria2` (`/a2`), `/gdl` (`/gallerydl`), `/u1`, `/ps`.",
        "",
        f"Command admin (`{help_admin_access_label()}`):",
        "- `/extract`, `/rclone`, `/ls`, `/du`, `/df`, `/rm`, `/copy` (`/cp`), `/mv`, `/pkill`.",
        "",
        f"Command private (`{help_private_access_label()}`):",
        "- `/mkdir`, `/ucancel`.",
        "",
        "Contoh cepat:",
        "- `/extract`",
        "- `/aria2 https://example.com/file.zip`",
        "- `/gdl https://gofile.io/d/xxxxx`",
    ]
    return trim_output("\n".join(lines))


@app.on_message(command_filter(["start", "help"], allow_public=True))
async def start_help_command(client: Client, message):
    del client

    raw_text = (message.text or message.caption or "").strip()
    command_name = ""
    if raw_text.startswith("/"):
        command_name = raw_text.split(maxsplit=1)[0].split("@", 1)[0].lstrip("/").lower()
    access_command_name = command_name if command_name in {"start", "help"} else "help"

    if not await require_public_command_access(message, access_command_name):
        return

    args = command_args(message)
    topic = args[0] if (args and command_name == "help") else ""
    topic_text = build_help_topic_text(topic)
    if topic_text:
        await open_status_message(message, topic_text)
        return

    if topic:
        await open_status_message(
            message,
            trim_output(
                f"Topik bantuan `{topic}` tidak dikenali.\n"
                "Gunakan salah satu: `extract`, `aria2`, `gdl`, `u1`.\n\n"
                + build_general_help_text()
            ),
        )
        return

    await open_status_message(message, build_general_help_text())


# Manual command for Telegram media/link download.
@app.on_message(command_filter("d1", allow_public=True))
async def download_command(client: Client, message):
    if not await require_public_command_access(message, "d1"):
        return

    args = command_args(message)
    direct_link_text = " ".join(args).strip() if args else ""

    if not message.reply_to_message and not direct_link_text:
        await open_status_message(
            message,
            "Gunakan salah satu format berikut:\n"
            "1. Balas pesan yang berisi file/video atau link Telegram, lalu kirim `/d1`.\n"
            "2. Kirim langsung `/d1 <link_telegram>`."
        )
        return

    status_message = await open_status_message(message, "Memeriksa sumber download...")
    if message.reply_to_message:
        target_message, error_message = await resolve_target_message(
            client, message.reply_to_message
        )
    else:
        target_message, error_message = await resolve_target_message_from_link_text(
            client, direct_link_text
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
        enable_upload_buttons=False,
    )
    await update_status_message(
        message,
        status_message,
        "Permintaan download masuk antrian.\n"
        f"ID: `#{request_id}`\n"
        f"File: {name}\n"
        f"Posisi antrian: `{queue_position}`\n"
        f"Posisi total (termasuk yang aktif): `{overall_position}`\n"
        "Setelah selesai, gunakan `/u1 <path_file>` untuk upload manual."
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
    reply_message = message.reply_to_message
    reply_text = (reply_message.text or reply_message.caption or "").strip() if reply_message else ""
    reply_non_telegram_links = extract_non_telegram_links(reply_text) if reply_message else []

    if not args and reply_message:
        if (has_downloadable_media(reply_message) or parse_telegram_link(reply_text)) and not reply_non_telegram_links:
            await open_status_message(
                message,
                "Untuk file/link Telegram, gunakan `/d1`.\n"
                "`/aria2` khusus link direct (http/https/ftp/magnet).",
            )
            return

    urls, output_name, target_dir, parse_error = parse_aria2_command_args(
        args,
        fallback_urls=reply_non_telegram_links if reply_message else None,
    )
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

    preselect_token = register_pending_upload_choice(
        requester_id=getattr(message.from_user, "id", None),
        chat_id=message.chat.id,
        source_label="aria2",
    )
    preselect_markup = build_aria2_upload_keyboard(preselect_token)

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
        "Permintaan /aria2 diterima.\n"
        "Pilih tujuan upload dulu lewat tombol di bawah.\n"
        "Download baru akan mulai setelah kamu memilih.\n\n"
        f"Target folder: `{target_dir}`\n"
        f"Sumber: `{len(urls or [])}` item",
        reply_markup=preselect_markup,
    )

    selected_action = ""
    wait_deadline = time.time() + ARIA2_BUTTON_TTL_SECONDS
    while time.time() < wait_deadline:
        pending_payload = ARIA2_PENDING_UPLOAD_CHOICES.get(preselect_token)
        if not pending_payload:
            await update_status_message(
                message,
                status_message,
                "Sesi pilihan upload tidak ditemukan atau sudah kedaluwarsa.",
                reply_markup=None,
            )
            return

        selected_action = str(pending_payload.get("selected_action") or "").strip()
        if selected_action in {"tg", "gd", "tb", "db", "skip"}:
            break
        await asyncio.sleep(0.5)
    else:
        ARIA2_PENDING_UPLOAD_CHOICES.pop(preselect_token, None)
        await update_status_message(
            message,
            status_message,
            "Waktu memilih tujuan upload habis. Jalankan `/aria2` lagi.",
            reply_markup=None,
        )
        return

    ARIA2_PENDING_UPLOAD_CHOICES.pop(preselect_token, None)
    action_label = {
        "tg": "Telegram",
        "gd": "rclone Google Drive",
        "tb": "rclone Terabox",
        "db": "rclone Dropbox",
        "skip": "Lewati upload",
    }.get(selected_action, selected_action)
    status_message = await update_status_message(
        message,
        status_message,
        "Pilihan upload sudah disimpan.\n"
        f"Tujuan: `{action_label}`\n\n"
        "Memulai download via aria2...\n"
        f"Target folder: `{target_dir}`\n"
        f"Sumber: `{len(urls or [])}` item",
        reply_markup=None,
    )

    before_snapshot = snapshot_directory_file_state(target_dir)
    started_at = time.time()
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        ARIA2_PENDING_UPLOAD_CHOICES.pop(preselect_token, None)
        await update_status_message(
            message,
            status_message,
            "aria2c tidak ditemukan.\n"
            f"Set env `ARIA2_BIN` (saat ini: `{ARIA2_BIN}`) ke binary aria2 yang valid.",
        )
        return
    except Exception as e:
        ARIA2_PENDING_UPLOAD_CHOICES.pop(preselect_token, None)
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
                requester_name=telegram_user_display_name(getattr(message, "from_user", None)),
                chat_id=message.chat.id,
                files=downloaded_files,
                target_dir=target_dir,
                source_label="aria2",
                token=preselect_token,
            )
            if token:
                summary_lines.append(f"File terdeteksi: `{len(downloaded_files)}`")
                summary_lines.append("")
                summary_lines.append(
                    f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`."
                )
                if len(downloaded_files) <= 5:
                    summary_lines.append("")
                    summary_lines.append("Daftar file:")
                    summary_lines.extend(f"- `{item.name}`" for item in downloaded_files)
                if selected_action in {"tg", "gd", "tb", "db"}:
                    summary_lines.append("")
                    summary_lines.append(
                        f"Pilihan upload otomatis: `{selected_action}`. Upload akan langsung dijalankan."
                    )
                    status_message = await update_status_message(
                        message,
                        status_message,
                        trim_output("\n".join(summary_lines)),
                        reply_markup=None,
                    )
                    status_message, _ = await execute_upload_action_for_token(
                        client=client,
                        status_message=status_message,
                        token=token,
                        action=selected_action,
                        source_paths=downloaded_files,
                    )
                    return
                if selected_action == "skip":
                    summary_lines.append("")
                    summary_lines.append("Upload lanjutan: dilewati (dipilih sebelumnya).")
                    await update_status_message(
                        message,
                        status_message,
                        trim_output("\n".join(summary_lines)),
                        reply_markup=None,
                    )
                    ARIA2_UPLOAD_JOBS.pop(token, None)
                    return
                summary_lines.append(
                    "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox / rclone Dropbox."
                )
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


@app.on_message(command_filter(["gdl", "gallerydl"], allow_public=True))
async def gallery_dl_download_command(client: Client, message):
    if not await require_public_command_access(message, "gdl"):
        return

    args = command_args(message)
    reply_message = message.reply_to_message
    reply_text = (reply_message.text or reply_message.caption or "").strip() if reply_message else ""
    reply_non_telegram_links = extract_non_telegram_links(reply_text) if reply_message else []

    if not args and reply_message:
        if (has_downloadable_media(reply_message) or parse_telegram_link(reply_text)) and not reply_non_telegram_links:
            await open_status_message(
                message,
                "Untuk file/link Telegram, gunakan `/d1`.\n"
                "`/gdl` khusus link direct (http/https/ftp).",
            )
            return

    gallery_args, target_dir, source_urls, parse_error = parse_gallery_dl_command_args(
        args,
        fallback_urls=reply_non_telegram_links if reply_message else None,
    )
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

    source_count = len(source_urls or [])
    command = [GALLERY_DL_BIN, *(gallery_args or [])]
    preselect_token = register_pending_upload_choice(
        requester_id=getattr(message.from_user, "id", None),
        chat_id=message.chat.id,
        source_label="gdl",
    )
    preselect_markup = build_aria2_upload_keyboard(preselect_token)

    status_message = await open_status_message(
        message,
        "Permintaan /gdl diterima.\n"
        "Pilih tujuan upload dulu lewat tombol di bawah.\n"
        "Download baru akan mulai setelah kamu memilih.\n\n"
        f"Target folder: `{target_dir}`\n"
        f"Sumber: `{source_count}` URL",
        reply_markup=preselect_markup,
    )

    selected_action = ""
    wait_deadline = time.time() + ARIA2_BUTTON_TTL_SECONDS
    while time.time() < wait_deadline:
        pending_payload = ARIA2_PENDING_UPLOAD_CHOICES.get(preselect_token)
        if not pending_payload:
            await update_status_message(
                message,
                status_message,
                "Sesi pilihan upload tidak ditemukan atau sudah kedaluwarsa.",
                reply_markup=None,
            )
            return

        selected_action = str(pending_payload.get("selected_action") or "").strip()
        if selected_action in {"tg", "gd", "tb", "db", "skip"}:
            break
        await asyncio.sleep(0.5)
    else:
        ARIA2_PENDING_UPLOAD_CHOICES.pop(preselect_token, None)
        await update_status_message(
            message,
            status_message,
            "Waktu memilih tujuan upload habis. Jalankan `/gdl` lagi.",
            reply_markup=None,
        )
        return

    ARIA2_PENDING_UPLOAD_CHOICES.pop(preselect_token, None)
    action_label = {
        "tg": "Telegram",
        "gd": "rclone Google Drive",
        "tb": "rclone Terabox",
        "db": "rclone Dropbox",
        "skip": "Lewati upload",
    }.get(selected_action, selected_action)
    status_message = await update_status_message(
        message,
        status_message,
        "Pilihan upload sudah disimpan.\n"
        f"Tujuan: `{action_label}`\n\n"
        "Memulai download via gallery-dl...\n"
        f"Target folder: `{target_dir}`\n"
        f"Sumber: `{source_count}` URL",
        reply_markup=None,
    )

    before_snapshot = snapshot_directory_file_state(target_dir)
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
            "gallery-dl tidak ditemukan.\n"
            f"Set env `GALLERY_DL_BIN` (saat ini: `{GALLERY_DL_BIN}`) ke binary gallery-dl yang valid.",
        )
        return
    except Exception as e:
        await update_status_message(message, status_message, f"Gagal menjalankan gallery-dl: `{e}`")
        return

    progress_state: Dict[str, object] = {
        "line": None,
        "updated_at": None,
        "stdout_tail": "",
        "stderr_tail": "",
    }
    stdout_chunks: List[str] = []
    stderr_chunks: List[str] = []
    stdout_task = asyncio.create_task(
        collect_process_stream(process.stdout, stdout_chunks, progress_state, "stdout_tail")
    )
    stderr_task = asyncio.create_task(
        collect_process_stream(process.stderr, stderr_chunks, progress_state, "stderr_tail")
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
                updated_at = progress_state.get("updated_at")
                update_age = (
                    format_duration(int(max(0, now - float(updated_at))))
                    if updated_at
                    else "unknown"
                )
                progress_lines = [
                    "gallery-dl sedang berjalan...",
                    f"PID: `{process.pid}`",
                    f"Target folder: `{target_dir}`",
                    f"Sumber URL: `{source_count}`",
                    f"Elapsed: `{elapsed}`",
                    f"Update terakhir: `{update_age}` lalu",
                ]
                raw_line = progress_state.get("line")
                if raw_line:
                    progress_lines.append("")
                    progress_lines.append("Log terakhir:")
                    progress_lines.append(f"`{raw_line}`")
                status_message = await update_status_message(
                    message,
                    status_message,
                    "\n".join(progress_lines),
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
        )
        summary_lines = [
            "gallery-dl selesai.",
            f"Target folder: `{target_dir}`",
            f"Durasi: `{elapsed_text}`",
            f"Return code: `{process.returncode}`",
        ]
        if downloaded_files:
            token = register_aria2_upload_job(
                requester_id=getattr(message.from_user, "id", None),
                requester_name=telegram_user_display_name(getattr(message, "from_user", None)),
                chat_id=message.chat.id,
                files=downloaded_files,
                target_dir=target_dir,
                source_label="gdl",
                token=preselect_token,
            )
            if token:
                summary_lines.append(f"File terdeteksi: `{len(downloaded_files)}`")
                summary_lines.append("")
                summary_lines.append(
                    f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`."
                )
                if len(downloaded_files) <= 5:
                    summary_lines.append("")
                    summary_lines.append("Daftar file:")
                    summary_lines.extend(f"- `{item.name}`" for item in downloaded_files)
                if selected_action in {"tg", "gd", "tb", "db"}:
                    summary_lines.append("")
                    summary_lines.append(
                        f"Pilihan upload otomatis: `{selected_action}`. Upload akan langsung dijalankan."
                    )
                    status_message = await update_status_message(
                        message,
                        status_message,
                        trim_output("\n".join(summary_lines)),
                        reply_markup=None,
                    )
                    status_message, _ = await execute_upload_action_for_token(
                        client=client,
                        status_message=status_message,
                        token=token,
                        action=selected_action,
                        source_paths=downloaded_files,
                    )
                    return
                if selected_action == "skip":
                    summary_lines.append("")
                    summary_lines.append("Upload lanjutan: dilewati (dipilih sebelumnya).")
                    await update_status_message(
                        message,
                        status_message,
                        trim_output("\n".join(summary_lines)),
                        reply_markup=None,
                    )
                    ARIA2_UPLOAD_JOBS.pop(token, None)
                    return
                summary_lines.append(
                    "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox / rclone Dropbox."
                )
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
            summary_lines.append("Ringkasan gallery-dl (stdout):")
            summary_lines.append("```text")
            summary_lines.append(stdout_tail)
            summary_lines.append("```")
        await update_status_message(message, status_message, trim_output("\n".join(summary_lines)))
        return

    failed_lines = [
        "gallery-dl gagal.",
        f"Target folder: `{target_dir}`",
        f"Durasi: `{elapsed_text}`",
        f"Return code: `{process.returncode}`",
    ]
    if stderr_tail:
        failed_lines.append("")
        failed_lines.append("Error gallery-dl (stderr):")
        failed_lines.append("```text")
        failed_lines.append(stderr_tail)
        failed_lines.append("```")
    elif stdout_tail:
        failed_lines.append("")
        failed_lines.append("Output gallery-dl (stdout):")
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
    pending_payload = ARIA2_PENDING_UPLOAD_CHOICES.get(token)
    active_payload = job_payload or pending_payload
    if not active_payload:
        await callback_query.answer("Tombol upload sudah kedaluwarsa.", show_alert=True)
        return

    source_label = str(active_payload.get("source_label") or "aria2").strip() or "aria2"
    source_tag = f"/{source_label}"

    actor = getattr(callback_query, "from_user", None)
    requester_id = active_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer(
            f"Tombol ini hanya untuk requester {source_tag}.",
            show_alert=True,
        )
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    if not job_payload and pending_payload:
        if source_label == "d1":
            ARIA2_PENDING_UPLOAD_CHOICES.pop(token, None)
            await callback_query.answer(
                "Sesi tombol /d1 lama tidak berlaku lagi. Jalankan `/d1` ulang.",
                show_alert=True,
            )
            await update_status_message(
                status_message,
                status_message,
                "Sesi tombol /d1 lama tidak berlaku lagi.\nGunakan `/d1` lagi.",
                reply_markup=None,
            )
            return

        if action == "retry":
            await callback_query.answer(
                "Retry belum tersedia sebelum download selesai.",
                show_alert=True,
            )
            return

        if action not in {"tg", "gd", "tb", "db", "skip"}:
            await callback_query.answer("Aksi tombol tidak dikenal.", show_alert=True)
            return

        pending_payload["selected_action"] = action
        pending_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
        start_text = "Download dimulai sekarang."
        if action == "skip":
            await callback_query.answer(
                f"Pilihan disimpan: upload lanjutan akan dilewati. {start_text}"
            )
            return

        action_label = {
            "tg": "Telegram",
            "gd": "rclone Google Drive",
            "tb": "rclone Terabox",
            "db": "rclone Dropbox",
        }.get(action, action)
        await callback_query.answer(
            f"Pilihan disimpan: {action_label}. {start_text}"
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
        if last_action not in {"tg", "gd", "tb", "db"}:
            await callback_query.answer(
                "Belum ada aksi upload sebelumnya untuk di-retry.",
                show_alert=True,
            )
            return
        action = last_action

    await execute_upload_action_for_token(
        client=client,
        status_message=status_message,
        token=token,
        action=action,
        callback_query=callback_query,
    )


@app.on_callback_query(filters.regex(r"^rclpg\|"))
async def rclone_output_page_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, page_text = parts
    try:
        requested_page = int(page_text)
    except ValueError:
        await callback_query.answer("Nomor halaman tidak valid.", show_alert=True)
        return

    cleanup_expired_rclone_output_sessions()
    session_payload = RCLONE_OUTPUT_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Output /rclone sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /rclone.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    pages_raw = session_payload.get("pages")
    summary_raw = session_payload.get("summary_lines")
    if not isinstance(pages_raw, list) or not pages_raw:
        RCLONE_OUTPUT_SESSIONS.pop(token, None)
        await callback_query.answer("Output /rclone tidak tersedia lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Output /rclone tidak tersedia lagi.",
            reply_markup=None,
        )
        return

    output_pages = [str(page) for page in pages_raw]
    summary_lines = (
        [str(line) for line in summary_raw]
        if isinstance(summary_raw, list)
        else ["Hasil /rclone."]
    )
    safe_page = max(0, min(requested_page, len(output_pages) - 1))
    session_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS

    await update_status_message(
        status_message,
        status_message,
        build_rclone_output_text(summary_lines, output_pages, page=safe_page),
        reply_markup=build_rclone_output_keyboard(
            token=token,
            total_pages=len(output_pages),
            page=safe_page,
        ),
    )
    await callback_query.answer()


@app.on_callback_query(filters.regex(r"^u1page\|"))
async def u1_file_page_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, page_text = parts
    try:
        requested_page = int(page_text)
    except ValueError:
        await callback_query.answer("Nomor halaman tidak valid.", show_alert=True)
        return

    cleanup_expired_u1_file_pick_sessions()
    session_payload = U1_FILE_PICK_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Pilihan file /u1 sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /u1.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    source_paths = resolve_u1_file_pick_paths(session_payload)
    if not source_paths:
        U1_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("File sumber tidak tersedia lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan file /u1 dibatalkan: file sumber sudah tidak tersedia.",
            reply_markup=None,
        )
        return

    session_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
    session_payload["files"] = [str(path) for path in source_paths]
    page_count = max(1, (len(source_paths) + U1_PICKER_PAGE_SIZE - 1) // U1_PICKER_PAGE_SIZE)
    safe_page = max(0, min(requested_page, page_count - 1))
    source_input = str(session_payload.get("source_input") or "").strip()
    target_chat = session_payload.get("target_chat", status_message.chat.id)
    await update_status_message(
        status_message,
        status_message,
        build_u1_file_picker_text(
            source_input=source_input,
            target_chat=target_chat,
            source_paths=source_paths,
            page=safe_page,
        ),
        reply_markup=build_u1_file_picker_keyboard(
            token=token,
            total_files=len(source_paths),
            page=safe_page,
        ),
    )
    await callback_query.answer()


@app.on_callback_query(filters.regex(r"^u1pick\|"))
async def u1_file_pick_callback(client: Client, callback_query):
    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, pick_value = parts
    cleanup_expired_u1_file_pick_sessions()
    session_payload = U1_FILE_PICK_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Pilihan file /u1 sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /u1.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    if pick_value == "cancel":
        U1_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Pemilihan file /u1 dibatalkan.")
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan file /u1 dibatalkan.",
            reply_markup=None,
        )
        return

    try:
        selected_index = int(pick_value)
    except ValueError:
        await callback_query.answer("Nomor file tidak valid.", show_alert=True)
        return

    source_paths = resolve_u1_file_pick_paths(session_payload)
    if not source_paths:
        U1_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("File sumber tidak tersedia lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan file /u1 dibatalkan: file sumber sudah tidak tersedia.",
            reply_markup=None,
        )
        return

    session_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
    if selected_index < 0 or selected_index >= len(source_paths):
        await callback_query.answer("Nomor file di luar daftar.", show_alert=True)
        return

    selected_path = source_paths[selected_index]
    target_chat = session_payload.get("target_chat", status_message.chat.id)
    if selected_path.is_dir():
        folder_mode_token = register_u1_folder_mode_session(
            requester_id=requester_id if isinstance(requester_id, int) else getattr(actor, "id", None),
            target_chat=target_chat,
            folder_path=selected_path,
            source_input=str(selected_path),
        )
        U1_FILE_PICK_SESSIONS.pop(token, None)
        if not folder_mode_token:
            await callback_query.answer("Gagal menyiapkan mode folder.", show_alert=True)
            await update_status_message(
                status_message,
                status_message,
                "Folder dipilih, tetapi sesi mode folder gagal dibuat.",
                reply_markup=None,
            )
            return

        await callback_query.answer(f"Folder #{selected_index + 1} dipilih.")
        await update_status_message(
            status_message,
            status_message,
            "Folder /u1 dipilih.\n"
            f"Nomor: `{selected_index + 1}`\n"
            f"Folder: `{selected_path}`\n"
            f"Target Telegram: `{target_label(target_chat)}`\n\n"
            "Pilih mode upload folder:\n"
            "1. Upload semua file beserta folder\n"
            "2. Upload semua file kecuali folder\n"
            "3. Upload single file",
            reply_markup=build_u1_folder_mode_keyboard(folder_mode_token),
        )
        return

    upload_token = register_aria2_upload_job(
        requester_id=requester_id if isinstance(requester_id, int) else getattr(actor, "id", None),
        requester_name=telegram_user_display_name(actor),
        chat_id=target_chat,
        files=[selected_path],
        target_dir=selected_path.parent,
        source_label="u1",
    )
    U1_FILE_PICK_SESSIONS.pop(token, None)

    if not upload_token:
        await callback_query.answer("Gagal menyiapkan sesi upload.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Gagal menyiapkan sesi upload untuk file terpilih.",
            reply_markup=None,
        )
        return

    await callback_query.answer(f"File #{selected_index + 1} dipilih.")
    await update_status_message(
        status_message,
        status_message,
        "File /u1 dipilih.\n"
        f"Nomor: `{selected_index + 1}`\n"
        f"Nama: `{selected_path.name}`\n"
        f"Sumber: `{selected_path}`\n"
        f"Target Telegram: `{target_label(target_chat)}`\n\n"
        "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox / rclone Dropbox.\n"
        f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
        reply_markup=build_aria2_upload_keyboard(upload_token),
    )


@app.on_callback_query(filters.regex(r"^u1fmode\|"))
async def u1_folder_mode_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, mode = parts
    cleanup_expired_u1_folder_mode_sessions()
    session_payload = U1_FOLDER_MODE_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Mode folder /u1 sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /u1.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    if mode == "cancel":
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        await callback_query.answer("Pemilihan mode folder dibatalkan.")
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan mode folder /u1 dibatalkan.",
            reply_markup=None,
        )
        return

    valid_modes = {"all_with_folder", "all_no_folder", "single_file"}
    if mode not in valid_modes:
        await callback_query.answer("Mode folder tidak dikenal.", show_alert=True)
        return

    folder_path_raw = str(session_payload.get("folder_path") or "").strip()
    if not folder_path_raw:
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        await callback_query.answer("Folder sumber tidak tersedia.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Folder sumber /u1 tidak tersedia lagi.",
            reply_markup=None,
        )
        return

    try:
        folder_path = local_path_from_text(folder_path_raw)
    except Exception as e:
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        await callback_query.answer("Path folder tidak valid.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            f"Path folder /u1 tidak valid: `{e}`",
            reply_markup=None,
        )
        return

    if not folder_path.exists() or not folder_path.is_dir():
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        await callback_query.answer("Folder tidak ditemukan.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            f"Folder /u1 tidak ditemukan:\n`{folder_path}`",
            reply_markup=None,
        )
        return

    target_chat = session_payload.get("target_chat", status_message.chat.id)
    source_input = str(session_payload.get("source_input") or folder_path).strip()

    if mode == "all_with_folder":
        source_paths, collect_error = collect_folder_files(folder_path, recursive=True)
    elif mode == "all_no_folder":
        source_paths, collect_error = collect_folder_files(folder_path, recursive=False)
    else:
        source_paths, collect_error = collect_folder_files(folder_path, recursive=True)

    if collect_error:
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        await callback_query.answer("Gagal membaca isi folder.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            f"Gagal membaca folder /u1:\n`{collect_error}`",
            reply_markup=None,
        )
        return

    if not source_paths:
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        await callback_query.answer("Tidak ada file di folder.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Folder tidak berisi file yang bisa diupload.",
            reply_markup=None,
        )
        return

    if mode == "single_file":
        picker_token = register_u1_file_pick_session(
            requester_id=requester_id if isinstance(requester_id, int) else getattr(actor, "id", None),
            command_chat_id=status_message.chat.id,
            target_chat=target_chat,
            source_input=source_input,
            files=source_paths,
        )
        U1_FOLDER_MODE_SESSIONS.pop(token, None)
        if not picker_token:
            await callback_query.answer("Gagal menyiapkan daftar file.", show_alert=True)
            await update_status_message(
                status_message,
                status_message,
                "Gagal menyiapkan daftar file untuk mode single file.",
                reply_markup=None,
            )
            return

        picker_payload = U1_FILE_PICK_SESSIONS.get(picker_token)
        if not picker_payload:
            await callback_query.answer("Sesi pemilihan file tidak ditemukan.", show_alert=True)
            await update_status_message(
                status_message,
                status_message,
                "Sesi pemilihan file /u1 tidak ditemukan.",
                reply_markup=None,
            )
            return

        prepared_paths = [item for item in resolve_u1_file_pick_paths(picker_payload) if item.is_file()]
        if not prepared_paths:
            U1_FILE_PICK_SESSIONS.pop(picker_token, None)
            await callback_query.answer("File tidak tersedia lagi.", show_alert=True)
            await update_status_message(
                status_message,
                status_message,
                "File sumber tidak tersedia lagi untuk mode single file.",
                reply_markup=None,
            )
            return

        picker_payload["files"] = [str(path) for path in prepared_paths]
        await callback_query.answer("Mode single file dipilih.")
        await update_status_message(
            status_message,
            status_message,
            build_u1_file_picker_text(
                source_input=source_input,
                target_chat=target_chat,
                source_paths=prepared_paths,
                page=0,
            ),
            reply_markup=build_u1_file_picker_keyboard(
                token=picker_token,
                total_files=len(prepared_paths),
                page=0,
            ),
        )
        return

    upload_token = register_aria2_upload_job(
        requester_id=requester_id if isinstance(requester_id, int) else getattr(actor, "id", None),
        requester_name=telegram_user_display_name(actor),
        chat_id=target_chat,
        files=source_paths,
        target_dir=folder_path,
        source_label="u1",
    )
    U1_FOLDER_MODE_SESSIONS.pop(token, None)
    if not upload_token:
        await callback_query.answer("Gagal menyiapkan sesi upload.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Gagal menyiapkan sesi upload untuk mode folder.",
            reply_markup=None,
        )
        return

    mode_text = u1_folder_mode_label(mode)
    await callback_query.answer(f"Mode dipilih: {mode_text}")
    await update_status_message(
        status_message,
        status_message,
        "Mode folder /u1 dipilih.\n"
        f"Mode: `{mode_text}`\n"
        f"Folder: `{folder_path}`\n"
        f"Total file: `{len(source_paths)}`\n"
        f"Target Telegram: `{target_label(target_chat)}`\n\n"
        "Pilih upload lanjutan via tombol: Telegram / rclone Google Drive / rclone Terabox / rclone Dropbox.\n"
        f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
        reply_markup=build_aria2_upload_keyboard(upload_token),
    )


@app.on_callback_query(filters.regex(r"^xpick\|"))
async def extract_mode_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, action = parts
    cleanup_expired_extract_pick_sessions()
    session_payload = EXTRACT_PICK_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Pilihan extract sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /extract.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    if action == "cancel":
        EXTRACT_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Menu /extract dibatalkan.")
        await update_status_message(
            status_message,
            status_message,
            "Menu /extract dibatalkan.",
            reply_markup=None,
        )
        return

    if action not in EXTRACT_MODES:
        await callback_query.answer("Mode extract tidak dikenal.", show_alert=True)
        return

    source_inputs_raw = session_payload.get("source_inputs")
    source_inputs = (
        [str(item).strip() for item in source_inputs_raw if str(item).strip()]
        if isinstance(source_inputs_raw, list)
        else []
    )
    if not source_inputs:
        EXTRACT_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Input arsip tidak tersedia.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Input arsip /extract tidak tersedia lagi.",
            reply_markup=None,
        )
        return

    target_dir_raw = str(session_payload.get("target_dir") or str(extract_root)).strip()
    try:
        target_dir = local_path_from_text(target_dir_raw)
    except Exception:
        target_dir = extract_root

    EXTRACT_PICK_SESSIONS.pop(token, None)
    candidate_paths, resolve_errors = resolve_extract_candidates(source_inputs)
    matching_files: List[Path] = []
    skipped_lines: List[str] = []
    mode_display = extract_mode_label(action)
    for candidate_path in candidate_paths:
        if not archive_matches_extract_mode(candidate_path, action):
            continue
        if not path_exists_or_symlink(candidate_path):
            skipped_lines.append(f"- `{candidate_path}` -> path tidak ditemukan")
            continue
        if candidate_path.is_dir():
            skipped_lines.append(f"- `{candidate_path}` -> bukan file arsip")
            continue
        matching_files.append(candidate_path)

    if not matching_files:
        summary_lines = [
            "Tidak ada arsip yang cocok untuk mode terpilih.",
            f"Mode: `{mode_display}`",
            f"Target folder: `{target_dir}`",
            f"Sumber input: `{len(source_inputs)}`",
            f"Kandidat: `{len(candidate_paths)}`",
            f"Cocok: `0`",
        ]
        if resolve_errors:
            summary_lines.append("")
            summary_lines.append("Error input:")
            summary_lines.extend(resolve_errors[:10])
            if len(resolve_errors) > 10:
                summary_lines.append(f"... {len(resolve_errors) - 10} error lain.")
        if skipped_lines:
            summary_lines.append("")
            summary_lines.append("Diskip:")
            summary_lines.extend(skipped_lines[:10])
            if len(skipped_lines) > 10:
                summary_lines.append(f"... {len(skipped_lines) - 10} item lain.")
        await callback_query.answer("Tidak ada file untuk mode ini.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            trim_output("\n".join(summary_lines)),
            reply_markup=None,
        )
        return

    picker_token = register_extract_file_pick_session(
        requester_id=requester_id if isinstance(requester_id, int) else getattr(actor, "id", None),
        extract_mode=action,
        target_dir=target_dir,
        files=matching_files,
    )
    if not picker_token:
        await callback_query.answer("Gagal menyiapkan daftar file.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Gagal menyiapkan daftar file /extract.",
            reply_markup=None,
        )
        return

    picker_payload = EXTRACT_FILE_PICK_SESSIONS.get(picker_token)
    if not picker_payload:
        await callback_query.answer("Sesi pemilihan file tidak ditemukan.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Sesi pemilihan file /extract tidak ditemukan.",
            reply_markup=None,
        )
        return

    prepared_paths = resolve_extract_file_pick_paths(picker_payload)
    if not prepared_paths:
        EXTRACT_FILE_PICK_SESSIONS.pop(picker_token, None)
        await callback_query.answer("Arsip tidak tersedia lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Arsip sumber /extract tidak tersedia lagi saat membuat tombol pilihan.",
            reply_markup=None,
        )
        return

    picker_payload["files"] = [str(path) for path in prepared_paths]
    picker_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS

    note_lines = []
    if resolve_errors:
        note_lines.append(f"Catatan: `{len(resolve_errors)}` input tidak valid.")
    if skipped_lines:
        note_lines.append(f"Catatan: `{len(skipped_lines)}` kandidat diskip.")
    picker_text = build_extract_file_picker_text(
        extract_mode=action,
        target_dir=target_dir,
        source_paths=prepared_paths,
        page=0,
    )
    if note_lines:
        picker_text = trim_output(f"{picker_text}\n\n" + "\n".join(note_lines))

    await callback_query.answer(f"Mode {mode_display} dipilih.")
    await update_status_message(
        status_message,
        status_message,
        picker_text,
        reply_markup=build_extract_file_picker_keyboard(
            token=picker_token,
            total_files=len(prepared_paths),
            page=0,
        ),
    )


@app.on_callback_query(filters.regex(r"^xpage\|"))
async def extract_file_page_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, page_text = parts
    try:
        requested_page = int(page_text)
    except ValueError:
        await callback_query.answer("Nomor halaman tidak valid.", show_alert=True)
        return

    cleanup_expired_extract_file_pick_sessions()
    session_payload = EXTRACT_FILE_PICK_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Pilihan file /extract sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /extract.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    source_paths = resolve_extract_file_pick_paths(session_payload)
    if not source_paths:
        EXTRACT_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Arsip sumber tidak tersedia lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan file /extract dibatalkan: arsip sumber sudah tidak tersedia.",
            reply_markup=None,
        )
        return

    extract_mode = str(session_payload.get("extract_mode") or "").strip().lower()
    target_dir_raw = str(session_payload.get("target_dir") or str(extract_root)).strip()
    try:
        target_dir = local_path_from_text(target_dir_raw)
    except Exception:
        target_dir = extract_root

    session_payload["files"] = [str(path) for path in source_paths]
    session_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
    page_count = max(1, (len(source_paths) + EXTRACT_PICKER_PAGE_SIZE - 1) // EXTRACT_PICKER_PAGE_SIZE)
    safe_page = max(0, min(requested_page, page_count - 1))

    await update_status_message(
        status_message,
        status_message,
        build_extract_file_picker_text(
            extract_mode=extract_mode,
            target_dir=target_dir,
            source_paths=source_paths,
            page=safe_page,
        ),
        reply_markup=build_extract_file_picker_keyboard(
            token=token,
            total_files=len(source_paths),
            page=safe_page,
        ),
    )
    await callback_query.answer()


@app.on_callback_query(filters.regex(r"^xfile\|"))
async def extract_file_pick_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, pick_value = parts
    cleanup_expired_extract_file_pick_sessions()
    session_payload = EXTRACT_FILE_PICK_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Pilihan file /extract sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /extract.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    if pick_value == "cancel":
        EXTRACT_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Pemilihan file /extract dibatalkan.")
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan file /extract dibatalkan.",
            reply_markup=None,
        )
        return

    try:
        selected_index = int(pick_value)
    except ValueError:
        await callback_query.answer("Nomor file tidak valid.", show_alert=True)
        return

    source_paths = resolve_extract_file_pick_paths(session_payload)
    if not source_paths:
        EXTRACT_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Arsip sumber tidak tersedia lagi.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Pemilihan file /extract dibatalkan: arsip sumber sudah tidak tersedia.",
            reply_markup=None,
        )
        return

    extract_mode = str(session_payload.get("extract_mode") or "").strip().lower()
    if extract_mode not in EXTRACT_MODES:
        EXTRACT_FILE_PICK_SESSIONS.pop(token, None)
        await callback_query.answer("Mode extract tidak valid.", show_alert=True)
        await update_status_message(
            status_message,
            status_message,
            "Mode extract pada sesi tidak valid.",
            reply_markup=None,
        )
        return

    target_dir_raw = str(session_payload.get("target_dir") or str(extract_root)).strip()
    try:
        target_dir = local_path_from_text(target_dir_raw)
    except Exception:
        target_dir = extract_root

    session_payload["expires_at"] = time.time() + ARIA2_BUTTON_TTL_SECONDS
    if selected_index < 0 or selected_index >= len(source_paths):
        await callback_query.answer("Nomor file di luar daftar.", show_alert=True)
        return

    selected_path = source_paths[selected_index]
    EXTRACT_FILE_PICK_SESSIONS.pop(token, None)

    await callback_query.answer(f"File #{selected_index + 1} dipilih.")
    await update_status_message(
        status_message,
        status_message,
        "Memproses ekstrak arsip...\n"
        f"Mode: `{extract_mode_label(extract_mode)}`\n"
        f"Nomor: `{selected_index + 1}`\n"
        f"Nama: `{selected_path.name}`\n"
        f"Sumber: `{selected_path}`\n"
        f"Target folder: `{target_dir}`",
        reply_markup=None,
    )
    await execute_extract_operation(
        command_message=status_message,
        status_message=status_message,
        source_inputs=[str(selected_path)],
        extract_mode=extract_mode,
        target_dir=target_dir,
        requester_id=requester_id if isinstance(requester_id, int) else getattr(actor, "id", None),
    )


@app.on_callback_query(filters.regex(r"^xdel\|"))
async def extract_delete_confirm_callback(client: Client, callback_query):
    del client

    payload = callback_query.data or ""
    parts = payload.split("|", 2)
    if len(parts) != 3:
        await callback_query.answer("Data tombol tidak valid.", show_alert=True)
        return

    _, token, action = parts
    cleanup_expired_extract_delete_confirm_sessions()
    session_payload = EXTRACT_DELETE_CONFIRM_SESSIONS.get(token)
    if not session_payload:
        await callback_query.answer("Konfirmasi hapus arsip sudah kedaluwarsa.", show_alert=True)
        return

    actor = getattr(callback_query, "from_user", None)
    requester_id = session_payload.get("requester_id")
    owner_override = bool(BOT_MODE and OWNER_USER_ID and actor and actor.id == OWNER_USER_ID)
    if isinstance(requester_id, int) and actor and actor.id != requester_id and not owner_override:
        await callback_query.answer("Tombol ini hanya untuk requester /extract.", show_alert=True)
        return

    status_message = callback_query.message
    if not status_message:
        await callback_query.answer("Pesan tombol tidak ditemukan.", show_alert=True)
        return

    base_summary_text = str(session_payload.get("summary_text") or "").strip()
    if not base_summary_text:
        base_summary_text = "extract selesai."

    if action == "no":
        EXTRACT_DELETE_CONFIRM_SESSIONS.pop(token, None)
        await callback_query.answer("Arsip sumber tetap disimpan.")
        await update_status_message(
            status_message,
            status_message,
            trim_output(
                f"{base_summary_text}\n\n"
                "Pilihan hapus arsip: `Tidak`.\n"
                "File arsip sumber tidak dihapus."
            ),
            reply_markup=None,
        )
        return

    if action != "yes":
        await callback_query.answer("Pilihan tombol tidak dikenal.", show_alert=True)
        return

    archive_paths_raw = session_payload.get("archive_paths")
    total_targets = len(archive_paths_raw) if isinstance(archive_paths_raw, list) else 0
    archive_paths = resolve_extract_delete_confirm_paths(session_payload)

    deleted_lines: List[str] = []
    failed_lines: List[str] = []
    for archive_path in archive_paths:
        if archive_path.is_dir():
            failed_lines.append(f"- `{archive_path}` -> path folder tidak dihapus")
            continue
        try:
            archive_path.unlink()
            deleted_lines.append(f"- `{archive_path.name}`")
        except Exception as e:
            failed_lines.append(f"- `{archive_path}` -> {e}")

    EXTRACT_DELETE_CONFIRM_SESSIONS.pop(token, None)
    missing_count = max(0, total_targets - len(archive_paths))

    summary_lines = [
        base_summary_text,
        "",
        "Pilihan hapus arsip: `Ya`.",
        f"Target arsip: `{total_targets}`",
        f"Berhasil dihapus: `{len(deleted_lines)}`",
        f"Gagal dihapus: `{len(failed_lines)}`",
    ]
    if missing_count:
        summary_lines.append(f"Tidak ditemukan saat proses hapus: `{missing_count}`")

    if deleted_lines:
        summary_lines.append("")
        summary_lines.append("Daftar terhapus:")
        summary_lines.extend(deleted_lines[:20])
        if len(deleted_lines) > 20:
            summary_lines.append(f"... {len(deleted_lines) - 20} arsip lain.")

    if failed_lines:
        summary_lines.append("")
        summary_lines.append("Daftar gagal hapus:")
        summary_lines.extend(failed_lines[:20])
        if len(failed_lines) > 20:
            summary_lines.append(f"... {len(failed_lines) - 20} item lain.")

    await callback_query.answer("Proses hapus arsip selesai.")
    await update_status_message(
        status_message,
        status_message,
        trim_output("\n".join(summary_lines)),
        reply_markup=None,
    )


@app.on_message(command_filter("u1", allow_public=True))
async def upload_command(client: Client, message):
    del client

    if not await require_public_command_access(message, "u1"):
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
    default_upload_pattern = str(upload_root / "*")

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
        source_path_text = default_upload_pattern

    source_paths, resolve_error = resolve_upload_sources(source_path_text)
    if resolve_error:
        await open_status_message(message, resolve_error)
        return

    if target_chat == "me":
        target_chat = message.chat.id

    picker_token = register_u1_file_pick_session(
        requester_id=getattr(message.from_user, "id", None),
        command_chat_id=message.chat.id,
        target_chat=target_chat,
        source_input=source_path_text,
        files=source_paths,
    )
    if not picker_token:
        await open_status_message(
            message,
            "Gagal menyiapkan daftar path /u1. Pastikan file/folder masih ada dan bisa diakses.",
        )
        return

    picker_payload = U1_FILE_PICK_SESSIONS.get(picker_token)
    if not picker_payload:
        await open_status_message(message, "Sesi pemilihan file /u1 tidak ditemukan.")
        return

    prepared_paths = resolve_u1_file_pick_paths(picker_payload)
    if not prepared_paths:
        U1_FILE_PICK_SESSIONS.pop(picker_token, None)
        await open_status_message(
            message,
            "Path sumber /u1 tidak tersedia lagi saat membuat tombol pilihan.",
        )
        return

    picker_payload["files"] = [str(path) for path in prepared_paths]
    await open_status_message(
        message,
        build_u1_file_picker_text(
            source_input=source_path_text,
            target_chat=target_chat,
            source_paths=prepared_paths,
            page=0,
        ),
        reply_markup=build_u1_file_picker_keyboard(
            token=picker_token,
            total_files=len(prepared_paths),
            page=0,
        ),
    )


@app.on_message(command_filter("ps", allow_public=True))
async def ps_command(client: Client, message):
    del client

    if not await require_public_command_access(message, "ps"):
        return

    args = command_args(message)
    if not args:
        await open_status_message(
            message,
            "Format:\n"
            "`/ps <nama_proses|pid>`\n"
            "Contoh:\n"
            "`/ps aria2`\n"
            "`/ps 12345`",
        )
        return

    query = " ".join(args).strip()
    if not query:
        await open_status_message(message, "Query `/ps` tidak boleh kosong.")
        return

    status_message = await open_status_message(
        message,
        f"Mencari proses dengan query: `{query}` ...",
    )

    try:
        process = await asyncio.create_subprocess_exec(
            "ps",
            "aux",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        await update_status_message(
            message,
            status_message,
            "Command `ps` tidak ditemukan di sistem.",
        )
        return
    except Exception as e:
        await update_status_message(
            message,
            status_message,
            f"Gagal menjalankan `ps aux`: `{e}`",
        )
        return

    stdout_raw, stderr_raw = await process.communicate()
    stdout_text = stdout_raw.decode("utf-8", errors="ignore").strip()
    stderr_text = stderr_raw.decode("utf-8", errors="ignore").strip()
    if process.returncode != 0:
        error_text = stderr_text or stdout_text or f"return code {process.returncode}"
        await update_status_message(
            message,
            status_message,
            trim_output(f"Gagal menjalankan `ps aux`: `{error_text}`"),
        )
        return

    lines = stdout_text.splitlines()
    if not lines:
        await update_status_message(
            message,
            status_message,
            "Output `ps aux` kosong.",
        )
        return

    header = lines[0]
    query_lower = query.lower()
    query_is_pid = query.isdigit()
    matched_lines: List[str] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        lowered = line.lower()
        if query_lower in lowered:
            matched_lines.append(line)
            continue
        if query_is_pid:
            parts = line.split(None, 2)
            if len(parts) >= 2 and parts[1] == query:
                matched_lines.append(line)

    if not matched_lines:
        await update_status_message(
            message,
            status_message,
            "Tidak ada proses yang cocok.\n"
            f"Query: `{query}`",
        )
        return

    max_lines = 40
    shown_lines = matched_lines[:max_lines]
    output_lines = [
        f"Hasil `ps aux | grep {query}`",
        f"Total cocok: `{len(matched_lines)}`",
        "```text",
        header,
        *shown_lines,
        "```",
    ]
    if len(matched_lines) > max_lines:
        output_lines.append(f"Catatan: ditampilkan {max_lines} dari {len(matched_lines)} proses.")

    await update_status_message(
        message,
        status_message,
        trim_output("\n".join(output_lines)),
    )


@app.on_message(command_filter("pkill"))
async def pkill_command(client: Client, message):
    del client

    if not await require_whitelist_admin_access(message, "pkill"):
        return

    args = command_args(message)
    if not args:
        await open_status_message(
            message,
            "Format pkill:\n"
            "`/pkill <pattern>`\n"
            "`/pkill --signal TERM <pattern>`\n"
            "`/pkill -9 <pattern>`",
        )
        return

    signal_value = ""
    pattern_parts = list(args)

    if pattern_parts and pattern_parts[0] in {"-s", "--signal"}:
        if len(pattern_parts) < 3:
            await open_status_message(
                message,
                "Format signal tidak valid.\n"
                "Contoh: `/pkill --signal TERM aria2c`",
            )
            return
        signal_value = pattern_parts[1].strip().lstrip("-")
        pattern_parts = pattern_parts[2:]
    elif pattern_parts and pattern_parts[0].startswith("-") and len(pattern_parts[0]) > 1:
        signal_value = pattern_parts[0][1:].strip()
        pattern_parts = pattern_parts[1:]

    pattern_text = " ".join(pattern_parts).strip()
    if not pattern_text:
        await open_status_message(
            message,
            "Pattern proses tidak boleh kosong.\n"
            "Contoh: `/pkill aria2c`",
        )
        return

    command = ["pkill"]
    if signal_value:
        command.append(f"-{signal_value}")
    command.extend(["-f", pattern_text])

    status_message = await open_status_message(
        message,
        "Menjalankan pkill...\n"
        f"Signal: `{signal_value or 'default (TERM)'}`\n"
        f"Pattern: `{pattern_text}`",
    )

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
            "Command `pkill` tidak ditemukan di sistem.",
        )
        return
    except Exception as e:
        await update_status_message(
            message,
            status_message,
            f"Gagal menjalankan pkill: `{e}`",
        )
        return

    stdout_raw, stderr_raw = await process.communicate()
    stdout_text = stdout_raw.decode("utf-8", errors="ignore").strip()
    stderr_text = stderr_raw.decode("utf-8", errors="ignore").strip()

    result_lines = [
        "pkill selesai.",
        f"Signal: `{signal_value or 'default (TERM)'}`",
        f"Pattern: `{pattern_text}`",
        f"Return code: `{process.returncode}`",
    ]
    if process.returncode == 0:
        result_lines.append("Status: proses cocok ditemukan dan sinyal dikirim.")
    elif process.returncode == 1:
        result_lines.append("Status: tidak ada proses yang cocok.")
    else:
        result_lines.append("Status: gagal mengeksekusi pkill.")

    if stderr_text:
        result_lines.extend(["", "stderr:", "```text", stderr_text[-800:], "```"])
    elif stdout_text:
        result_lines.extend(["", "stdout:", "```text", stdout_text[-800:], "```"])

    await update_status_message(message, status_message, trim_output("\n".join(result_lines)))


@app.on_message(command_filter("rclone"))
async def rclone_command(client: Client, message):
    del client

    if not await require_owner_or_whitelist_access(message, "rclone"):
        return

    args = command_args(message)
    if not args:
        await open_status_message(
            message,
            "Format:\n"
            "`/rclone <subcommand> [opsi]`\n"
            "Contoh:\n"
            "`/rclone ls terabox:Mirror`\n"
            "`/rclone copy /home/runner/downloads terabox:Mirror --progress --transfers 8`\n"
            "`/rclone move terabox:Mirror/file.zip gdrive:Backup/`",
        )
        return

    command = [RCLONE_BIN, *args]
    command_preview = " ".join(shlex.quote(item) for item in command)
    command_preview_display = trim_output(command_preview)
    if len(command_preview_display) > 600:
        command_preview_display = f"{command_preview_display[:597]}..."
    status_message = await open_status_message(
        message,
        "Menjalankan rclone...\n"
        f"Timeout: `{format_duration(RCLONE_COMMAND_TIMEOUT_SECONDS)}`\n"
        f"Command: `{command_preview_display}`",
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
            f"Binary rclone tidak ditemukan.\nSet env `RCLONE_BIN` (saat ini: `{RCLONE_BIN}`).",
        )
        return
    except Exception as e:
        await update_status_message(
            message,
            status_message,
            f"Gagal menjalankan rclone: `{e}`",
        )
        return

    timed_out = False
    try:
        stdout_raw, stderr_raw = await asyncio.wait_for(
            process.communicate(),
            timeout=RCLONE_COMMAND_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        timed_out = True
        process.kill()
        stdout_raw, stderr_raw = await process.communicate()

    stdout_text = stdout_raw.decode("utf-8", errors="ignore").strip()
    stderr_text = stderr_raw.decode("utf-8", errors="ignore").strip()
    elapsed_text = format_duration(int(max(0, time.time() - started_at)))

    result_lines = [
        "rclone selesai." if not timed_out else "rclone dihentikan karena timeout.",
        f"Durasi: `{elapsed_text}`",
        f"Command: `{command_preview_display}`",
        f"Return code: `{process.returncode}`",
    ]
    if timed_out:
        result_lines.append(
            f"Status: timeout setelah `{format_duration(RCLONE_COMMAND_TIMEOUT_SECONDS)}`."
        )
    elif process.returncode == 0:
        result_lines.append("Status: berhasil.")
    else:
        result_lines.append("Status: gagal.")

    output_sections: List[str] = []
    if stderr_text:
        output_sections.append(f"[stderr]\n{stderr_text}")
    if stdout_text:
        output_sections.append(f"[stdout]\n{stdout_text}")

    combined_output = "\n\n".join(output_sections).strip()
    if not combined_output:
        await update_status_message(message, status_message, trim_output("\n".join(result_lines)))
        return

    summary_text = "\n".join(result_lines)
    page_body_limit = max(500, min(RCLONE_PAGE_BODY_CHARS, LIST_MAX_CHARS - len(summary_text) - 180))
    output_pages = split_text_into_pages(combined_output, max_chars=page_body_limit)
    if not output_pages:
        await update_status_message(message, status_message, trim_output("\n".join(result_lines)))
        return

    if len(output_pages) <= 1:
        await update_status_message(
            message,
            status_message,
            trim_output(build_rclone_output_text(result_lines, output_pages, page=0)),
        )
        return

    output_token = register_rclone_output_session(
        requester_id=getattr(message.from_user, "id", None),
        summary_lines=result_lines,
        output_pages=output_pages,
    )
    if not output_token:
        await update_status_message(
            message,
            status_message,
            trim_output(build_rclone_output_text(result_lines, output_pages, page=0)),
        )
        return

    await update_status_message(
        message,
        status_message,
        build_rclone_output_text(result_lines, output_pages, page=0),
        reply_markup=build_rclone_output_keyboard(
            token=output_token,
            total_pages=len(output_pages),
            page=0,
        ),
    )


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

    if not await require_owner_or_whitelist_access(message, "ls"):
        return

    args = command_args(message)
    show_all = False
    options_ended = False
    path_parts = []
    for arg in args:
        if not options_ended and arg == "--":
            options_ended = True
            continue
        if not options_ended and arg.startswith("-"):
            normalized = arg.lower()
            if normalized == "--all" or "a" in normalized.lstrip("-"):
                show_all = True
            # Opsi lain diabaikan agar kompatibel dengan gaya shell (/ls -lh, dll).
            continue
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

    if not await require_owner_or_whitelist_access(message, "du"):
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

    if not await require_owner_or_whitelist_access(message, "df"):
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

    if not await require_owner_or_whitelist_access(message, "rm"):
        return

    raw_args = command_args(message)
    options_ended = False
    args = []
    for arg in raw_args:
        if not options_ended and arg == "--":
            options_ended = True
            continue
        if not options_ended and arg.startswith("-"):
            # Kompatibilitas: terima opsi seperti -r/-f/-rf, lalu abaikan.
            continue
        args.append(arg)

    if not args:
        await open_status_message(
            message,
            "Format:\n"
            "`/rm /home/runner/uploads/file.txt`\n"
            "`/rm /home/runner/uploads/tmp-folder`\n"
            "`/rm *.tmp`\n"
            "Catatan: opsi seperti `-rf` boleh dipakai untuk kompatibilitas."
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


@app.on_message(command_filter(["copy", "cp"]))
async def copy_command(client: Client, message):
    del client

    if not await require_owner_or_whitelist_access(message, "copy"):
        return

    args = command_args(message)
    if len(args) != 2:
        await open_status_message(
            message,
            "Format:\n"
            "`/copy /home/runner/uploads/a.txt /home/runner/backup/a.txt`\n"
            "`/cp /home/runner/uploads/a.txt /home/runner/backup/a.txt`\n"
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

    if not await require_owner_or_whitelist_access(message, "mv"):
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


@app.on_message(command_filter("extract"))
async def extract_archive_command(client: Client, message):
    del client

    if not await require_owner_or_whitelist_access(message, "extract"):
        return

    args = command_args(message)
    source_inputs = [str(item).strip() for item in args if str(item).strip()]
    reply_message = message.reply_to_message
    if not source_inputs and reply_message:
        replied_text = (reply_message.text or reply_message.caption or "").strip()
        if replied_text:
            source_inputs = [replied_text]

    used_default_source = False
    if not source_inputs:
        source_inputs = [str(extract_root / "*")]
        used_default_source = True

    picker_token = register_extract_pick_session(
        requester_id=getattr(message.from_user, "id", None),
        source_inputs=source_inputs,
        target_dir=extract_root,
    )
    if not picker_token:
        await open_status_message(
            message,
            "Gagal menyiapkan menu /extract.\n"
            "Coba kirim ulang dengan path arsip, contoh:\n"
            "`/extract /home/runner/downloads/sample.rar`",
        )
        return

    source_preview = ", ".join(f"`{item}`" for item in source_inputs[:3])
    if len(source_inputs) > 3:
        source_preview += f", +{len(source_inputs) - 3} item lain"

    menu_lines = [
        "Menu extract siap.",
        f"Target folder: `{extract_root}`",
        f"Sumber arsip: {source_preview}",
        "",
        "Pilih mode extract:",
        "- `unrar` (unrar x)",
        "- `unzip` (unzip -o)",
        "- `untar` (tar -xzf)",
        "- `un7z` (7z x)",
        "- Setelah pilih mode, pilih file via tombol angka (maks 10 file/halaman).",
        "",
        f"Masa berlaku tombol: `{format_duration(ARIA2_BUTTON_TTL_SECONDS)}`.",
    ]
    if used_default_source:
        menu_lines.append(
            f"Catatan: tanpa argumen, default sumber = `{extract_root / '*'}`."
        )

    await open_status_message(
        message,
        trim_output("\n".join(menu_lines)),
        reply_markup=build_extract_mode_keyboard(picker_token),
    )


if __name__ == "__main__":
    runtime_mode = "BOT TOKEN" if BOT_MODE else "USERBOT SESSION"
    print(f"Manual downloader aktif. Mode: {runtime_mode}")
    if BOT_MODE:
        print(f"Owner ID: {OWNER_USER_ID if OWNER_USER_ID else '(belum diatur)'}")
    print(f"Mode command: {'PUBLIC' if PUBLIC_MODE else 'PRIVATE'}")
    print("Langkah pakai:")
    if BOT_MODE and PUBLIC_MODE:
        print("1. Di chat/group, semua member bisa pakai: /d1 /dstatus /dqueue /ps /aria2 (/a2) /gdl (/gallerydl) /u1.")
        print("2. Untuk /d1: reply file/video/link t.me + /d1, atau langsung /d1 <link t.me>.")
        print("3. Cek antrian download: /dstatus atau /dqueue.")
        print("4. Khusus whitelist/owner (`PKILL_ADMIN_IDS` atau OWNER_USER_ID): /pkill /rclone /ls /rm /du /df /copy (/cp) /mv /extract.")
        print("5. Khusus owner bot (OWNER_USER_ID): /ucancel /mkdir.")
    elif BOT_MODE:
        print("1. Command private tetap hanya owner (OWNER_USER_ID).")
        print("2. /pkill /rclone /ls /rm /du /df /copy (/cp) /mv /extract untuk user whitelist `PKILL_ADMIN_IDS` (owner juga bisa).")
        print("3. Public command nonaktif. Aktifkan PUBLIC_MODE=1 untuk membuka /d1 /dstatus /dqueue /ps /aria2 /gdl /u1.")
    elif PUBLIC_MODE:
        print("1. Di chat/group, semua member bisa pakai: /d1 /dstatus /dqueue /ps /aria2 (/a2) /gdl (/gallerydl) /u1.")
        print("2. Untuk /d1: reply file/video/link t.me + /d1, atau langsung /d1 <link t.me>.")
        print("3. Cek antrian download: /dstatus atau /dqueue.")
        print("4. /pkill /rclone /ls /rm /du /df /copy (/cp) /mv /extract hanya untuk whitelist `PKILL_ADMIN_IDS` (atau owner).")
        print("5. Command owner (Saved Messages): /ucancel /mkdir.")
    else:
        print("1. Forward file/video ATAU kirim link t.me ke Saved Messages.")
        print("2. Reply pesan tersebut dengan /d1, atau kirim /d1 <link t.me>.")
        print("3. Cek antrian download: /dstatus atau /dqueue.")
        print("4. Upload lokal: /u1 /path/file, /u1 /path/folder, /u1 *.txt, /u1 /path/*.mp4 --to @username, atau cukup /u1 (default folder upload).")
    print("- Download external via aria2: /aria2 <url|magnet> (default folder DOWNLOAD_DIR)")
    print("- Di /d1, bot hanya download ke lokal (tanpa tombol upload).")
    print("- Di /aria2, wajib pilih tujuan upload dulu; download baru dimulai setelah pilihan dibuat.")
    print("- Di /gdl, wajib pilih tujuan upload dulu; download baru dimulai setelah pilihan dibuat.")
    print("- Info command: /start atau /help (detail: /help extract, /help aria2, /help gdl, /help u1)")
    print("- Download via gallery-dl (contoh GoFile): /gdl https://gofile.io/d/xxxxx  (default: -d /home/runner/downloads -o directory=\"\")")
    print("- Override manual tetap bisa: /gdl -d /path/target -o directory=\"\" https://gofile.io/d/xxxxx")
    print("- /aria2 dan /gdl juga bisa dipakai sambil reply link direct (http/https/ftp)")
    print("- Di /u1, bot menampilkan daftar file/folder + tombol angka; jika pilih folder akan muncul 3 mode (semua+folder, semua tanpa folder, single file).")
    print(f"- Jika /u1 tanpa argumen, default pola path: `{upload_root / '*'}`")
    print("- Hasil /d1 bisa diupload manual pakai /u1 <path_file>; upload sukses akan hapus file lokal.")
    print("- Untuk /aria2 dan /gdl: jika pilih tujuan upload, bot auto-upload; jika pilih Lewati, upload lanjutan tidak dijalankan.")
    print("- Jika upload gagal, gunakan tombol `Retry Terakhir` atau pilih tujuan upload lagi")
    print(
        f"- Remote rclone: GDrive=`{RCLONE_GDRIVE_REMOTE or '(belum diatur)'}`, "
        f"Terabox=`{RCLONE_TERABOX_REMOTE or '(belum diatur)'}`, "
        f"Dropbox=`{RCLONE_DROPBOX_REMOTE or '(belum diatur)'}`"
    )
    print("- Cek isi direktori: /ls [opsi] [path] (mis. /ls -lh /path, /ls --all /path) [whitelist/owner]")
    print("- Cek disk: /du [opsi] [path] dan /df [opsi] [path] (opsi diterima utk kompatibilitas) [whitelist/owner]")
    print("- Batalkan upload yang sedang berjalan: /ucancel")
    print("- Cek proses: /ps <nama_proses|pid>  (public jika PUBLIC_MODE=1, contoh: /ps aria2)")
    print("- Admin whitelist: /pkill <pattern> (opsional signal: /pkill --signal TERM <pattern>)")
    print("- Admin whitelist: /rclone <subcommand> [opsi] (opsi fleksibel, contoh: /rclone ls remote:)")
    print(
        f"- PKILL_ADMIN_IDS: `{', '.join(str(item) for item in sorted(PKILL_ADMIN_USER_IDS)) or '(kosong)'}`"
    )
    print("- Manajemen file: /mkdir <path> (owner)")
    print("- Copy/move: /copy|/cp <source> <target>, /mv <source> <target> [whitelist/owner]")
    print("- Ekstrak arsip: /extract [path_arsip] -> pilih mode lalu pilih file via tombol angka (10 file/halaman, Prev/Next) ke /home/runner/downloads [whitelist/owner]")
    print("- Hapus file/folder: /rm [opsi] <path> (mis. /rm -rf /path) [whitelist/owner]")
    print(f"- File download disimpan ke: {download_root}")
    print(f"- Default folder upload: {upload_root}")
    app.run()
