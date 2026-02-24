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
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageIdInvalid

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/home/runner/downloads")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/home/runner/uploads")
PROGRESS_INTERVAL = int(os.getenv("PROGRESS_INTERVAL", "5"))

if not API_ID or not API_HASH:
    raise RuntimeError("API_ID dan API_HASH wajib diisi.")

api_id = int(API_ID)
download_root = Path(DOWNLOAD_DIR).expanduser().resolve()
download_root.mkdir(parents=True, exist_ok=True)
download_target = f"{download_root}{os.sep}"
upload_root = Path(UPLOAD_DIR).expanduser().resolve()
upload_root.mkdir(parents=True, exist_ok=True)

if SESSION_STRING:
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

    text = (
        "Download berjalan\n"
        f"File: {media_name}\n"
        f"Progress: {percent:.2f}%\n"
        f"Size: {format_bytes(current)} / {format_bytes(total)}\n"
        f"Speed: {format_bytes(speed)}/s\n"
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


def local_path_from_text(path_text: str) -> Path:
    raw = path_text.strip()
    if raw.startswith("file://"):
        raw = raw[7:]
    path = Path(os.path.expandvars(raw)).expanduser()
    return path.resolve()


def has_wildcard(path_text: str) -> bool:
    return any(char in path_text for char in ("*", "?", "["))


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


def is_filesystem_root(path: Path) -> bool:
    return path == path.parent


def resolve_fs_sources(path_text: str) -> Tuple[List[Path], Optional[str]]:
    raw = path_text.strip()
    if not raw:
        return [], "Path kosong."

    if raw.startswith("file://"):
        raw = raw[7:]

    expanded = os.path.expandvars(raw)
    is_absolute_or_home = expanded.startswith("~") or Path(expanded).is_absolute()
    candidates = [expanded]
    if not is_absolute_or_home:
        candidates.append(str(upload_root / expanded))

    matched_paths: List[Path] = []
    seen_paths = set()
    has_pattern = False

    for candidate in candidates:
        candidate_expanded = os.path.expanduser(candidate)
        if has_wildcard(candidate_expanded):
            has_pattern = True
            raw_matches = sorted(glob.glob(candidate_expanded, recursive=True))
            for item in raw_matches:
                resolved = Path(item).resolve()
                key = str(resolved)
                if key not in seen_paths:
                    seen_paths.add(key)
                    matched_paths.append(resolved)
        else:
            resolved = Path(candidate_expanded).resolve()
            if resolved.exists():
                key = str(resolved)
                if key not in seen_paths:
                    seen_paths.add(key)
                    matched_paths.append(resolved)

    if matched_paths:
        return matched_paths, None

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


def delete_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink()


def copy_path(source: Path, destination: Path) -> Path:
    if source.is_dir() and not source.is_symlink():
        if destination.exists():
            raise FileExistsError("Tujuan folder sudah ada.")
        shutil.copytree(source, destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return destination.resolve()


def move_path(source: Path, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    moved_to = Path(shutil.move(str(source), str(destination)))
    return moved_to.resolve()


def remove_file_quietly(path: Optional[Path]) -> None:
    if not path:
        return
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


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
@app.on_message(filters.me & filters.command("d1", prefixes="/"))
async def download_command(client: Client, message):
    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /d1 hanya di Saved Messages.")
        return

    if not message.reply_to_message:
        await message.edit_text(
            "Balas pesan yang berisi file/video atau link Telegram, lalu kirim /d1."
        )
        return

    await message.edit_text("Memeriksa pesan yang dibalas...")
    target_message, error_message = await resolve_target_message(
        client, message.reply_to_message
    )
    if error_message:
        await message.edit_text(error_message)
        return

    name = media_label(target_message)
    state = {"started_at": time.time(), "last_tick": 0.0}

    await message.edit_text(
        "Memulai download ke storage VPS\n"
        f"Folder: `{download_root}`\n"
        f"File: {name}"
    )

    try:
        file_path = await target_message.download(
            file_name=download_target,
            progress=progress_callback,
            progress_args=(message, name, state),
        )
    except Exception as e:
        await message.edit_text(f"Download gagal: `{e}`")
        return

    if not file_path:
        await message.edit_text("Download gagal: path file kosong.")
        return

    await message.edit_text(
        "Download selesai.\n"
        f"Lokasi: `{file_path}`"
    )


@app.on_message(filters.me & filters.command(["u1", "rclone"], prefixes="/"))
async def upload_command(client: Client, message):
    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /u1 atau /rclone hanya di Saved Messages.")
        return

    running_task = UPLOAD_CONTROL.get("task")
    if running_task and not running_task.done():
        await message.edit_text(
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
            await message.edit_text(
                "Format target tidak valid.\n"
                "Contoh: `/rclone /home/runner/uploads/file.mp4 --to @username`"
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
        await message.edit_text(
            "Format upload:\n"
            "`/rclone /home/runner/uploads/file.mp4`\n"
            "`/rclone *.txt`\n"
            "`/rclone /home/runner/uploads/*.mp4 --to @username`\n"
            "Alias lama: `/u1`\n"
            "Atau reply pesan berisi path file lalu kirim `/rclone`."
        )
        return

    source_paths, resolve_error = resolve_upload_sources(source_path_text)
    if resolve_error:
        await message.edit_text(resolve_error)
        return

    cancel_event = asyncio.Event()
    UPLOAD_CONTROL["task"] = asyncio.current_task()
    UPLOAD_CONTROL["cancel_event"] = cancel_event

    target = target_label(target_chat)
    total_files = len(source_paths)

    await message.edit_text(
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

        await message.edit_text(
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
                    "progress_args": (message, media_name, state),
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
                    progress_args=(message, media_name, state),
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
        success_lines.append(f"- `{media_name}` -> `{ref}`")

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
        await message.edit_text(trim_output("\n".join(summary_lines)))
    finally:
        current_task = asyncio.current_task()
        if UPLOAD_CONTROL.get("task") is current_task:
            UPLOAD_CONTROL["task"] = None
            UPLOAD_CONTROL["cancel_event"] = None


@app.on_message(filters.me & filters.command("ucancel", prefixes="/"))
async def cancel_upload_command(client: Client, message):
    del client

    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /ucancel hanya di Saved Messages.")
        return

    running_task = UPLOAD_CONTROL.get("task")
    cancel_event = UPLOAD_CONTROL.get("cancel_event")

    if not running_task or running_task.done() or not cancel_event:
        UPLOAD_CONTROL["task"] = None
        UPLOAD_CONTROL["cancel_event"] = None
        await message.edit_text("Tidak ada upload yang sedang berjalan.")
        return

    cancel_event.set()
    await message.edit_text(
        "Permintaan cancel diterima.\n"
        "Menunggu proses upload berhenti..."
    )


@app.on_message(filters.me & filters.command("ls", prefixes="/"))
async def list_command(client: Client, message):
    del client

    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /ls hanya di Saved Messages.")
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
        await message.edit_text(f"Path tidak valid: `{e}`")
        return

    if not target_path.exists():
        await message.edit_text(f"Path tidak ditemukan:\n`{target_path}`")
        return

    if target_path.is_file():
        output = (
            f"Path: `{target_path}`\n"
            "Type: file\n\n"
            "```text\n"
            f"{format_entry_line(target_path)}\n"
            "```"
        )
        await message.edit_text(trim_output(output))
        return

    try:
        lines, total_entries, truncated = list_directory_lines(target_path, show_all)
    except PermissionError:
        await message.edit_text(f"Akses ditolak:\n`{target_path}`")
        return
    except Exception as e:
        await message.edit_text(f"Gagal membaca direktori: `{e}`")
        return

    if not lines:
        output = (
            f"Path: `{target_path}`\n"
            f"Total: `{total_entries}`\n\n"
            "(kosong)"
        )
        await message.edit_text(output)
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
    await message.edit_text(trim_output(output))


@app.on_message(filters.me & filters.command("rm", prefixes="/"))
async def remove_command(client: Client, message):
    del client

    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /rm hanya di Saved Messages.")
        return

    args = command_args(message)
    path_text = " ".join(args).strip()
    if not path_text and message.reply_to_message:
        path_text = (
            message.reply_to_message.text or message.reply_to_message.caption or ""
        ).strip()

    if not path_text:
        await message.edit_text(
            "Format hapus:\n"
            "`/rm /home/runner/uploads/file.txt`\n"
            "`/rm /home/runner/uploads/folder`\n"
            "`/rm /home/runner/uploads/*.tmp`\n"
            "Atau reply pesan berisi path lalu kirim `/rm`."
        )
        return

    targets, resolve_error = resolve_fs_sources(path_text)
    if resolve_error:
        await message.edit_text(resolve_error)
        return

    success_lines = []
    failed_lines = []

    for target in targets:
        if not target.exists():
            failed_lines.append(f"- `{target}` -> path tidak ditemukan")
            continue
        if is_filesystem_root(target):
            failed_lines.append(f"- `{target}` -> menolak hapus root filesystem")
            continue

        try:
            delete_path(target)
            success_lines.append(f"- `{target}`")
        except Exception as e:
            failed_lines.append(f"- `{target}` -> {e}")

    summary_lines = [
        "Hapus selesai.",
        f"Input: `{path_text}`",
        f"Target: `{len(targets)}`",
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

    await message.edit_text(trim_output("\n".join(summary_lines)))


@app.on_message(filters.me & filters.command("copy", prefixes="/"))
async def copy_command(client: Client, message):
    del client

    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /copy hanya di Saved Messages.")
        return

    args = command_args(message)
    if len(args) < 2:
        await message.edit_text(
            "Format copy:\n"
            "`/copy <sumber> <tujuan>`\n"
            "Contoh:\n"
            "`/copy /home/runner/uploads/file.txt /home/runner/downloads/file.txt`\n"
            "`/copy /home/runner/uploads/*.mp4 /home/runner/downloads/`"
        )
        return

    source_text = " ".join(args[:-1]).strip()
    destination_text = args[-1].strip()
    if not source_text or not destination_text:
        await message.edit_text("Format copy tidak valid.")
        return

    source_paths, resolve_error = resolve_fs_sources(source_text)
    if resolve_error:
        await message.edit_text(resolve_error)
        return

    try:
        destination_base = local_path_from_text(destination_text)
    except Exception as e:
        await message.edit_text(f"Tujuan tidak valid: `{e}`")
        return

    if len(source_paths) > 1 and (not destination_base.exists() or not destination_base.is_dir()):
        await message.edit_text(
            "Jika sumber lebih dari satu, tujuan harus folder yang sudah ada."
        )
        return

    success_lines = []
    failed_lines = []

    for source_path in source_paths:
        if not source_path.exists():
            failed_lines.append(f"- `{source_path}` -> sumber tidak ditemukan")
            continue
        if is_filesystem_root(source_path):
            failed_lines.append(
                f"- `{source_path}` -> menolak operasi pada root filesystem"
            )
            continue

        if len(source_paths) > 1 or (
            destination_base.exists() and destination_base.is_dir()
        ):
            destination_path = destination_base / source_path.name
        else:
            destination_path = destination_base

        if source_path.resolve() == destination_path.resolve():
            failed_lines.append(f"- `{source_path}` -> sumber dan tujuan sama")
            continue
        if destination_path.exists():
            failed_lines.append(f"- `{source_path}` -> tujuan sudah ada: `{destination_path}`")
            continue

        try:
            copied_to = copy_path(source_path, destination_path)
            success_lines.append(f"- `{source_path}` -> `{copied_to}`")
        except Exception as e:
            failed_lines.append(f"- `{source_path}` -> {e}")

    summary_lines = [
        "Copy selesai.",
        f"Sumber: `{source_text}`",
        f"Tujuan: `{destination_base}`",
        f"Target: `{len(source_paths)}`",
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

    await message.edit_text(trim_output("\n".join(summary_lines)))


@app.on_message(filters.me & filters.command("mv", prefixes="/"))
async def move_command(client: Client, message):
    del client

    if not message.from_user or message.chat.id != message.from_user.id:
        await message.edit_text("Gunakan /mv hanya di Saved Messages.")
        return

    args = command_args(message)
    if len(args) < 2:
        await message.edit_text(
            "Format move:\n"
            "`/mv <sumber> <tujuan>`\n"
            "Contoh:\n"
            "`/mv /home/runner/uploads/file.txt /home/runner/downloads/file.txt`\n"
            "`/mv /home/runner/uploads/*.mp4 /home/runner/downloads/`"
        )
        return

    source_text = " ".join(args[:-1]).strip()
    destination_text = args[-1].strip()
    if not source_text or not destination_text:
        await message.edit_text("Format move tidak valid.")
        return

    source_paths, resolve_error = resolve_fs_sources(source_text)
    if resolve_error:
        await message.edit_text(resolve_error)
        return

    try:
        destination_base = local_path_from_text(destination_text)
    except Exception as e:
        await message.edit_text(f"Tujuan tidak valid: `{e}`")
        return

    if len(source_paths) > 1 and (not destination_base.exists() or not destination_base.is_dir()):
        await message.edit_text(
            "Jika sumber lebih dari satu, tujuan harus folder yang sudah ada."
        )
        return

    success_lines = []
    failed_lines = []

    for source_path in source_paths:
        if not source_path.exists():
            failed_lines.append(f"- `{source_path}` -> sumber tidak ditemukan")
            continue
        if is_filesystem_root(source_path):
            failed_lines.append(
                f"- `{source_path}` -> menolak operasi pada root filesystem"
            )
            continue

        if len(source_paths) > 1 or (
            destination_base.exists() and destination_base.is_dir()
        ):
            destination_path = destination_base / source_path.name
        else:
            destination_path = destination_base

        if source_path.resolve() == destination_path.resolve():
            failed_lines.append(f"- `{source_path}` -> sumber dan tujuan sama")
            continue
        if destination_path.exists():
            failed_lines.append(f"- `{source_path}` -> tujuan sudah ada: `{destination_path}`")
            continue

        try:
            moved_to = move_path(source_path, destination_path)
            success_lines.append(f"- `{source_path}` -> `{moved_to}`")
        except Exception as e:
            failed_lines.append(f"- `{source_path}` -> {e}")

    summary_lines = [
        "Move selesai.",
        f"Sumber: `{source_text}`",
        f"Tujuan: `{destination_base}`",
        f"Target: `{len(source_paths)}`",
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

    await message.edit_text(trim_output("\n".join(summary_lines)))


if __name__ == "__main__":
    print("Userbot manual downloader aktif.")
    print("Langkah pakai:")
    print("1. Forward file/video ATAU kirim link t.me ke Saved Messages.")
    print("2. Reply pesan tersebut dengan /d1.")
    print("3. Upload file lokal: /rclone /path/file, /rclone *.txt, atau /rclone /path/*.mp4 --to @username (alias: /u1)")
    print("4. Cek isi direktori: /ls /path  (opsional: /ls -a /path)")
    print("5. Hapus/copy/move file: /rm, /copy, /mv")
    print("6. Batalkan upload yang sedang berjalan: /ucancel")
    print(f"7. File download disimpan ke: {download_root}")
    print(f"8. Default folder upload: {upload_root}")
    app.run()
