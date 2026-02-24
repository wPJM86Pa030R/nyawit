import os
import re
import time
import json
import shlex
import asyncio
import glob
import shutil
import sys
import base64
import struct
import tempfile
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Prevent filename/package collision when this file is named "telethon.py".
_SCRIPT_DIR = Path(__file__).resolve().parent
try:
    if sys.path and Path(sys.path[0]).resolve() == _SCRIPT_DIR:
        sys.path.pop(0)
except Exception:
    pass

from telethon import TelegramClient, events
from telethon.crypto.authkey import AuthKey
from telethon.errors import FloodWaitError, MessageIdInvalidError, MessageNotModifiedError
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
RAW_SESSION_STRING = os.getenv("SESSION_STRING_TELETHON") or os.getenv("SESSION_STRING")
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


def normalize_session_string(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None
    value = raw_value.strip()
    if (
        len(value) >= 2
        and ((value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")))
    ):
        value = value[1:-1].strip()
    return value or None


def decode_urlsafe_base64(raw_value: str) -> bytes:
    padded = raw_value + "=" * (-len(raw_value) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def convert_pyrogram_session_to_telethon(raw_value: str) -> Optional[str]:
    dc_map = {
        1: ("149.154.175.53", 443),
        2: ("149.154.167.51", 443),
        3: ("149.154.175.100", 443),
        4: ("149.154.167.91", 443),
        5: ("91.108.56.130", 443),
    }

    try:
        payload = decode_urlsafe_base64(raw_value)
    except Exception:
        return None

    # Pyrogram v2 string session
    fmt_v2 = ">BI?256sQ?"
    # Pyrogram v1 legacy format
    fmt_v1 = ">B?256sI?"

    dc_id = None
    auth_key = None

    if len(payload) == struct.calcsize(fmt_v2):
        unpacked = struct.unpack(fmt_v2, payload)
        dc_id = unpacked[0]
        auth_key = unpacked[3]
    elif len(payload) == struct.calcsize(fmt_v1):
        unpacked = struct.unpack(fmt_v1, payload)
        dc_id = unpacked[0]
        auth_key = unpacked[2]

    if not dc_id or not auth_key:
        return None

    if dc_id not in dc_map:
        return None

    ip, port = dc_map[dc_id]
    session = StringSession()
    session.set_dc(dc_id, ip, port)
    session.auth_key = AuthKey(auth_key)
    return session.save()


SESSION_STRING = normalize_session_string(RAW_SESSION_STRING)
if SESSION_STRING:
    try:
        StringSession(SESSION_STRING)
    except ValueError:
        converted = convert_pyrogram_session_to_telethon(SESSION_STRING)
        if converted:
            print(
                "[INFO] SESSION string terdeteksi format Pyrogram. "
                "Konversi otomatis ke format Telethon berhasil."
            )
            SESSION_STRING = converted

if SESSION_STRING:
    try:
        client = TelegramClient(StringSession(SESSION_STRING), api_id, API_HASH)
    except ValueError as e:
        raise RuntimeError(
            "SESSION_STRING tidak valid untuk Telethon. "
            "Gunakan session string Telethon (atau SESSION_STRING Pyrogram "
            "yang valid agar bisa dikonversi otomatis)."
        ) from e
else:
    client = TelegramClient("manual_downloader_telethon", api_id, API_HASH)

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
RCLONE_BIN = os.getenv("RCLONE_BIN", "rclone")
RCLONE_REMOTE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*:(?!//).*$")
WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/].*")
try:
    RCLONE_TIMEOUT = max(1, int(os.getenv("RCLONE_TIMEOUT", "600")))
except ValueError:
    RCLONE_TIMEOUT = 600

SELF_ID: Optional[int] = None
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


def parse_telegram_link(text: Optional[str]) -> Optional[Tuple[Union[str, int], int]]:
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
    if not message:
        return False
    return bool(getattr(message, "photo", None) or getattr(message, "document", None))


def media_label(message) -> str:
    media_file = getattr(message, "file", None)
    file_name = media_file.name if media_file else None
    mime_type = media_file.mime_type if media_file else ""

    if getattr(message, "document", None):
        if mime_type and str(mime_type).startswith("video/"):
            return f"video: {file_name or 'unknown'}"
        if mime_type and str(mime_type).startswith("audio/"):
            return f"audio: {file_name or 'unknown'}"
        return f"document: {file_name or 'unknown'}"
    if getattr(message, "photo", None):
        return "photo"
    return "media"


def parse_chat_target(raw_target: str):
    value = raw_target.strip()
    if not value or value.lower() == "me":
        return "me"
    if value.startswith("@"):
        value = value[1:]
    if value.lstrip("-").isdigit():
        return int(value)
    return value


def command_args(raw_text: str):
    raw = (raw_text or "").strip()
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


def shell_join(parts: List[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def sanitize_output_block(text: str) -> str:
    return text.replace("```", "'''")


def is_rclone_remote_path(path_text: str) -> bool:
    raw = (path_text or "").strip()
    if not raw:
        return False
    if raw.startswith("file://"):
        return False
    if WINDOWS_DRIVE_RE.match(raw):
        return False
    return bool(RCLONE_REMOTE_RE.match(raw))


def format_command_output(
    command: List[str],
    returncode: Optional[int],
    stdout_text: str,
    stderr_text: str,
) -> str:
    lines = [f"Command: `{shell_join(command)}`"]
    lines.append(f"Exit code: `{returncode if returncode is not None else 'N/A'}`")

    stdout_clean = sanitize_output_block(stdout_text.strip())
    stderr_clean = sanitize_output_block(stderr_text.strip())

    if stdout_clean:
        lines.append("")
        lines.append("stdout:")
        lines.append("```text")
        lines.append(stdout_clean)
        lines.append("```")
    if stderr_clean:
        lines.append("")
        lines.append("stderr:")
        lines.append("```text")
        lines.append(stderr_clean)
        lines.append("```")
    if not stdout_clean and not stderr_clean:
        lines.append("")
        lines.append("Tidak ada output.")

    return trim_output("\n".join(lines))


async def run_subprocess_command(
    command: List[str], timeout_seconds: int
) -> Tuple[Optional[int], str, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return None, "", f"Binary tidak ditemukan: {command[0]}"
    except Exception as e:
        return None, "", str(e)

    try:
        stdout_data, stderr_data = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        process.kill()
        stdout_data, stderr_data = await process.communicate()
        stdout_text = stdout_data.decode("utf-8", errors="ignore")
        stderr_text = stderr_data.decode("utf-8", errors="ignore").strip()
        timeout_text = f"Perintah timeout setelah {timeout_seconds} detik."
        if stderr_text:
            stderr_text = f"{stderr_text}\n{timeout_text}"
        else:
            stderr_text = timeout_text
        return process.returncode, stdout_text, stderr_text

    stdout_text = stdout_data.decode("utf-8", errors="ignore")
    stderr_text = stderr_data.decode("utf-8", errors="ignore")
    return process.returncode, stdout_text, stderr_text


async def run_rclone_command(args: List[str]) -> Tuple[List[str], Optional[int], str, str]:
    command = [RCLONE_BIN, *args]
    returncode, stdout_text, stderr_text = await run_subprocess_command(
        command,
        timeout_seconds=RCLONE_TIMEOUT,
    )
    return command, returncode, stdout_text, stderr_text


def resolve_local_destination_path(source_path: Path, target_text: str) -> Path:
    raw_target = target_text.strip()
    target_path = local_path_from_text(raw_target)
    target_is_dir_hint = raw_target.endswith("/") or raw_target.endswith("\\")

    if target_path.exists() and target_path.is_dir():
        return target_path / source_path.name
    if target_is_dir_hint:
        return target_path / source_path.name
    return target_path


def remove_local_path(path_text: str) -> Tuple[bool, str]:
    try:
        target_path = local_path_from_text(path_text)
    except Exception as e:
        return False, f"Path tidak valid: `{e}`"

    if not target_path.exists():
        return False, f"Path tidak ditemukan:\n`{target_path}`"

    try:
        if target_path.is_dir() and not target_path.is_symlink():
            shutil.rmtree(target_path)
            kind = "folder"
        else:
            target_path.unlink()
            kind = "file"
    except Exception as e:
        return False, f"Gagal menghapus `{target_path}`:\n`{e}`"

    return True, f"Berhasil menghapus {kind}:\n`{target_path}`"


def copy_local_path(source_text: str, target_text: str) -> Tuple[bool, str]:
    try:
        source_path = local_path_from_text(source_text)
    except Exception as e:
        return False, f"Sumber tidak valid: `{e}`"

    if not source_path.exists():
        return False, f"Sumber tidak ditemukan:\n`{source_path}`"

    try:
        target_path = resolve_local_destination_path(source_path, target_text)
    except Exception as e:
        return False, f"Tujuan tidak valid: `{e}`"

    if source_path == target_path:
        return False, "Sumber dan tujuan tidak boleh sama."

    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.is_dir() and not source_path.is_symlink():
            shutil.copytree(source_path, target_path, dirs_exist_ok=True)
            kind = "folder"
        else:
            shutil.copy2(source_path, target_path)
            kind = "file"
    except Exception as e:
        return False, f"Gagal copy:\n`{e}`"

    return (
        True,
        "Copy selesai.\n"
        f"Tipe: `{kind}`\n"
        f"Sumber: `{source_path}`\n"
        f"Tujuan: `{target_path}`",
    )


def move_local_path(source_text: str, target_text: str) -> Tuple[bool, str]:
    try:
        source_path = local_path_from_text(source_text)
    except Exception as e:
        return False, f"Sumber tidak valid: `{e}`"

    if not source_path.exists():
        return False, f"Sumber tidak ditemukan:\n`{source_path}`"

    try:
        target_path = resolve_local_destination_path(source_path, target_text)
    except Exception as e:
        return False, f"Tujuan tidak valid: `{e}`"

    if source_path == target_path:
        return False, "Sumber dan tujuan tidak boleh sama."

    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        moved_to = Path(shutil.move(str(source_path), str(target_path))).resolve()
    except Exception as e:
        return False, f"Gagal move:\n`{e}`"

    return (
        True,
        "Move selesai.\n"
        f"Sumber: `{source_path}`\n"
        f"Tujuan: `{moved_to}`",
    )


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

    with tempfile.NamedTemporaryFile(prefix="telethon_thumb_", suffix=".jpg", delete=False) as tmp_file:
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


async def safe_edit(event, text: str) -> None:
    try:
        await event.edit(text)
    except FloodWaitError as e:
        await asyncio.sleep(max(1, int(e.seconds)))
        try:
            await event.edit(text)
        except Exception:
            pass
    except MessageNotModifiedError:
        pass
    except Exception:
        pass


def make_progress_callback(kind: str, event, media_name: str, state: dict):
    loop = asyncio.get_running_loop()

    def callback(current, total):
        cancel_event = state.get("cancel_event")
        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError("Upload dibatalkan oleh pengguna.")

        now = time.time()
        is_done = total > 0 and current >= total
        last_tick = state["last_tick"]
        if not is_done and (now - last_tick) < PROGRESS_INTERVAL:
            return
        if state.get("updating", False):
            return

        elapsed = max(now - state["started_at"], 0.001)
        speed = current / elapsed
        eta = int((total - current) / speed) if speed > 0 and total > 0 else 0
        percent = (current * 100 / total) if total > 0 else 0

        text = (
            f"{kind} berjalan\n"
            f"File: {media_name}\n"
            f"Progress: {percent:.2f}%\n"
            f"Size: {format_bytes(current)} / {format_bytes(total)}\n"
            f"Speed: {format_bytes(speed)}/s\n"
            f"ETA: {format_duration(eta)}"
        )

        print(
            f"[{kind[:2].upper()}] {media_name} | {percent:.2f}% | "
            f"{format_bytes(current)}/{format_bytes(total)} | "
            f"{format_bytes(speed)}/s | ETA {format_duration(eta)}"
        )

        state["last_tick"] = now

        async def updater():
            state["updating"] = True
            try:
                await safe_edit(event, text)
            finally:
                state["updating"] = False

        loop.create_task(updater())

    return callback


async def resolve_target_message(replied_message):
    if has_downloadable_media(replied_message):
        return replied_message, None

    source_text = replied_message.message or ""
    parsed = parse_telegram_link(source_text)
    if not parsed:
        return None, "Pesan yang dibalas tidak berisi media atau link Telegram yang valid."

    chat_id_or_username, message_id = parsed
    try:
        target_message = await client.get_messages(chat_id_or_username, ids=message_id)
    except MessageIdInvalidError:
        return None, "ID pesan pada link tidak valid."
    except Exception as e:
        return None, f"Gagal mengambil pesan dari link: {e}"

    if not target_message or getattr(target_message, "empty", False):
        return None, "Pesan dari link tidak ditemukan atau akun tidak punya akses."

    if not has_downloadable_media(target_message):
        return None, "Pesan dari link ditemukan, tapi tidak berisi file/video."

    return target_message, None


def is_saved_messages(event) -> bool:
    return bool(SELF_ID) and event.chat_id == SELF_ID


@client.on(events.NewMessage(outgoing=True, pattern=r"^/d1(?:\s|$)"))
async def download_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /d1 hanya di Saved Messages.")
        return

    if not event.is_reply:
        await safe_edit(
            event,
            "Balas pesan yang berisi file/video atau link Telegram, lalu kirim /d1.",
        )
        return

    await safe_edit(event, "Memeriksa pesan yang dibalas...")
    replied_message = await event.get_reply_message()
    target_message, error_message = await resolve_target_message(replied_message)
    if error_message:
        await safe_edit(event, error_message)
        return

    name = media_label(target_message)
    state = {"started_at": time.time(), "last_tick": 0.0, "updating": False}
    progress_cb = make_progress_callback("Download", event, name, state)

    await safe_edit(
        event,
        "Memulai download ke storage VPS\n"
        f"Folder: `{download_root}`\n"
        f"File: {name}",
    )

    try:
        file_path = await target_message.download_media(
            file=download_target,
            progress_callback=progress_cb,
        )
    except Exception as e:
        await safe_edit(event, f"Download gagal: `{e}`")
        return

    if not file_path:
        await safe_edit(event, "Download gagal: path file kosong.")
        return

    await safe_edit(
        event,
        "Download selesai.\n"
        f"Lokasi: `{file_path}`",
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^/u1(?:\s|$)"))
async def upload_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /u1 hanya di Saved Messages.")
        return

    running_task = UPLOAD_CONTROL.get("task")
    if running_task and not running_task.done():
        await safe_edit(
            event,
            "Masih ada upload aktif.\n"
            "Gunakan `/ucancel` untuk membatalkan upload yang sedang berjalan.",
        )
        return

    args = command_args(event.raw_text)
    source_path_text = ""
    target_chat = "me"

    if "--to" in args:
        idx = args.index("--to")
        source_path_text = " ".join(args[:idx]).strip()
        target_text = " ".join(args[idx + 1 :]).strip()
        if not target_text:
            await safe_edit(
                event,
                "Format target tidak valid.\n"
                "Contoh: `/u1 /home/runner/uploads/file.mp4 --to @username`",
            )
            return
        target_chat = parse_chat_target(target_text)
    else:
        source_path_text = " ".join(args).strip()

    if not source_path_text and event.is_reply:
        replied = await event.get_reply_message()
        source_path_text = (replied.message or "").strip()

    if not source_path_text:
        await safe_edit(
            event,
            "Format upload:\n"
            "`/u1 /home/runner/uploads/file.mp4`\n"
            "`/u1 *.txt`\n"
            "`/u1 /home/runner/uploads/*.mp4 --to @username`\n"
            "Atau reply pesan berisi path file lalu kirim `/u1`.",
        )
        return

    source_paths, resolve_error = resolve_upload_sources(source_path_text)
    if resolve_error:
        await safe_edit(event, resolve_error)
        return

    cancel_event = asyncio.Event()
    UPLOAD_CONTROL["task"] = asyncio.current_task()
    UPLOAD_CONTROL["cancel_event"] = cancel_event

    target = target_label(target_chat)
    total_files = len(source_paths)

    await safe_edit(
        event,
        "Memulai upload dari storage VPS\n"
        f"Input: `{source_path_text}`\n"
        f"Ditemukan: `{total_files}` file\n"
        f"Default folder upload: `{upload_root}`\n"
        f"Tujuan: `{target}`",
    )

    success_lines = []
    failed_lines = []
    cancelled = False

    for index, source_path in enumerate(source_paths, start=1):
        if cancel_event.is_set():
            cancelled = True
            break

        media_name = source_path.name
        state = {
            "started_at": time.time(),
            "last_tick": 0.0,
            "updating": False,
            "cancel_event": cancel_event,
        }
        progress_cb = make_progress_callback("Upload", event, media_name, state)

        await safe_edit(
            event,
            "Upload berjalan\n"
            f"File: `{index}/{total_files}`\n"
            f"Nama: `{media_name}`\n"
            f"Sumber: `{source_path}`\n"
            f"Tujuan: `{target}`",
        )

        try:
            suffix = source_path.suffix.lower()
            if suffix in VIDEO_EXTENSIONS:
                video_kwargs = {
                    "entity": target_chat,
                    "file": str(source_path),
                    "caption": f"`{media_name}`",
                    "supports_streaming": True,
                    "progress_callback": progress_cb,
                }

                video_metadata = await probe_video_metadata(source_path)
                duration = video_metadata.get("duration")
                width = video_metadata.get("width")
                height = video_metadata.get("height")

                if duration and width and height:
                    video_kwargs["attributes"] = [
                        DocumentAttributeVideo(
                            duration=duration,
                            w=width,
                            h=height,
                            supports_streaming=True,
                        )
                    ]
                else:
                    print(
                        f"[WARN] Metadata video tidak lengkap untuk {source_path.name}. "
                        "Telethon bisa mengirim tanpa atribut video penuh."
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
                    sent_message = await client.send_file(**video_kwargs)
                finally:
                    if thumb_path:
                        remove_file_quietly(thumb_path)
            else:
                sent_message = await client.send_file(
                    entity=target_chat,
                    file=str(source_path),
                    force_document=True,
                    caption=f"`{media_name}`",
                    progress_callback=progress_cb,
                )
        except asyncio.CancelledError:
            cancel_event.set()
            cancelled = True
            failed_lines.append(f"- `{media_name}` -> dibatalkan")
            continue
        except Exception as e:
            failed_lines.append(f"- `{media_name}` -> {e}")
            continue

        if isinstance(target_chat, str) and target_chat not in ("me",):
            ref = f"https://t.me/{target_chat}/{sent_message.id}"
        else:
            ref = f"{getattr(sent_message, 'chat_id', target_chat)}/{sent_message.id}"
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
        await safe_edit(event, trim_output("\n".join(summary_lines)))
    finally:
        current_task = asyncio.current_task()
        if UPLOAD_CONTROL.get("task") is current_task:
            UPLOAD_CONTROL["task"] = None
            UPLOAD_CONTROL["cancel_event"] = None


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ucancel(?:\s|$)"))
async def cancel_upload_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /ucancel hanya di Saved Messages.")
        return

    running_task = UPLOAD_CONTROL.get("task")
    cancel_event = UPLOAD_CONTROL.get("cancel_event")

    if not running_task or running_task.done() or not cancel_event:
        UPLOAD_CONTROL["task"] = None
        UPLOAD_CONTROL["cancel_event"] = None
        await safe_edit(event, "Tidak ada upload yang sedang berjalan.")
        return

    cancel_event.set()
    await safe_edit(
        event,
        "Permintaan cancel diterima.\n"
        "Menunggu proses upload berhenti...",
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ls(?:\s|$)"))
async def list_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /ls hanya di Saved Messages.")
        return

    args = command_args(event.raw_text)
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
        await safe_edit(event, f"Path tidak valid: `{e}`")
        return

    if not target_path.exists():
        await safe_edit(event, f"Path tidak ditemukan:\n`{target_path}`")
        return

    if target_path.is_file():
        output = (
            f"Path: `{target_path}`\n"
            "Type: file\n\n"
            "```text\n"
            f"{format_entry_line(target_path)}\n"
            "```"
        )
        await safe_edit(event, trim_output(output))
        return

    try:
        lines, total_entries, truncated = list_directory_lines(target_path, show_all)
    except PermissionError:
        await safe_edit(event, f"Akses ditolak:\n`{target_path}`")
        return
    except Exception as e:
        await safe_edit(event, f"Gagal membaca direktori: `{e}`")
        return

    if not lines:
        output = (
            f"Path: `{target_path}`\n"
            f"Total: `{total_entries}`\n\n"
            "(kosong)"
        )
        await safe_edit(event, output)
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
    await safe_edit(event, trim_output(output))


@client.on(events.NewMessage(outgoing=True, pattern=r"^/rm(?:\s|$)"))
async def remove_path_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /rm hanya di Saved Messages.")
        return

    args = command_args(event.raw_text)
    if not args and event.is_reply:
        replied = await event.get_reply_message()
        reply_text = (replied.message or "").strip()
        if reply_text:
            args = [reply_text]

    if len(args) != 1:
        await safe_edit(
            event,
            "Format hapus:\n"
            "`/rm <path>`\n"
            "Contoh lokal: `/rm /home/runner/downloads/file.mp4`\n"
            "Contoh remote: `/rm gdrive:folder/file.mp4`",
        )
        return

    target_text = args[0]
    if is_rclone_remote_path(target_text):
        await safe_edit(event, f"Menjalankan rclone delete:\n`{target_text}`")
        command, returncode, stdout_text, stderr_text = await run_rclone_command(
            ["delete", target_text, "--rmdirs"]
        )
        status = "Hapus remote selesai." if returncode == 0 else "Hapus remote gagal."
        output = (
            f"{status}\n"
            f"Target: `{target_text}`\n\n"
            f"{format_command_output(command, returncode, stdout_text, stderr_text)}"
        )
        await safe_edit(event, trim_output(output))
        return

    _, output = remove_local_path(target_text)
    await safe_edit(event, output)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/copy(?:\s|$)"))
async def copy_path_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /copy hanya di Saved Messages.")
        return

    args = command_args(event.raw_text)
    if len(args) != 2:
        await safe_edit(
            event,
            "Format copy:\n"
            "`/copy <sumber> <tujuan>`\n"
            "Contoh lokal: `/copy /home/runner/a.mp4 /home/runner/b.mp4`\n"
            "Contoh remote: `/copy gdrive:a.mp4 gdrive:backup/a.mp4`",
        )
        return

    source_text, target_text = args
    if is_rclone_remote_path(source_text) or is_rclone_remote_path(target_text):
        await safe_edit(
            event,
            "Menjalankan rclone copyto...\n"
            f"Sumber: `{source_text}`\n"
            f"Tujuan: `{target_text}`",
        )
        command, returncode, stdout_text, stderr_text = await run_rclone_command(
            ["copyto", source_text, target_text]
        )
        status = "Copy remote selesai." if returncode == 0 else "Copy remote gagal."
        output = (
            f"{status}\n"
            f"Sumber: `{source_text}`\n"
            f"Tujuan: `{target_text}`\n\n"
            f"{format_command_output(command, returncode, stdout_text, stderr_text)}"
        )
        await safe_edit(event, trim_output(output))
        return

    _, output = copy_local_path(source_text, target_text)
    await safe_edit(event, output)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/mv(?:\s|$)"))
async def move_path_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /mv hanya di Saved Messages.")
        return

    args = command_args(event.raw_text)
    if len(args) != 2:
        await safe_edit(
            event,
            "Format move:\n"
            "`/mv <sumber> <tujuan>`\n"
            "Contoh lokal: `/mv /home/runner/a.mp4 /home/runner/archive/a.mp4`\n"
            "Contoh remote: `/mv gdrive:a.mp4 gdrive:archive/a.mp4`",
        )
        return

    source_text, target_text = args
    if is_rclone_remote_path(source_text) or is_rclone_remote_path(target_text):
        await safe_edit(
            event,
            "Menjalankan rclone moveto...\n"
            f"Sumber: `{source_text}`\n"
            f"Tujuan: `{target_text}`",
        )
        command, returncode, stdout_text, stderr_text = await run_rclone_command(
            ["moveto", source_text, target_text]
        )
        status = "Move remote selesai." if returncode == 0 else "Move remote gagal."
        output = (
            f"{status}\n"
            f"Sumber: `{source_text}`\n"
            f"Tujuan: `{target_text}`\n\n"
            f"{format_command_output(command, returncode, stdout_text, stderr_text)}"
        )
        await safe_edit(event, trim_output(output))
        return

    _, output = move_local_path(source_text, target_text)
    await safe_edit(event, output)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/rclone(?:\s|$)"))
async def raw_rclone_command(event):
    if not is_saved_messages(event):
        await safe_edit(event, "Gunakan /rclone hanya di Saved Messages.")
        return

    args = command_args(event.raw_text)
    if not args:
        await safe_edit(
            event,
            "Format rclone:\n"
            "`/rclone <argumen rclone>`\n"
            "Contoh:\n"
            "`/rclone ls gdrive:`\n"
            "`/rclone lsf gdrive:folder`\n"
            "`/rclone mkdir gdrive:folder-baru`",
        )
        return

    command_preview = shell_join([RCLONE_BIN, *args])
    await safe_edit(event, f"Menjalankan perintah rclone:\n`{command_preview}`")

    command, returncode, stdout_text, stderr_text = await run_rclone_command(args)
    status = "Perintah rclone selesai." if returncode == 0 else "Perintah rclone gagal."
    output = (
        f"{status}\n\n"
        f"{format_command_output(command, returncode, stdout_text, stderr_text)}"
    )
    await safe_edit(event, trim_output(output))


if __name__ == "__main__":
    print("Userbot manual downloader Telethon aktif.")
    print("Langkah pakai:")
    print("1. Forward file/video ATAU kirim link t.me ke Saved Messages.")
    print("2. Reply pesan tersebut dengan /d1.")
    print("3. Upload file lokal: /u1 /path/file, /u1 *.txt, atau /u1 /path/*.mp4 --to @username")
    print("4. Cek isi direktori: /ls /path  (opsional: /ls -a /path)")
    print("5. Operasi file: /rm <path>, /copy <sumber> <tujuan>, /mv <sumber> <tujuan>")
    print("6. Rclone native: /rclone ls gdrive:")
    print("7. Batalkan upload yang sedang berjalan: /ucancel")
    print(f"8. File download disimpan ke: {download_root}")
    print(f"9. Default folder upload: {upload_root}")

    client.parse_mode = "md"
    client.start()
    SELF_ID = client.loop.run_until_complete(client.get_me()).id
    client.run_until_disconnected()
