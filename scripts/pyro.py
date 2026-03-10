#!/usr/bin/env python3
"""
Auto delete Google Drive files when the day changes.

Behavior:
- Deletes files created before the current local day start.
- Uses files.delete so deletion is permanent (not moved to trash).
"""

import argparse
import datetime as dt
import os
import sys
import time
from typing import Callable
from zoneinfo import ZoneInfo

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive"]
FOLDER_MIME = "application/vnd.google-apps.folder"


def load_credentials(credentials_path: str, token_path: str) -> Credentials:
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w", encoding="utf-8") as token_file:
            token_file.write(creds.to_json())

    return creds


def to_utc_day_start(local_tz: ZoneInfo) -> dt.datetime:
    now_local = dt.datetime.now(local_tz)
    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start_local.astimezone(dt.timezone.utc)


def build_query(day_start_utc: dt.datetime, folder_id: str | None) -> str:
    cutoff = day_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    conditions = [
        f"createdTime < '{cutoff}'",
        "trashed = false",
        f"mimeType != '{FOLDER_MIME}'",
    ]

    if folder_id:
        conditions.append(f"'{folder_id}' in parents")

    return " and ".join(conditions)


def delete_old_files(service, folder_id: str | None, local_tz: ZoneInfo) -> int:
    day_start_utc = to_utc_day_start(local_tz)
    query = build_query(day_start_utc, folder_id)

    deleted_count = 0
    page_token = None

    print(f"[INFO] Delete cutoff UTC: {day_start_utc.isoformat()}")
    print(f"[INFO] Query: {query}")

    while True:
        response = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name,createdTime)",
                pageSize=1000,
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )

        files = response.get("files", [])

        for item in files:
            file_id = item["id"]
            file_name = item.get("name", "(no name)")

            try:
                service.files().delete(
                    fileId=file_id,
                    supportsAllDrives=True,
                ).execute()
                deleted_count += 1
                print(f"[DELETED] {file_name} ({file_id})")
            except HttpError as exc:
                print(f"[ERROR] Failed to delete {file_name} ({file_id}): {exc}")

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    print(f"[INFO] Total permanently deleted: {deleted_count}")
    return deleted_count


def delete_specific_files(service, file_ids: list[str]) -> int:
    deleted_count = 0

    for file_id in file_ids:
        try:
            service.files().delete(
                fileId=file_id,
                supportsAllDrives=True,
            ).execute()
            deleted_count += 1
            print(f"[DELETED] File ID: {file_id}")
        except HttpError as exc:
            print(f"[ERROR] Failed to delete file ID {file_id}: {exc}")

    print(f"[INFO] Total permanently deleted: {deleted_count}")
    return deleted_count


def run_daemon(cleanup_func: Callable[[], int], local_tz: ZoneInfo, poll_seconds: int) -> None:
    last_day = dt.datetime.now(local_tz).date()
    print(f"[INFO] Daemon running. Current local day: {last_day}")

    while True:
        current_day = dt.datetime.now(local_tz).date()
        if current_day != last_day:
            print(f"[INFO] Day changed: {last_day} -> {current_day}")
            cleanup_func()
            last_day = current_day

        time.sleep(poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto delete Google Drive files permanently when day changes."
    )
    parser.add_argument(
        "--credentials",
        default="credentials.json",
        help="Path to OAuth client JSON from Google Cloud Console.",
    )
    parser.add_argument(
        "--token",
        default="token.json",
        help="Path to saved OAuth user token.",
    )
    parser.add_argument(
        "--folder-id",
        default=None,
        help="Optional folder ID. If set, only files inside this folder are deleted.",
    )
    parser.add_argument(
        "--file-ids",
        default=None,
        help="Optional comma-separated file IDs for direct delete mode.",
    )
    parser.add_argument(
        "--timezone",
        default="Asia/Jakarta",
        help="IANA timezone used for day boundary, e.g. Asia/Jakarta.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=20,
        help="Daemon check interval in seconds.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one cleanup and exit.",
    )
    parser.add_argument(
        "--skip-start-cleanup",
        action="store_true",
        help="Skip cleanup at startup before daemon loop.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not os.path.exists(args.credentials):
        print(f"[ERROR] Credentials file not found: {args.credentials}")
        return 1

    try:
        local_tz = ZoneInfo(args.timezone)
    except Exception as exc:
        print(f"[ERROR] Invalid timezone '{args.timezone}': {exc}")
        return 1

    creds = load_credentials(args.credentials, args.token)
    service = build("drive", "v3", credentials=creds)
    file_ids = (
        [file_id.strip() for file_id in args.file_ids.split(",") if file_id.strip()]
        if args.file_ids
        else []
    )

    if file_ids and args.folder_id:
        print("[INFO] --file-ids is set, --folder-id will be ignored.")

    if file_ids:
        cleanup_func = lambda: delete_specific_files(service, file_ids)
    else:
        cleanup_func = lambda: delete_old_files(service, args.folder_id, local_tz)

    if args.once:
        cleanup_func()
        return 0

    if not args.skip_start_cleanup:
        cleanup_func()

    try:
        run_daemon(cleanup_func, local_tz, args.poll_seconds)
    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
