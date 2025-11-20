#!/usr/bin/env python3
"""Daemon that uploads files from a local folder to S3 with retention safeguards.

The script watches a configured directory, waits for files to become stable,
uploads them to S3 using a YYYY/MM/filename key structure, and records uploads in
an SQLite database so objects are never re-sent. A cleanup pass removes local
files only after they are confirmed in S3. Configuration is sourced from
``config.ini`` and covers timings, storage class, concurrency, retry behavior,
and cleanup aggressiveness.
"""

import os
import time
import logging
import sqlite3
import configparser
import threading

from collections import Counter, OrderedDict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, Optional

import boto3
import botocore.exceptions


# ----------------------------
# Config / constants
# ----------------------------

CONFIG_FILE = "config.ini"


@dataclass
class AppSettings:
    running_interval: int
    cleanup_interval: int
    log_path: str
    uploaded_files_db: str
    stable_file_age: int
    cleanup_file_age: int
    upload_workers: int
    max_s3_retries: int
    s3_retry_base_delay: float
    s3_list_page_size: int
    bucket_name: str
    directory_to_watch: str
    storage_class: str
    max_pending_uploads: int
    cleanup_batch_size: int
    cleanup_prefix_batch_size: int


def _read_config() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    read_files = parser.read(CONFIG_FILE)
    if not read_files:
        raise RuntimeError(f"{CONFIG_FILE} not found")
    return parser


def _load_settings(parser: configparser.ConfigParser) -> AppSettings:
    try:
        settings_section = parser["settings"]
        aws_section = parser["aws"]
    except KeyError as exc:
        raise RuntimeError(f"Missing config section: {exc}") from exc

    def get_required(section, key):
        value = section.get(key)
        if value is None:
            raise RuntimeError(f"Missing config key: {key}")
        return value

    running_interval = settings_section.getint("running_interval", fallback=120)
    cleanup_interval = settings_section.getint("cleanup_interval", fallback=172800)
    log_path = get_required(settings_section, "log_path")
    uploaded_files_db = get_required(settings_section, "uploaded_files_db")
    stable_file_age = settings_section.getint("stable_file_age", fallback=30)
    cleanup_file_age = settings_section.getint("cleanup_file_age", fallback=3600)
    upload_workers = max(1, settings_section.getint("upload_workers", fallback=4))
    max_s3_retries = max(1, settings_section.getint("max_s3_retries", fallback=3))
    s3_retry_base_delay = settings_section.getfloat("s3_retry_base_delay", fallback=1.0)
    s3_list_page_size = max(1, settings_section.getint("s3_list_page_size", fallback=1000))
    max_pending_uploads = max(
        upload_workers,
        settings_section.getint("max_pending_uploads", fallback=200),
    )
    cleanup_batch_size = max(1, settings_section.getint("cleanup_batch_size", fallback=200))
    cleanup_prefix_batch_size = max(
        1,
        settings_section.getint("cleanup_prefix_batch_size", fallback=25),
    )

    bucket_name = get_required(aws_section, "bucket_name")
    directory_to_watch = get_required(aws_section, "directory_to_watch")
    storage_class = aws_section.get("storage_class", "STANDARD")

    return AppSettings(
        running_interval=running_interval,
        cleanup_interval=cleanup_interval,
        log_path=log_path,
        uploaded_files_db=uploaded_files_db,
        stable_file_age=stable_file_age,
        cleanup_file_age=cleanup_file_age,
        upload_workers=upload_workers,
        max_s3_retries=max_s3_retries,
        s3_retry_base_delay=s3_retry_base_delay,
        s3_list_page_size=s3_list_page_size,
        bucket_name=bucket_name,
        directory_to_watch=directory_to_watch,
        storage_class=storage_class,
        max_pending_uploads=max_pending_uploads,
        cleanup_batch_size=cleanup_batch_size,
        cleanup_prefix_batch_size=cleanup_prefix_batch_size,
    )


SETTINGS = _load_settings(_read_config())
CONFIG_MTIME = os.path.getmtime(CONFIG_FILE)
IMMUTABLE_FIELDS = {
    "log_path",
    "uploaded_files_db",
    "upload_workers",
    "bucket_name",
    "directory_to_watch",
}
CONFIG_RELOAD_LOCK = threading.Lock()


def get_settings() -> AppSettings:
    return SETTINGS


# ----------------------------
# Logging setup
# ----------------------------

def _ensure_parent_dir(path: str) -> None:
    """Create parent directory if there is one in the path."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


_ensure_parent_dir(SETTINGS.log_path)

logger = logging.getLogger("s3_uploader")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(SETTINGS.log_path)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(message)s")
)
logger.addHandler(file_handler)

# Optional console logging (handy when running manually)
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(message)s")
)
logger.addHandler(console_handler)


def maybe_reload_settings() -> None:
    """Reload config.ini when it changes on disk."""
    global SETTINGS, CONFIG_MTIME

    with CONFIG_RELOAD_LOCK:
        try:
            mtime = os.path.getmtime(CONFIG_FILE)
        except FileNotFoundError:
            logger.error("Config file %s disappeared; keeping previous settings", CONFIG_FILE)
            return

        if mtime <= CONFIG_MTIME:
            return

        parser = _read_config()
        new_settings = _load_settings(parser)
        immutable_changes = [
            field
            for field in IMMUTABLE_FIELDS
            if getattr(new_settings, field) != getattr(SETTINGS, field)
        ]
        for field in immutable_changes:
            setattr(new_settings, field, getattr(SETTINGS, field))

        if immutable_changes:
            logger.warning(
                "Ignoring runtime changes to immutable config options: %s (restart required)",
                ", ".join(sorted(immutable_changes)),
            )

        SETTINGS = new_settings
        CONFIG_MTIME = mtime
        logger.info("Reloaded configuration from %s", CONFIG_FILE)


# ----------------------------
# SQLite helpers
# ----------------------------

DB_LOCK = threading.Lock()


class UploadStats:
    """Thread-safe counters that summarize activity for each monitor cycle."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counters = Counter()

    def increment(self, key: str) -> None:
        with self._lock:
            self._counters[key] += 1

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._counters)


def init_db(db_path: str) -> sqlite3.Connection:
    _ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            s3_key TEXT UNIQUE NOT NULL,
            local_path TEXT,
            uploaded_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def record_uploaded_file(db_conn: sqlite3.Connection, s3_key: str, local_path: str) -> None:
    with DB_LOCK:
        db_conn.execute(
            """
            INSERT OR IGNORE INTO uploaded_files (s3_key, local_path, uploaded_at)
            VALUES (?, ?, ?)
            """,
            (s3_key, local_path, datetime.utcnow().isoformat()),
        )
        db_conn.commit()


def has_been_uploaded(db_conn: sqlite3.Connection, s3_key: str) -> bool:
    with DB_LOCK:
        cur = db_conn.execute(
            "SELECT 1 FROM uploaded_files WHERE s3_key = ? LIMIT 1", (s3_key,)
        )
        return cur.fetchone() is not None


# ----------------------------
# Retry helpers
# ----------------------------

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_ERROR_CODES = {"SlowDown", "Throttling", "RequestTimeout"}


def should_retry_exception(exc: Exception) -> bool:
    """Return True when an exception is a transient S3/botocore error."""
    if isinstance(exc, botocore.exceptions.ClientError):
        error = exc.response.get("Error", {})
        code = error.get("Code", "")
        status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in RETRYABLE_ERROR_CODES:
            return True
        if status_code in RETRYABLE_STATUS_CODES:
            return True
        return False

    return isinstance(exc, botocore.exceptions.BotoCoreError)


def retry_with_backoff(operation, description: str, settings: AppSettings, max_attempts: Optional[int] = None):
    """Execute a callable with exponential backoff on retryable exceptions."""
    if max_attempts is None:
        max_attempts = settings.max_s3_retries

    delay = settings.s3_retry_base_delay
    attempt = 1

    while True:
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 - we need to inspect exception types
            if attempt >= max_attempts or not should_retry_exception(exc):
                raise

            logger.warning(
                f"{description} failed (attempt {attempt} of {max_attempts}). "
                f"Retrying in {delay:.1f}s: {exc}"
            )
            time.sleep(delay)
            delay *= 2
            attempt += 1


# ----------------------------
# S3 helpers
# ----------------------------

def s3_object_exists(s3_client, bucket_name: str, key: str, settings: AppSettings) -> bool:
    try:
        retry_with_backoff(
            lambda: s3_client.head_object(Bucket=bucket_name, Key=key),
            description=f"head_object for {key}",
            settings=settings,
        )
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        # Any other error should bubble up so we see it in logs
        raise


def fetch_existing_keys_by_prefixes(
    s3_client,
    bucket_name: str,
    prefixes,
    settings: AppSettings,
) -> dict:
    """Return a map of prefix -> keys present in S3 for the given prefixes."""
    keys_by_prefix = {}
    for prefix in prefixes:
        continuation_token = None
        prefix_keys = set()

        try:
            while True:
                def execute_request():
                    params = {
                        "Bucket": bucket_name,
                        "Prefix": prefix,
                        "MaxKeys": settings.s3_list_page_size,
                    }
                    if continuation_token:
                        params["ContinuationToken"] = continuation_token
                    return s3_client.list_objects_v2(**params)

                response = retry_with_backoff(
                    execute_request,
                    description=f"list_objects_v2 for prefix {prefix}",
                    settings=settings,
                )

                for obj in response.get("Contents", []):
                    key = obj.get("Key")
                    if key:
                        prefix_keys.add(key)

                if response.get("IsTruncated"):
                    continuation_token = response.get("NextContinuationToken")
                else:
                    break

            keys_by_prefix[prefix] = prefix_keys

        except Exception as exc:  # noqa: BLE001 - surfaced via log, cleanup keeps file
            logger.error(
                f"Failed to list objects for prefix {prefix}: {exc}",
                exc_info=True,
            )

    return keys_by_prefix


# ----------------------------
# File helpers
# ----------------------------

def get_s3_key_for_file(file_path: str) -> str:
    """
    Build S3 key as YEAR/MONTH/filename based on file mtime.
    That key is what we must never overwrite, to preserve lifecycle / retention.
    """
    file_name = os.path.basename(file_path)
    file_mod_time = os.path.getmtime(file_path)
    file_date = datetime.fromtimestamp(file_mod_time)

    year = file_date.strftime("%Y")
    month = file_date.strftime("%m")
    s3_folder = f"{year}/{month}/"
    return s3_folder + file_name


def _prefix_for_s3_key(s3_key: str) -> str:
    parts = s3_key.rsplit("/", 1)
    if len(parts) == 2:
        return parts[0] + "/"
    return ""


def is_stable_file(file_path: str, min_age_seconds: int) -> bool:
    """
    "Stable" means not modified in the last min_age_seconds.
    Helps avoid uploading partially written files.
    """
    mtime = os.path.getmtime(file_path)
    age = time.time() - mtime
    return age >= min_age_seconds


def iter_watched_files(directory: str) -> Iterable[str]:
    """
    Yield files under the directory recursively, skipping hidden dirs/files.
    """
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for file_name in files:
            if file_name.startswith("."):
                continue
            file_path = os.path.join(root, file_name)
            if os.path.isfile(file_path):
                yield file_path


# ----------------------------
# Core logic
# ----------------------------

def upload_file_to_s3(
    file_path: str,
    bucket_name: str,
    s3_client,
    db_conn: sqlite3.Connection,
    stats: UploadStats,
    settings: AppSettings,
) -> None:
    try:
        if not os.path.isfile(file_path):
            return

        # Only upload stable files
        if not is_stable_file(file_path, settings.stable_file_age):
            return

        s3_key = get_s3_key_for_file(file_path)

        # Check local DB
        if has_been_uploaded(db_conn, s3_key):
            stats.increment("skipped_db")
            return

        # Double check against S3 to avoid overwriting existing object
        if s3_object_exists(s3_client, bucket_name, s3_key, settings):
            # Ensure DB is consistent with reality
            record_uploaded_file(db_conn, s3_key, file_path)
            stats.increment("skipped_s3")
            return

        # Safe to upload: key does not exist yet
        retry_with_backoff(
            lambda: s3_client.upload_file(
                file_path,
                bucket_name,
                s3_key,
                ExtraArgs={"StorageClass": settings.storage_class},
            ),
            description=f"upload_file for {s3_key}",
            settings=settings,
        )
        logger.info(
            f"Uploaded {file_path} to s3://{bucket_name}/{s3_key} "
            f"with storage class {settings.storage_class}"
        )

        record_uploaded_file(db_conn, s3_key, file_path)
        stats.increment("uploaded")

    except FileNotFoundError:
        stats.increment("missing")
    except Exception as e:
        logger.error(f"Upload failed for {file_path}: {e}", exc_info=True)
        stats.increment("failed")


def cleanup_folder(
    directory: str,
    bucket_name: str,
    s3_client,
    db_conn: sqlite3.Connection,
    settings: AppSettings,
) -> None:
    """
    Remove local files that:
    - are regular files
    - are older than CLEANUP_FILE_AGE
    - correspond to S3 keys that are uploaded (either in DB or confirmed via S3)
    """
    try:
        now = time.time()

        pending_by_prefix = OrderedDict()
        files_considered = 0

        for file_path in iter_watched_files(directory):
            if files_considered >= settings.cleanup_batch_size:
                break

            try:
                age = now - os.path.getmtime(file_path)
            except FileNotFoundError:
                continue

            if age < settings.cleanup_file_age:
                continue

            s3_key = get_s3_key_for_file(file_path)

            if has_been_uploaded(db_conn, s3_key):
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    pass
                else:
                    logger.info(f"Removed local file (already uploaded): {file_path}")
                continue

            prefix = _prefix_for_s3_key(s3_key)

            if (
                prefix not in pending_by_prefix
                and len(pending_by_prefix) >= settings.cleanup_prefix_batch_size
            ):
                continue

            pending_by_prefix.setdefault(prefix, []).append((file_path, s3_key))
            files_considered += 1

        if not pending_by_prefix:
            return

        existing_keys = fetch_existing_keys_by_prefixes(
            s3_client,
            bucket_name,
            pending_by_prefix.keys(),
            settings,
        )

        for prefix, file_entries in pending_by_prefix.items():
            known_keys = existing_keys.get(prefix, set())
            for file_path, s3_key in file_entries:
                if s3_key in known_keys:
                    record_uploaded_file(db_conn, s3_key, file_path)
                    try:
                        os.remove(file_path)
                    except FileNotFoundError:
                        continue
                    logger.info(
                        f"Removed local file (S3 confirms uploaded): {file_path}"
                    )

    except Exception as e:
        logger.error(f"Cleanup failed for directory {directory}: {e}", exc_info=True)


def monitor_folder(
    s3_client,
    db_conn: sqlite3.Connection,
) -> None:
    settings = get_settings()
    directory = settings.directory_to_watch
    bucket_name = settings.bucket_name

    last_cleanup = datetime.now()
    stats = UploadStats()
    last_summary = {}
    inflight_uploads = set()

    logger.info(
        f"Starting folder monitor. Directory={directory}, "
        f"Bucket={bucket_name}, Interval={settings.running_interval}s, "
        f"Cleanup every {settings.cleanup_interval}s, StorageClass={settings.storage_class}, "
        f"Workers={settings.upload_workers}"
    )

    with ThreadPoolExecutor(max_workers=settings.upload_workers) as executor:
        while True:
            try:
                maybe_reload_settings()
                settings = get_settings()
                directory = settings.directory_to_watch
                bucket_name = settings.bucket_name

                # Clean completed futures so queue length reflects in-flight uploads
                inflight_uploads = {f for f in inflight_uploads if not f.done()}
                pending_capacity = max(settings.max_pending_uploads - len(inflight_uploads), 0)

                for file_path in iter_watched_files(directory):
                    if pending_capacity <= 0:
                        break

                    future = executor.submit(
                        upload_file_to_s3,
                        file_path,
                        bucket_name,
                        s3_client,
                        db_conn,
                        stats,
                        settings,
                    )
                    inflight_uploads.add(future)
                    pending_capacity -= 1

                if pending_capacity == 0:
                    logger.info(
                        "Upload queue is full (%d in flight); remaining files will be retried next cycle",
                        len(inflight_uploads),
                    )

                if datetime.now() - last_cleanup >= timedelta(seconds=settings.cleanup_interval):
                    cleanup_folder(directory, bucket_name, s3_client, db_conn, settings)
                    last_cleanup = datetime.now()

                current_summary = stats.snapshot()
                delta = {
                    "uploaded": current_summary.get("uploaded", 0) - last_summary.get("uploaded", 0),
                    "skipped_db": current_summary.get("skipped_db", 0) - last_summary.get("skipped_db", 0),
                    "skipped_s3": current_summary.get("skipped_s3", 0) - last_summary.get("skipped_s3", 0),
                    "failed": current_summary.get("failed", 0) - last_summary.get("failed", 0),
                    "missing": current_summary.get("missing", 0) - last_summary.get("missing", 0),
                }
                if any(value > 0 for value in delta.values()):
                    logger.info(
                        "Cycle summary: uploads=%d skipped_db=%d skipped_s3=%d missing=%d failures=%d",
                        delta["uploaded"],
                        delta["skipped_db"],
                        delta["skipped_s3"],
                        delta["missing"],
                        delta["failed"],
                    )
                last_summary = current_summary

                time.sleep(settings.running_interval)

            except KeyboardInterrupt:
                logger.info("Shutting down due to KeyboardInterrupt")
                break
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}", exc_info=True)
                time.sleep(5)


def main():
    settings = get_settings()

    if not os.path.isdir(settings.directory_to_watch):
        raise RuntimeError(
            f"Directory to watch does not exist: {settings.directory_to_watch}"
        )

    db_conn = init_db(settings.uploaded_files_db)
    s3_client = boto3.client("s3")

    try:
        monitor_folder(s3_client, db_conn)
    finally:
        db_conn.close()


if __name__ == "__main__":
    main()
