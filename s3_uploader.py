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

from collections import Counter
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore.exceptions


# ----------------------------
# Config / constants
# ----------------------------

CONFIG_FILE = "config.ini"

config = configparser.ConfigParser()
read_files = config.read(CONFIG_FILE)
if not read_files:
    raise RuntimeError(f"{CONFIG_FILE} not found")

try:
    RUNNING_INTERVAL = int(config["settings"]["running_interval"])
    CLEANUP_INTERVAL = int(config["settings"]["cleanup_interval"])
    LOG_PATH = config["settings"]["log_path"]
    UPLOADED_FILES_DB = config["settings"]["uploaded_files_db"]
    # Optional tunables
    STABLE_FILE_AGE = int(config["settings"].get("stable_file_age", 30))       # seconds
    CLEANUP_FILE_AGE = int(config["settings"].get("cleanup_file_age", 3600))   # seconds
    UPLOAD_WORKERS = max(1, int(config["settings"].get("upload_workers", 4)))
    MAX_S3_RETRIES = max(1, int(config["settings"].get("max_s3_retries", 3)))
    S3_RETRY_BASE_DELAY = float(config["settings"].get("s3_retry_base_delay", 1.0))
    S3_LIST_PAGE_SIZE = max(1, int(config["settings"].get("s3_list_page_size", 1000)))

    BUCKET_NAME = config["aws"]["bucket_name"]
    DIRECTORY_TO_WATCH = config["aws"]["directory_to_watch"]
    STORAGE_CLASS = config["aws"].get("storage_class", "STANDARD")
except KeyError as e:
    raise RuntimeError(f"Missing config key: {e}")


# ----------------------------
# Logging setup
# ----------------------------

def _ensure_parent_dir(path: str) -> None:
    """Create parent directory if there is one in the path."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


_ensure_parent_dir(LOG_PATH)

logger = logging.getLogger("s3_uploader")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_PATH)
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


def retry_with_backoff(operation, description: str, max_attempts: int = MAX_S3_RETRIES):
    """Execute a callable with exponential backoff on retryable exceptions."""
    delay = S3_RETRY_BASE_DELAY
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

def s3_object_exists(s3_client, bucket_name: str, key: str) -> bool:
    try:
        retry_with_backoff(
            lambda: s3_client.head_object(Bucket=bucket_name, Key=key),
            description=f"head_object for {key}",
        )
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        # Any other error should bubble up so we see it in logs
        raise


def fetch_existing_keys_by_prefixes(s3_client, bucket_name: str, prefixes) -> dict:
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
                        "MaxKeys": S3_LIST_PAGE_SIZE,
                    }
                    if continuation_token:
                        params["ContinuationToken"] = continuation_token
                    return s3_client.list_objects_v2(**params)

                response = retry_with_backoff(
                    execute_request,
                    description=f"list_objects_v2 for prefix {prefix}",
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


# ----------------------------
# Core logic
# ----------------------------

def upload_file_to_s3(
    file_path: str,
    bucket_name: str,
    s3_client,
    db_conn: sqlite3.Connection,
    stats: UploadStats,
) -> None:
    try:
        if not os.path.isfile(file_path):
            return

        # Only upload stable files
        if not is_stable_file(file_path, STABLE_FILE_AGE):
            return

        s3_key = get_s3_key_for_file(file_path)

        # Check local DB
        if has_been_uploaded(db_conn, s3_key):
            stats.increment("skipped_db")
            return

        # Double check against S3 to avoid overwriting existing object
        if s3_object_exists(s3_client, bucket_name, s3_key):
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
                ExtraArgs={"StorageClass": STORAGE_CLASS},
            ),
            description=f"upload_file for {s3_key}",
        )
        logger.info(
            f"Uploaded {file_path} to s3://{bucket_name}/{s3_key} "
            f"with storage class {STORAGE_CLASS}"
        )

        record_uploaded_file(db_conn, s3_key, file_path)
        stats.increment("uploaded")

    except Exception as e:
        logger.error(f"Upload failed for {file_path}: {e}", exc_info=True)
        stats.increment("failed")


def cleanup_folder(
    directory: str,
    bucket_name: str,
    s3_client,
    db_conn: sqlite3.Connection
) -> None:
    """
    Remove local files that:
    - are regular files
    - are older than CLEANUP_FILE_AGE
    - correspond to S3 keys that are uploaded (either in DB or confirmed via S3)
    """
    try:
        now = time.time()

        pending_by_prefix = {}

        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)

            if not os.path.isfile(file_path):
                continue

            age = now - os.path.getmtime(file_path)
            if age < CLEANUP_FILE_AGE:
                continue

            s3_key = get_s3_key_for_file(file_path)

            if has_been_uploaded(db_conn, s3_key):
                os.remove(file_path)
                logger.info(f"Removed local file (already uploaded): {file_path}")
                continue

            prefix = _prefix_for_s3_key(s3_key)
            pending_by_prefix.setdefault(prefix, []).append((file_path, s3_key))

        if not pending_by_prefix:
            return

        existing_keys = fetch_existing_keys_by_prefixes(
            s3_client,
            bucket_name,
            pending_by_prefix.keys(),
        )

        for prefix, file_entries in pending_by_prefix.items():
            known_keys = existing_keys.get(prefix, set())
            for file_path, s3_key in file_entries:
                if s3_key in known_keys:
                    record_uploaded_file(db_conn, s3_key, file_path)
                    os.remove(file_path)
                    logger.info(
                        f"Removed local file (S3 confirms uploaded): {file_path}"
                    )

    except Exception as e:
        logger.error(f"Cleanup failed for directory {directory}: {e}", exc_info=True)


def monitor_folder(
    directory: str,
    bucket_name: str,
    s3_client,
    db_conn: sqlite3.Connection
) -> None:
    last_cleanup = datetime.now()
    stats = UploadStats()
    last_summary = {}

    logger.info(
        f"Starting folder monitor. Directory={directory}, "
        f"Bucket={bucket_name}, Interval={RUNNING_INTERVAL}s, "
        f"Cleanup every {CLEANUP_INTERVAL}s, StorageClass={STORAGE_CLASS}"
    )

    inflight_uploads = set()

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as executor:
        while True:
            try:
                # Upload new / changed files
                for file_name in os.listdir(directory):
                    # Ignore hidden / temp files if you want
                    if file_name.startswith("."):
                        continue

                    file_path = os.path.join(directory, file_name)

                    if os.path.isfile(file_path):
                        future = executor.submit(
                            upload_file_to_s3,
                            file_path,
                            bucket_name,
                            s3_client,
                            db_conn,
                            stats,
                        )
                        inflight_uploads.add(future)

                # Clean up completed futures so the set doesn't grow forever
                inflight_uploads = {f for f in inflight_uploads if not f.done()}

                # Periodic cleanup
                if datetime.now() - last_cleanup >= timedelta(seconds=CLEANUP_INTERVAL):
                    cleanup_folder(directory, bucket_name, s3_client, db_conn)
                    last_cleanup = datetime.now()

                # Emit summary for deltas since last log so counts stay accurate
                current_summary = stats.snapshot()
                delta = {
                    "uploaded": current_summary.get("uploaded", 0) - last_summary.get("uploaded", 0),
                    "skipped_db": current_summary.get("skipped_db", 0) - last_summary.get("skipped_db", 0),
                    "skipped_s3": current_summary.get("skipped_s3", 0) - last_summary.get("skipped_s3", 0),
                    "failed": current_summary.get("failed", 0) - last_summary.get("failed", 0),
                }
                if any(value > 0 for value in delta.values()):
                    logger.info(
                        "Cycle summary: uploads=%d skipped_db=%d skipped_s3=%d failures=%d",
                        delta["uploaded"],
                        delta["skipped_db"],
                        delta["skipped_s3"],
                        delta["failed"],
                    )
                last_summary = current_summary

                time.sleep(RUNNING_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down due to KeyboardInterrupt")
                break
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}", exc_info=True)
                # Avoid a tight error loop
                time.sleep(5)


def main():
    if not os.path.isdir(DIRECTORY_TO_WATCH):
        raise RuntimeError(f"Directory to watch does not exist: {DIRECTORY_TO_WATCH}")

    db_conn = init_db(UPLOADED_FILES_DB)
    s3_client = boto3.client("s3")

    try:
        monitor_folder(DIRECTORY_TO_WATCH, BUCKET_NAME, s3_client, db_conn)
    finally:
        db_conn.close()


if __name__ == "__main__":
    main()
