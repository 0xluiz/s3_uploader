# S3 Uploader

Python daemon that uploads files from a local directory to S3 once they have stayed stable for a configurable period. Uploaded keys are tracked in SQLite so files are never resent, and stale local files are deleted only after S3 confirms they exist.

## Configure

1. Copy the example configuration and edit it with your values:
   ```bash
   cp config.example.ini config.ini
   $EDITOR config.ini
   ```
2. Ensure the `directory_to_watch` exists locally and that your AWS credentials (environment variables or shared config) have read/write access to the bucket.

## Run Inside a Virtual Environment

1. Create and activate a virtual environment (any name works):
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install the required dependencies into that environment:
   ```bash
   pip install -r requirements.txt
   ```
3. Launch the uploader while the virtualenv is active:
   ```bash
   python s3_uploader.py
   ```

The SQLite database and log file paths are configured inside `config.ini`. Both folders are ignored by git so local runs don't pollute the repo. Deactivate the environment with `deactivate` when finished.

## Runtime Behavior Highlights

- Recursively walks the watch directory and skips hidden files/folders so nested log trees are handled automatically.
- Upload submission is throttled via `max_pending_uploads` to keep memory bounded even if the directory suddenly fills with thousands of files.
- Cleanup operations respect `cleanup_batch_size` and `cleanup_prefix_batch_size` to cap both local scanning and S3 `ListObjectsV2` traffic.

Tune these knobs inside `config.ini` without restarting (see next section) to strike the balance that fits your workload.

## Hot Reloading Config

The daemon re-reads `config.ini` whenever it changes and picks up new intervals, retry/backoff settings, cleanup aggressiveness, and storage-class tweaks automatically. Changes to immutable items (directory, bucket, upload worker count, log/db paths) require a restart—an informational warning is logged if you modify them at runtime.

## Run Under systemd

A template unit file lives at `contrib/systemd/s3_uploader.service`. Customize the `WorkingDirectory`, `ExecStart`, and `User` values, copy it to `/etc/systemd/system/`, then enable it:

```bash
sudo cp contrib/systemd/s3_uploader.service /etc/systemd/system/s3_uploader.service
sudo systemctl daemon-reload
sudo systemctl enable --now s3_uploader.service
```

Point the service at the virtualenv interpreter (e.g., `/opt/s3_uploader/.venv/bin/python`). systemd will supervise restarts so the uploader stays running continuously.
