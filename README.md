# S3 Uploader

Python daemon that uploads files from a local directory to S3 once they have stayed stable for a configurable period. Uploaded keys are tracked in SQLite so files are never resent, and stale local files are deleted only after S3 confirms they exist.

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
3. Fill out `config.ini` with your watch directory and S3 bucket (sample values are provided).
4. Launch the uploader while the virtualenv is active:
   ```bash
   python s3_uploader.py
   ```

The SQLite database and log file paths are configured inside `config.ini`. Both folders are ignored by git so local runs don't pollute the repo. Deactivate the environment with `deactivate` when finished.
