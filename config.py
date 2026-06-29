"""
config.py — Bot configuration.
Set values here directly OR export as environment variables.
"""

import os

# ── Required ──────────────────────────────────────────────────────────────────

# Your bot token from @BotFather
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

# Channel ID where videos will be dumped.
# Format: -100XXXXXXXXXX  (get via forwarding a channel msg to @userinfobot)
DUMP_CHANNEL_ID: int | str = os.getenv("DUMP_CHANNEL_ID", "-1001234567890")

# ── Access control ────────────────────────────────────────────────────────────

# Comma-separated Telegram user IDs allowed to use the bot.
# Leave blank to allow everyone.
_raw = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: list[int] = (
    [int(x) for x in _raw.split(",") if x.strip()] if _raw else []
)

# ── Worker / concurrency ──────────────────────────────────────────────────────

# Parallel download+upload workers. 4–5 is ideal.
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))

# Max upload retries per file (flood control + network errors)
MAX_UPLOAD_RETRIES: int = int(os.getenv("MAX_UPLOAD_RETRIES", "10"))

# ── Download settings ─────────────────────────────────────────────────────────

# Max total download time per file (seconds). 3600 = 1 hour.
DOWNLOAD_TIMEOUT: int = int(os.getenv("DOWNLOAD_TIMEOUT", "3600"))

# Stream chunk size in bytes. 16 MB = fast throughput, moderate RAM.
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", str(16 * 1024 * 1024)))

# Maximum file size in MB (Telegram bot limit is 2000 MB).
MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "2000"))

# ── Storage ───────────────────────────────────────────────────────────────────

# Temp directory for video files during download (auto-cleaned after upload).
TEMP_DIR: str = os.getenv("TEMP_DIR", "/tmp/video_dump_bot")

# SQLite database path — stores all uploaded URLs for resume-on-restart.
# Keep this on persistent storage so it survives restarts!
DB_PATH: str = os.getenv("DB_PATH", "uploaded.db")
