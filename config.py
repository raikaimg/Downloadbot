"""
config.py — Bot configuration.
Copy this file, fill in your values, and keep it out of version control.
"""

import os

# ── Required ──────────────────────────────────────────────────────────────────

# Your bot token from @BotFather
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

# Channel/group ID where videos will be uploaded.
# For channels, format is -100XXXXXXXXXX (negative with -100 prefix).
# To get it: forward a message from the channel to @userinfobot.
DUMP_CHANNEL_ID: int | str = os.getenv("DUMP_CHANNEL_ID", "-1001234567890")

# ── Access control ────────────────────────────────────────────────────────────

# Telegram user IDs allowed to use the bot.
# Leave empty list [] to allow everyone.
# To get your ID: message @userinfobot.
_raw_ids = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: list[int] = (
    [int(x) for x in _raw_ids.split(",") if x.strip()]
    if _raw_ids
    else []
)

# ── Worker / concurrency ──────────────────────────────────────────────────────

# How many videos to download+upload in parallel.
# Recommended: 4–5. More = faster but more RAM/CPU.
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))

# ── Download settings ─────────────────────────────────────────────────────────

# Total timeout for one download, in seconds (e.g. 3600 = 1 hour).
DOWNLOAD_TIMEOUT: int = int(os.getenv("DOWNLOAD_TIMEOUT", "3600"))

# Stream chunk size in bytes. 8 MB = good balance of memory and speed.
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", str(8 * 1024 * 1024)))

# Maximum allowed file size in MB. Telegram limit is 2000 MB for bots.
MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "2000"))

# ── Temp storage ──────────────────────────────────────────────────────────────

# Directory for temporary video files during download.
# Should be on a fast local disk with enough free space.
TEMP_DIR: str = os.getenv("TEMP_DIR", "/tmp/video_dump_bot")
