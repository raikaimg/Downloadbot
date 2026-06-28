# Video Dump Bot

A memory-efficient, high-throughput Telegram bot that downloads direct video URLs
and uploads them to a dump channel — with 4–5 parallel workers, streaming I/O,
and no risk of hanging or OOM-crashing.

---

## Architecture

```
User sends URLs
      │
      ▼
  asyncio.Queue  (unlimited backlog)
      │
      ├──► Worker 1 ─┐
      ├──► Worker 2 ─┤  asyncio.Semaphore(MAX_WORKERS)
      ├──► Worker 3 ─┤
      └──► Worker 4 ─┘
              │
    ┌─────────┴──────────┐
    │  Stream download   │  (aiohttp, chunked, never full file in RAM)
    │  Write to /tmp     │
    │  Upload to channel │  (python-telegram-bot, chunked)
    │  Delete temp file  │
    └────────────────────┘
```

**Key optimisations:**
| Problem | Solution |
|---|---|
| RAM overload from large files | `aiohttp` chunked streaming — only 8 MB in RAM at a time |
| Too many parallel downloads | `asyncio.Semaphore(MAX_WORKERS)` hard cap |
| Disk bloat | Temp file deleted immediately after upload |
| Bot crash on bad URLs | Per-job `try/except` — one failure doesn't stop others |
| Network hangs | `aiohttp.ClientTimeout` with configurable limits |
| Systemd OOM | `MemoryMax=2G` in service file |

---

## Quick Start

### 1. Clone / copy files
```bash
mkdir ~/video_dump_bot && cd ~/video_dump_bot
# copy bot.py, config.py, requirements.txt here
```

### 2. Create virtualenv & install deps
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure
Edit `config.py` directly, **or** set environment variables:

```bash
export BOT_TOKEN="7123456789:AAF..."
export DUMP_CHANNEL_ID="-1001234567890"
export ALLOWED_USER_IDS="123456789,987654321"   # comma-separated, or leave blank for public
export MAX_WORKERS=4
```

**Getting your channel ID:**
1. Add your bot to the channel as an Admin with "Post Messages" permission.
2. Forward any message from the channel to [@userinfobot](https://t.me/userinfobot).
3. It will show you the chat ID (starts with `-100`).

### 4. Run manually (test)
```bash
source venv/bin/activate
python bot.py
```

### 5. Install as a systemd service (production)
```bash
# Edit the service file first — set User, WorkingDirectory, env vars
sudo cp video-dump-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable video-dump-bot
sudo systemctl start video-dump-bot

# Check logs
journalctl -u video-dump-bot -f
```

---

## Usage

### Send URLs
Send a message with direct video URLs — one per line:
```
https://example.com/video1.mp4
https://example.com/video2.mp4
https://cdn.site.com/clip.webm
```

### Send a text file
Upload a `.txt` file — the bot will parse all URLs from it automatically.

### Commands
| Command | Description |
|---|---|
| `/start` | Welcome message |
| `/status` | Show queue size and last 20 jobs with progress |
| `/clear` | Remove finished/failed entries from status |

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `BOT_TOKEN` | *(required)* | From @BotFather |
| `DUMP_CHANNEL_ID` | *(required)* | Target channel ID |
| `ALLOWED_USER_IDS` | `""` (open) | Comma-separated user IDs |
| `MAX_WORKERS` | `4` | Parallel download+upload jobs |
| `DOWNLOAD_TIMEOUT` | `3600` | Max seconds for one download |
| `CHUNK_SIZE` | `8388608` | Download chunk size (bytes) |
| `MAX_FILE_SIZE_MB` | `2000` | Max file size (Telegram limit) |
| `TEMP_DIR` | `/tmp/video_dump_bot` | Temp storage directory |

---

## Memory footprint estimate

With `MAX_WORKERS=4` and `CHUNK_SIZE=8 MB`:
- Active RAM ≈ 4 × 8 MB = **~32 MB** (download buffers)
- Plus ~50 MB for the Python process itself
- Disk: one video at a time per worker (auto-deleted after upload)

Even with 2 GB videos, RAM stays flat because of streaming.

---

## Troubleshooting

**Bot doesn't upload to channel:**
Make sure the bot is an **Admin** in the channel with "Post Messages" enabled.

**`python-telegram-bot` version mismatch:**
Pin to the version in `requirements.txt`: `python-telegram-bot[job-queue]==21.6`

**`FileTooLarge` error from Telegram:**
Telegram's bot API limit is 2 GB. Use the `MAX_FILE_SIZE_MB` cap to filter early.

**Download hangs forever:**
Reduce `DOWNLOAD_TIMEOUT`. Default is 3600 s (1 hour) — lower for faster failure detection.
