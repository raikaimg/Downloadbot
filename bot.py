"""
Video Dump Bot v2 - Optimized with:
- Flood control auto-retry (RetryAfter handling)
- Resume system via SQLite (skips already-uploaded URLs on restart)
- Speed optimizations: connection pooling, larger chunks, download speed display
"""

import asyncio
import logging
import os
import re
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

import aiohttp
from telegram import Update, Message
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.constants import ParseMode

from config import (
    BOT_TOKEN,
    DUMP_CHANNEL_ID,
    MAX_WORKERS,
    DOWNLOAD_TIMEOUT,
    CHUNK_SIZE,
    MAX_FILE_SIZE_MB,
    TEMP_DIR,
    ALLOWED_USER_IDS,
    DB_PATH,
    MAX_UPLOAD_RETRIES,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Global state ──────────────────────────────────────────────────────────────
url_queue: asyncio.Queue = asyncio.Queue()
semaphore: Optional[asyncio.Semaphore] = None
active_jobs: dict[str, dict] = {}
worker_tasks: list[asyncio.Task] = []


# ── Database (resume system) ──────────────────────────────────────────────────

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS uploaded (
            url TEXT PRIMARY KEY,
            filename TEXT,
            size_bytes INTEGER,
            uploaded_at REAL
        )
    """)
    conn.commit()
    return conn

_db_lock = asyncio.Lock()
_db: Optional[sqlite3.Connection] = None

def _get_db() -> sqlite3.Connection:
    global _db
    if _db is None:
        _db = db_connect()
    return _db

async def is_uploaded(url: str) -> bool:
    async with _db_lock:
        row = _get_db().execute(
            "SELECT 1 FROM uploaded WHERE url = ?", (url,)
        ).fetchone()
        return row is not None

async def mark_uploaded(url: str, filename: str, size_bytes: int) -> None:
    async with _db_lock:
        _get_db().execute(
            "INSERT OR REPLACE INTO uploaded (url, filename, size_bytes, uploaded_at) "
            "VALUES (?, ?, ?, ?)",
            (url, filename, size_bytes, time.time()),
        )
        _get_db().commit()

async def get_upload_stats() -> dict:
    async with _db_lock:
        row = _get_db().execute(
            "SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM uploaded"
        ).fetchone()
        return {"count": row[0], "total_bytes": row[1]}


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_authorized(user_id: int) -> bool:
    if not ALLOWED_USER_IDS:
        return True
    return user_id in ALLOWED_USER_IDS

def extract_urls(text: str) -> list[str]:
    pattern = r'https?://[^\s\]\[\(\)\"\'<>]+'
    return [u.strip() for u in re.findall(pattern, text) if u.strip()]

def human_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"

def guess_filename(url: str, content_type: str = "") -> str:
    name = url.split("?")[0].rstrip("/").split("/")[-1]
    if not name or "." not in name:
        ct = content_type.lower()
        ext = (
            ".mp4"  if "mp4"       in ct else
            ".webm" if "webm"      in ct else
            ".mkv"  if "matroska"  in ct or "mkv" in ct else
            ".avi"  if "avi"       in ct else
            ".mov"  if "quicktime" in ct or "mov" in ct else
            ".mp4"
        )
        name = f"video_{uuid.uuid4().hex[:8]}{ext}"
    return re.sub(r'[^\w.\-]', '_', name)


# ── Download ──────────────────────────────────────────────────────────────────

async def stream_download(
    url: str,
    dest_dir: Path,
    job_id: str,
    session: aiohttp.ClientSession,
) -> tuple[int, str]:
    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024

    timeout = aiohttp.ClientTimeout(
        total=DOWNLOAD_TIMEOUT,
        connect=30,
        sock_read=120,
    )

    async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
        resp.raise_for_status()

        content_type   = resp.headers.get("Content-Type", "")
        content_length = int(resp.headers.get("Content-Length", 0) or 0)

        if content_length and content_length > max_bytes:
            raise ValueError(
                f"File too large: {human_size(content_length)} "
                f"(limit {MAX_FILE_SIZE_MB} MB)"
            )

        filename  = guess_filename(url, content_type)
        file_path = dest_dir / filename

        downloaded = 0
        last_log   = time.time()
        start      = time.time()

        with open(file_path, "wb", buffering=CHUNK_SIZE) as f:
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                if not chunk:
                    break
                downloaded += len(chunk)
                if downloaded > max_bytes:
                    file_path.unlink(missing_ok=True)
                    raise ValueError(f"File exceeded {MAX_FILE_SIZE_MB} MB limit")
                f.write(chunk)

                now = time.time()
                if now - last_log >= 4:
                    elapsed = now - start
                    speed   = downloaded / elapsed if elapsed > 0 else 0
                    pct     = (
                        f" ({downloaded/content_length*100:.0f}%)"
                        if content_length else ""
                    )
                    active_jobs[job_id]["progress"] = (
                        f"⬇️ {human_size(downloaded)}{pct} "
                        f"@ {human_size(int(speed))}/s"
                    )
                    last_log = now

    return downloaded, str(file_path)


# ── Upload with flood-control retry ──────────────────────────────────────────

async def upload_to_channel(
    file_path: str,
    caption: str,
    context: ContextTypes.DEFAULT_TYPE,
    job_id: str,
) -> None:
    file_size = os.path.getsize(file_path)
    active_jobs[job_id]["progress"] = f"📤 Uploading {human_size(file_size)}…"

    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
        try:
            with open(file_path, "rb") as video_file:
                await context.bot.send_video(
                    chat_id=DUMP_CHANNEL_ID,
                    video=video_file,
                    caption=caption[:1024],
                    supports_streaming=True,
                    read_timeout=600,
                    write_timeout=600,
                    connect_timeout=60,
                )
            return  # success

        except RetryAfter as e:
            wait = int(e.retry_after) + 2
            logger.warning(
                "Job %s — flood control, waiting %ds (attempt %d/%d)",
                job_id, wait, attempt, MAX_UPLOAD_RETRIES,
            )
            active_jobs[job_id]["progress"] = (
                f"⏳ Flood control — retrying in {wait}s "
                f"(attempt {attempt}/{MAX_UPLOAD_RETRIES})"
            )
            await asyncio.sleep(wait)

        except (TimedOut, NetworkError) as e:
            wait = 15 * attempt
            logger.warning(
                "Job %s — network error: %s, retry in %ds (attempt %d/%d)",
                job_id, e, wait, attempt, MAX_UPLOAD_RETRIES,
            )
            active_jobs[job_id]["progress"] = (
                f"🔄 Network error, retry {attempt}/{MAX_UPLOAD_RETRIES} in {wait}s"
            )
            await asyncio.sleep(wait)

        except TelegramError as e:
            raise RuntimeError(f"Telegram error: {e}") from e

    raise RuntimeError(
        f"Upload failed after {MAX_UPLOAD_RETRIES} attempts (flood/network)"
    )


# ── Worker ────────────────────────────────────────────────────────────────────

async def worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    connector = aiohttp.TCPConnector(
        limit=4,
        limit_per_host=2,
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=300,
    )
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0 (compatible; VideoDumpBot/2.0)"},
        connector=connector,
        trust_env=True,
    ) as session:

        while True:
            job     = await url_queue.get()
            job_id  = job["id"]
            url     = job["url"]
            notify_msg: Optional[Message] = job.get("notify_msg")

            # Resume check — skip if already uploaded
            if await is_uploaded(url):
                logger.info("SKIP (already uploaded): %s", url)
                active_jobs[job_id] = {
                    "url": url, "status": "skipped",
                    "progress": "⏭️ Already uploaded (skipped)",
                    "start": time.time(),
                }
                url_queue.task_done()
                continue

            async with semaphore:
                active_jobs[job_id] = {
                    "url": url, "status": "running",
                    "progress": "⏳ Starting…",
                    "start": time.time(),
                }

                tmp_dir: Path           = Path(tempfile.mkdtemp(dir=TEMP_DIR))
                file_path: Optional[str] = None

                try:
                    active_jobs[job_id]["progress"] = "⬇️ Connecting…"
                    size, file_path = await stream_download(
                        url, tmp_dir, job_id, session
                    )

                    fname   = Path(file_path).name
                    caption = (
                        f"🎬 {fname}\n"
                        f"📦 {human_size(size)}\n"
                        f"🔗 {url}"
                    )
                    await upload_to_channel(file_path, caption, context, job_id)

                    await mark_uploaded(url, fname, size)
                    elapsed = time.time() - active_jobs[job_id]["start"]
                    active_jobs[job_id]["status"]   = "done"
                    active_jobs[job_id]["progress"] = (
                        f"✅ Done in {elapsed:.0f}s ({human_size(size)})"
                    )
                    logger.info("Done: %s", url)

                    if notify_msg:
                        try:
                            await notify_msg.reply_text(
                                f"✅ Uploaded: `{fname}`",
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError:
                            pass

                except asyncio.CancelledError:
                    active_jobs[job_id]["status"] = "cancelled"
                    raise

                except Exception as exc:
                    active_jobs[job_id]["status"]   = "error"
                    active_jobs[job_id]["progress"] = f"❌ {exc}"
                    logger.error("FAIL %s — %s", url, exc)

                    if notify_msg:
                        try:
                            await notify_msg.reply_text(
                                f"❌ Failed: `{url[:80]}`\n`{exc}`",
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError:
                            pass

                finally:
                    try:
                        if file_path and Path(file_path).exists():
                            Path(file_path).unlink()
                        if tmp_dir.exists():
                            tmp_dir.rmdir()
                    except Exception:
                        pass

            url_queue.task_done()


# ── Command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    stats = await get_upload_stats()
    await update.message.reply_text(
        "🤖 *Video Dump Bot v2*\n\n"
        "Send video URLs (one per line) or a `.txt` file.\n"
        "Already-uploaded URLs are skipped automatically on restart.\n\n"
        f"📊 Total uploaded: *{stats['count']}* files "
        f"({human_size(stats['total_bytes'])})\n\n"
        "Commands:\n"
        "/status  — queue & active jobs\n"
        "/history — last 20 uploaded files\n"
        "/clear   — clear finished jobs from status\n",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        return
    pending = url_queue.qsize()
    lines   = [f"📊 *Queue:* {pending} pending  |  *Workers:* {MAX_WORKERS}\n"]
    for _, info in list(active_jobs.items())[-20:]:
        icon = {
            "running":   "🔄", "done":      "✅",
            "error":     "❌", "cancelled": "⛔", "skipped": "⏭️",
        }.get(info["status"], "❓")
        short_url = info["url"][:55] + ("…" if len(info["url"]) > 55 else "")
        lines.append(f"{icon} `{short_url}`\n   {info['progress']}")
    await update.message.reply_text(
        "\n".join(lines) or "Nothing running.", parse_mode=ParseMode.MARKDOWN
    )

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        return
    async with _db_lock:
        rows = _get_db().execute(
            "SELECT url, filename, size_bytes, uploaded_at "
            "FROM uploaded ORDER BY uploaded_at DESC LIMIT 20"
        ).fetchall()
    if not rows:
        await update.message.reply_text("No uploads recorded yet.")
        return
    lines = ["📁 *Last 20 uploads:*\n"]
    for url, fname, size, ts in rows:
        t = time.strftime("%d/%m %H:%M", time.localtime(ts))
        lines.append(
            f"• `{fname}` ({human_size(size)}) — {t}\n"
            f"  `{url[:60]}{'…' if len(url)>60 else ''}`"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        return
    removed = 0
    for jid in list(active_jobs):
        if active_jobs[jid]["status"] in ("done", "error", "cancelled", "skipped"):
            del active_jobs[jid]
            removed += 1
    await update.message.reply_text(f"🧹 Cleared {removed} finished jobs.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return

    text = ""
    if update.message.text:
        text = update.message.text
    elif update.message.document:
        doc = update.message.document
        if "text" in (doc.mime_type or ""):
            tg_file = await context.bot.get_file(doc.file_id)
            raw     = await tg_file.download_as_bytearray()
            text    = raw.decode("utf-8", errors="ignore")
        else:
            await update.message.reply_text("❌ Please send a `.txt` file.")
            return

    urls = extract_urls(text)
    if not urls:
        await update.message.reply_text("❌ No valid URLs found.")
        return

    new_urls, skip_count = [], 0
    for url in urls:
        if await is_uploaded(url):
            skip_count += 1
        else:
            new_urls.append(url)

    for url in new_urls:
        await url_queue.put({
            "id":          uuid.uuid4().hex[:12],
            "url":         url,
            "notify_msg":  update.message,
        })

    parts = [f"✅ Queued *{len(new_urls)}* URL(s)."]
    if skip_count:
        parts.append(f"⏭️ Skipped *{skip_count}* already uploaded.")
    parts += [f"📥 Queue depth: {url_queue.qsize()}", "Use /status to track progress."]
    await update.message.reply_text("\n".join(parts), parse_mode=ParseMode.MARKDOWN)


# ── Startup / Shutdown ────────────────────────────────────────────────────────

async def on_startup(application: Application) -> None:
    global semaphore
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    _get_db()
    for i in range(MAX_WORKERS):
        task = asyncio.create_task(worker(application), name=f"worker-{i}")
        worker_tasks.append(task)
        logger.info("Started worker-%d", i)
    stats = _get_db().execute("SELECT COUNT(*) FROM uploaded").fetchone()[0]
    logger.info("Bot ready | workers=%d | db_records=%d | dump=%s",
                MAX_WORKERS, stats, DUMP_CHANNEL_ID)

async def on_shutdown(application: Application) -> None:
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    if _db:
        _db.close()
    logger.info("Shutdown complete.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("clear",   cmd_clear))
    app.add_handler(
        MessageHandler(
            (filters.TEXT | filters.Document.ALL) & ~filters.COMMAND,
            handle_message,
        )
    )
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
