"""
Video Dump Bot - Optimized Telegram bot for bulk video downloading and uploading.

Architecture:
- asyncio.Queue  → holds pending video URLs
- asyncio.Semaphore(MAX_WORKERS) → limits concurrent download+upload jobs
- Streaming download via aiohttp → never loads full file into RAM
- Chunked upload via python-telegram-bot → handles large files correctly
- Auto-cleanup of temp files after each job
"""

import asyncio
import logging
import os
import re
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
from telegram.error import TelegramError
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
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Global state ──────────────────────────────────────────────────────────────
url_queue: asyncio.Queue = asyncio.Queue()
semaphore: Optional[asyncio.Semaphore] = None
active_jobs: dict[str, dict] = {}   # job_id → {url, status, progress, msg}
worker_tasks: list[asyncio.Task] = []


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_authorized(user_id: int) -> bool:
    if not ALLOWED_USER_IDS:
        return True  # open to all if list is empty
    return user_id in ALLOWED_USER_IDS


def extract_urls(text: str) -> list[str]:
    """Extract http/https URLs, one per line or space-separated."""
    pattern = r'https?://[^\s\]\[\(\)\"\'<>]+'
    return [u.strip() for u in re.findall(pattern, text) if u.strip()]


def human_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def guess_filename(url: str, content_type: str = "") -> str:
    """Derive a safe filename from URL or content-type."""
    name = url.split("?")[0].rstrip("/").split("/")[-1]
    if not name or "." not in name:
        ext = ""
        if "mp4" in content_type:
            ext = ".mp4"
        elif "webm" in content_type:
            ext = ".webm"
        elif "mkv" in content_type or "matroska" in content_type:
            ext = ".mkv"
        elif "avi" in content_type:
            ext = ".avi"
        elif "mov" in content_type or "quicktime" in content_type:
            ext = ".mov"
        else:
            ext = ".mp4"
        name = f"video_{uuid.uuid4().hex[:8]}{ext}"
    # Sanitize
    name = re.sub(r'[^\w.\-]', '_', name)
    return name


# ── Download ──────────────────────────────────────────────────────────────────

async def stream_download(
    url: str,
    dest_path: Path,
    job_id: str,
    session: aiohttp.ClientSession,
) -> tuple[int, str]:
    """
    Stream-download URL to dest_path.
    Returns (total_bytes, filename).
    Raises on error or file-too-large.
    """
    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024

    timeout = aiohttp.ClientTimeout(
        total=DOWNLOAD_TIMEOUT,
        connect=30,
        sock_read=60,
    )

    async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        content_length = int(resp.headers.get("Content-Length", 0))

        if content_length and content_length > max_bytes:
            raise ValueError(
                f"File too large: {human_size(content_length)} "
                f"(limit {MAX_FILE_SIZE_MB} MB)"
            )

        filename = guess_filename(url, content_type)
        file_path = dest_path / filename

        downloaded = 0
        last_log = time.time()

        with open(file_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                if not chunk:
                    break
                downloaded += len(chunk)
                if downloaded > max_bytes:
                    f.close()
                    file_path.unlink(missing_ok=True)
                    raise ValueError(
                        f"File exceeded {MAX_FILE_SIZE_MB} MB during download"
                    )
                f.write(chunk)

                # Update progress every 5 s
                now = time.time()
                if now - last_log >= 5:
                    active_jobs[job_id]["progress"] = (
                        f"⬇️ Downloading: {human_size(downloaded)}"
                        + (f" / {human_size(content_length)}" if content_length else "")
                    )
                    last_log = now

        return downloaded, str(file_path)


# ── Upload ────────────────────────────────────────────────────────────────────

async def upload_to_channel(
    file_path: str,
    caption: str,
    context: ContextTypes.DEFAULT_TYPE,
    job_id: str,
) -> None:
    """Upload video file to the dump channel."""
    active_jobs[job_id]["progress"] = "📤 Uploading to channel…"

    file_size = os.path.getsize(file_path)
    logger.info(
        "Uploading %s (%s) to channel %s",
        file_path, human_size(file_size), DUMP_CHANNEL_ID,
    )

    with open(file_path, "rb") as video_file:
        await context.bot.send_video(
            chat_id=DUMP_CHANNEL_ID,
            video=video_file,
            caption=caption[:1024],
            supports_streaming=True,
            read_timeout=300,
            write_timeout=300,
            connect_timeout=30,
        )


# ── Worker ────────────────────────────────────────────────────────────────────

async def worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Persistent worker that pulls jobs from the queue."""
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0 (compatible; VideoDumpBot/1.0)"},
        connector=aiohttp.TCPConnector(limit=10, force_close=True),
    ) as session:
        while True:
            job = await url_queue.get()
            job_id = job["id"]
            url = job["url"]
            notify_msg: Optional[Message] = job.get("notify_msg")

            async with semaphore:
                active_jobs[job_id] = {
                    "url": url,
                    "status": "running",
                    "progress": "⏳ Starting…",
                    "start": time.time(),
                }

                tmp_dir = Path(tempfile.mkdtemp(dir=TEMP_DIR))
                file_path = None

                try:
                    # Download
                    active_jobs[job_id]["progress"] = "⬇️ Connecting…"
                    size, file_path = await stream_download(
                        url, tmp_dir, job_id, session
                    )

                    # Upload
                    caption = (
                        f"🎬 {Path(file_path).name}\n"
                        f"📦 {human_size(size)}\n"
                        f"🔗 {url}"
                    )
                    await upload_to_channel(file_path, caption, context, job_id)

                    elapsed = time.time() - active_jobs[job_id]["start"]
                    active_jobs[job_id]["status"] = "done"
                    active_jobs[job_id]["progress"] = (
                        f"✅ Done in {elapsed:.0f}s ({human_size(size)})"
                    )
                    logger.info("Job %s done: %s", job_id, url)

                    if notify_msg:
                        try:
                            await notify_msg.reply_text(
                                f"✅ Uploaded: `{Path(file_path).name}`",
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError:
                            pass

                except asyncio.CancelledError:
                    active_jobs[job_id]["status"] = "cancelled"
                    raise

                except Exception as exc:
                    active_jobs[job_id]["status"] = "error"
                    active_jobs[job_id]["progress"] = f"❌ {exc}"
                    logger.error("Job %s failed: %s — %s", job_id, url, exc)

                    if notify_msg:
                        try:
                            await notify_msg.reply_text(
                                f"❌ Failed `{url[:80]}`\n`{exc}`",
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError:
                            pass

                finally:
                    # Always clean up temp files
                    try:
                        if file_path and Path(file_path).exists():
                            Path(file_path).unlink()
                        tmp_dir.rmdir()
                    except Exception:
                        pass

            url_queue.task_done()


# ── Command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 *Video Dump Bot*\n\n"
        "Send me a list of direct video URLs (one per line) "
        "and I'll download & upload them to the dump channel.\n\n"
        "Commands:\n"
        "/status — show queue and active jobs\n"
        "/clear  — clear completed/errored jobs from status\n",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        return

    pending = url_queue.qsize()
    lines = [f"📊 *Queue:* {pending} pending  |  *Workers:* {MAX_WORKERS}\n"]

    for jid, info in list(active_jobs.items())[-20:]:   # last 20
        icon = {"running": "🔄", "done": "✅", "error": "❌", "cancelled": "⛔"}.get(
            info["status"], "❓"
        )
        short_url = info["url"][:50] + ("…" if len(info["url"]) > 50 else "")
        lines.append(f"{icon} `{short_url}`\n   {info['progress']}")

    await update.message.reply_text(
        "\n".join(lines) or "Nothing running.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        return
    removed = 0
    for jid in list(active_jobs):
        if active_jobs[jid]["status"] in ("done", "error", "cancelled"):
            del active_jobs[jid]
            removed += 1
    await update.message.reply_text(f"🧹 Cleared {removed} finished jobs.")


async def handle_urls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle any text message containing URLs."""
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("⛔ You are not authorized.")
        return

    text = update.message.text or ""

    # Also handle .txt file uploads
    if update.message.document:
        doc = update.message.document
        if doc.mime_type == "text/plain":
            file = await context.bot.get_file(doc.file_id)
            raw = await file.download_as_bytearray()
            text = raw.decode("utf-8", errors="ignore")

    urls = extract_urls(text)
    if not urls:
        await update.message.reply_text("❌ No valid URLs found.")
        return

    queued = 0
    for url in urls:
        job_id = uuid.uuid4().hex[:12]
        await url_queue.put({
            "id": job_id,
            "url": url,
            "notify_msg": update.message,
        })
        queued += 1

    await update.message.reply_text(
        f"✅ Queued *{queued}* URL(s).\n"
        f"📥 Queue size: {url_queue.qsize()}\n"
        f"Use /status to track progress.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Startup / Shutdown ────────────────────────────────────────────────────────

async def on_startup(application: Application) -> None:
    global semaphore
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

    for i in range(MAX_WORKERS):
        task = asyncio.create_task(
            worker(application),
            name=f"worker-{i}",
        )
        worker_tasks.append(task)
        logger.info("Started worker-%d", i)

    logger.info(
        "Bot ready. Workers=%d  MaxFileMB=%d  DumpChannel=%s",
        MAX_WORKERS, MAX_FILE_SIZE_MB, DUMP_CHANNEL_ID,
    )


async def on_shutdown(application: Application) -> None:
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    logger.info("All workers stopped.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(
        MessageHandler(
            (filters.TEXT | filters.Document.MimeType("text/plain"))
            & ~filters.COMMAND,
            handle_urls,
        )
    )

    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
