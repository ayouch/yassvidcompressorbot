#!/usr/bin/env python3
"""Telegram bot that compresses videos with FFmpeg and sends them back."""

import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("video-compressor-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]

# Local Bot API server endpoints (raises the 20MB/50MB limits to 2GB).
# Falls back to the public API if not configured.
API_BASE = os.environ.get("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/")
LOCAL_MODE = os.environ.get("LOCAL_MODE", "true").lower() in ("1", "true", "yes")

# Optional allowlist of Telegram user IDs (comma-separated). Empty = open to all.
ALLOWED_USER_IDS = {
    int(x) for x in os.environ.get("ALLOWED_USER_IDS", "").replace(" ", "").split(",") if x
}

# Limit simultaneous FFmpeg jobs so the VPS does not get overwhelmed.
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "1"))
_job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)


def human_size(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def is_authorized(update: Update) -> bool:
    if not ALLOWED_USER_IDS:
        return True
    user = update.effective_user
    return bool(user and user.id in ALLOWED_USER_IDS)


async def run_ffmpeg(input_path: Path, output_path: Path) -> None:
    """Run the compression command. Raises RuntimeError on failure."""
    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-c:v", "libx264",
        "-crf", "23",
        "-preset", "slow",
        "-c:a", "aac",
        "-b:a", "128k",
        str(output_path),
    ]
    logger.info("Running: %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        tail = stderr.decode(errors="replace")[-1500:]
        raise RuntimeError(f"FFmpeg exited with code {proc.returncode}:\n{tail}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Send me a video and I'll compress it with FFmpeg "
        "(H.264, CRF 23, slow preset) and send it back.\n\n"
        "Tip: send large videos as a *file/document* to avoid Telegram "
        "shrinking them before I even see them.",
        parse_mode="Markdown",
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        await update.message.reply_text("Sorry, you're not authorized to use this bot.")
        return

    message = update.message
    # Accept both native videos and documents whose mime type is video/*.
    media = message.video or message.document
    if media is None:
        return

    mime = getattr(media, "mime_type", None) or ""
    if message.document and not mime.startswith("video/"):
        await message.reply_text("That doesn't look like a video file.")
        return

    file_name = getattr(media, "file_name", None) or f"video_{media.file_unique_id}.mp4"
    stem = Path(file_name).stem

    status = await message.reply_text("Downloading...")
    workdir = Path(tempfile.mkdtemp(prefix="vidc_"))
    input_path = workdir / file_name
    output_path = workdir / f"{stem}_compressed.mp4"

    try:
        tg_file = await context.bot.get_file(media.file_id)
        await tg_file.download_to_drive(custom_path=str(input_path))
        original_size = input_path.stat().st_size

        await status.edit_text(
            f"Downloaded ({human_size(original_size)}). Compressing... "
            "this can take a while for large videos."
        )

        async with _job_semaphore:
            await context.bot.send_chat_action(message.chat_id, ChatAction.UPLOAD_VIDEO)
            await run_ffmpeg(input_path, output_path)

        compressed_size = output_path.stat().st_size
        saved = original_size - compressed_size
        pct = (saved / original_size * 100) if original_size else 0.0

        await status.edit_text(
            f"Done. {human_size(original_size)} -> {human_size(compressed_size)} "
            f"({pct:+.1f}% size change). Uploading..."
        )

        with output_path.open("rb") as fh:
            await message.reply_document(
                document=fh,
                filename=output_path.name,
                caption=(
                    f"Compressed: {human_size(original_size)} -> "
                    f"{human_size(compressed_size)} ({pct:+.1f}%)"
                ),
            )
        await status.delete()

    except Exception as exc:  # noqa: BLE001 - report any failure back to the user
        logger.exception("Failed to process video")
        try:
            await status.edit_text(f"Failed to process video.\n{exc}")
        except Exception:  # noqa: BLE001
            await message.reply_text(f"Failed to process video.\n{exc}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def build_application() -> Application:
    builder = ApplicationBuilder().token(BOT_TOKEN)
    if API_BASE != "https://api.telegram.org":
        builder = builder.base_url(f"{API_BASE}/bot").base_file_url(f"{API_BASE}/file/bot")
        if LOCAL_MODE:
            builder = builder.local_mode(True)
    # Large uploads/downloads need generous timeouts.
    builder = builder.read_timeout(600).write_timeout(600).connect_timeout(60)
    return builder.build()


def main() -> None:
    app = build_application()
    app.add_handler(CommandHandler(["start", "help"], start))
    app.add_handler(
        MessageHandler(filters.VIDEO | filters.Document.VIDEO, handle_video)
    )
    logger.info("Bot starting (api_base=%s, local_mode=%s)...", API_BASE, LOCAL_MODE)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
