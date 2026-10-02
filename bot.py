#!/usr/bin/env python3
"""Telegram bot that compresses videos with FFmpeg and sends them back."""

import asyncio
from dataclasses import dataclass
import logging
import os
import shlex
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
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

PENDING_JOB_KEY = "pending_video_job"
AWAITING_CUSTOM_COMMAND_KEY = "awaiting_custom_ffmpeg_command"
DEFAULT_COMMAND_CALLBACK = "ffmpeg:default"
CUSTOM_COMMAND_CALLBACK = "ffmpeg:custom"
PROGRESS_UPDATE_INTERVAL_SECONDS = 5.0


@dataclass(frozen=True)
class QueueSnapshot:
    max_concurrent_jobs: int
    active_count: int
    waiting_count: int
    active_jobs: tuple[str, ...]
    waiting_jobs: tuple[str, ...]


@dataclass(frozen=True)
class CompressionProgress:
    percent: float | None
    elapsed_seconds: float
    eta_seconds: float | None
    speed_text: str | None


class JobTracker:
    def __init__(self, max_concurrent_jobs: int) -> None:
        self.max_concurrent_jobs = max_concurrent_jobs
        self._lock = asyncio.Lock()
        self._next_job_id = 0
        self._waiting: dict[int, str] = {}
        self._active: dict[int, str] = {}

    async def enqueue(self, job_name: str) -> int:
        async with self._lock:
            self._next_job_id += 1
            job_id = self._next_job_id
            self._waiting[job_id] = job_name
            return job_id

    async def start(self, job_id: int) -> None:
        async with self._lock:
            job_name = self._waiting.pop(job_id, None)
            if job_name is None:
                return
            self._active[job_id] = job_name

    async def finish(self, job_id: int) -> None:
        async with self._lock:
            self._waiting.pop(job_id, None)
            self._active.pop(job_id, None)

    async def snapshot(self) -> QueueSnapshot:
        async with self._lock:
            active_jobs = tuple(self._active.values())
            waiting_jobs = tuple(self._waiting.values())
            return QueueSnapshot(
                max_concurrent_jobs=self.max_concurrent_jobs,
                active_count=len(active_jobs),
                waiting_count=len(waiting_jobs),
                active_jobs=active_jobs,
                waiting_jobs=waiting_jobs,
            )


JOB_TRACKER = JobTracker(MAX_CONCURRENT_JOBS)


def parse_ffmpeg_speed(speed_text: str | None) -> float | None:
    if not speed_text:
        return None
    cleaned = speed_text.strip()
    if not cleaned or cleaned == "N/A":
        return None
    if cleaned.endswith("x"):
        cleaned = cleaned[:-1]
    try:
        speed = float(cleaned)
    except ValueError:
        return None
    return speed if speed > 0 else None


def should_emit_progress_update(
    last_update_at: float | None,
    current_time: float,
    interval_seconds: float = PROGRESS_UPDATE_INTERVAL_SECONDS,
) -> bool:
    if last_update_at is None:
        return True
    return (current_time - last_update_at) >= interval_seconds


def format_clock(total_seconds: float) -> str:
    whole_seconds = max(0, int(round(total_seconds)))
    minutes, seconds = divmod(whole_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def build_progress_snapshot(
    progress_fields: dict[str, str],
    duration_seconds: float | None,
    elapsed_seconds: float,
) -> CompressionProgress | None:
    raw_out_time = progress_fields.get("out_time_us") or progress_fields.get("out_time_ms")
    if raw_out_time is None:
        return None

    try:
        processed_seconds = max(0.0, int(raw_out_time) / 1_000_000)
    except ValueError:
        return None

    percent = None
    if duration_seconds and duration_seconds > 0:
        percent = min(100.0, max(0.0, processed_seconds / duration_seconds * 100))

    speed_text = progress_fields.get("speed", "").strip() or None
    speed_value = parse_ffmpeg_speed(speed_text)

    eta_seconds = None
    if duration_seconds and duration_seconds > 0 and processed_seconds < duration_seconds:
        remaining_seconds = max(0.0, duration_seconds - processed_seconds)
        if speed_value:
            eta_seconds = remaining_seconds / speed_value
        elif percent and percent > 0:
            eta_seconds = elapsed_seconds * (100.0 - percent) / percent

    return CompressionProgress(
        percent=percent,
        elapsed_seconds=elapsed_seconds,
        eta_seconds=eta_seconds,
        speed_text=speed_text,
    )


def format_progress_text(mode_label: str, progress: CompressionProgress) -> str:
    lines = [f"Compressing with the {mode_label} FFmpeg command..."]
    if progress.percent is not None:
        lines.append(f"Progress: {progress.percent:.1f}%")
    else:
        lines.append("Progress: working...")
    lines.append(f"Elapsed: {format_clock(progress.elapsed_seconds)}")
    if progress.eta_seconds is not None:
        lines.append(f"ETA: ~{format_clock(progress.eta_seconds)}")
    if progress.speed_text:
        lines.append(f"Speed: {progress.speed_text}")
    return "\n".join(lines)


async def probe_duration_seconds(input_path: Path) -> float | None:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=nokey=1:noprint_wrappers=1",
        str(input_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return None
    try:
        duration = float(stdout.decode().strip())
    except ValueError:
        return None
    return duration if duration > 0 else None


JOB_PROGRESS_CALLBACK = Callable[[CompressionProgress], Awaitable[None]]


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


def build_default_ffmpeg_command(input_path: Path, output_path: Path) -> list[str]:
    return [
        "ffmpeg",
        "-y",
        "-i",
        str(input_path),
        "-c:v",
        "libx264",
        "-crf",
        "23",
        "-preset",
        "slow",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        str(output_path),
    ]


def build_custom_ffmpeg_command(command_text: str, input_path: Path, output_path: Path) -> list[str]:
    command_text = command_text.strip()
    if not command_text:
        raise ValueError("Custom command cannot be empty.")
    if "{input}" not in command_text or "{output}" not in command_text:
        raise ValueError(
            "Custom command must include both {input} and {output} placeholders."
        )

    raw_tokens = shlex.split(command_text)
    if not raw_tokens:
        raise ValueError("Custom command cannot be empty.")
    if raw_tokens[0] != "ffmpeg":
        raise ValueError("Custom command must start with ffmpeg.")

    cmd: list[str] = []
    for token in raw_tokens:
        if token == "{input}":
            cmd.append(str(input_path))
        elif token == "{output}":
            cmd.append(str(output_path))
        else:
            cmd.append(token)
    return cmd


def command_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Use default compression", callback_data=DEFAULT_COMMAND_CALLBACK)],
            [InlineKeyboardButton("Paste custom FFmpeg command", callback_data=CUSTOM_COMMAND_CALLBACK)],
        ]
    )


def format_status_text(snapshot: QueueSnapshot) -> str:
    lines = [
        f"Status: {'busy' if snapshot.active_count or snapshot.waiting_count else 'idle'}",
        f"Active workers: {snapshot.active_count}/{snapshot.max_concurrent_jobs}",
        f"Waiting jobs: {snapshot.waiting_count}",
    ]
    if len(snapshot.active_jobs) == 1:
        lines.append(f"Current job: {snapshot.active_jobs[0]}")
    elif snapshot.active_jobs:
        lines.append("Current jobs:")
        lines.extend(f"- {job}" for job in snapshot.active_jobs)
    return "\n".join(lines)


def format_queue_text(snapshot: QueueSnapshot) -> str:
    lines = [
        "Queue overview",
        f"Active now ({snapshot.active_count}/{snapshot.max_concurrent_jobs}):",
    ]
    if snapshot.active_jobs:
        lines.extend(f"- {job}" for job in snapshot.active_jobs)
    else:
        lines.append("- none")

    lines.append(f"Waiting ({snapshot.waiting_count}):")
    if snapshot.waiting_jobs:
        lines.extend(f"- {job}" for job in snapshot.waiting_jobs)
    else:
        lines.append("- none")

    return "\n".join(lines)


def get_pending_job(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any] | None:
    pending = context.user_data.get(PENDING_JOB_KEY)
    return pending if isinstance(pending, dict) else None


def clear_pending_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    pending = get_pending_job(context)
    if pending:
        shutil.rmtree(pending.get("workdir", ""), ignore_errors=True)
    context.user_data.pop(PENDING_JOB_KEY, None)
    context.user_data.pop(AWAITING_CUSTOM_COMMAND_KEY, None)


async def run_ffmpeg(
    cmd: list[str],
    *,
    duration_seconds: float | None = None,
    on_progress: JOB_PROGRESS_CALLBACK | None = None,
) -> None:
    """Run the compression command. Raises RuntimeError on failure."""
    progress_cmd = [cmd[0], "-progress", "pipe:1", "-nostats", *cmd[1:]]
    logger.info("Running: %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *progress_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    start_time = time.monotonic()
    last_progress_update_at: float | None = None
    progress_fields: dict[str, str] = {}

    assert proc.stdout is not None
    async for raw_line in proc.stdout:
        line = raw_line.decode(errors="replace").strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        progress_fields[key] = value
        if key != "progress":
            continue

        snapshot = build_progress_snapshot(
            progress_fields,
            duration_seconds=duration_seconds,
            elapsed_seconds=time.monotonic() - start_time,
        )
        if (
            on_progress
            and snapshot is not None
            and value != "end"
            and should_emit_progress_update(last_progress_update_at, time.monotonic())
        ):
            await on_progress(snapshot)
            last_progress_update_at = time.monotonic()
        progress_fields = {}

    assert proc.stderr is not None
    stderr = await proc.stderr.read()
    return_code = await proc.wait()
    if return_code != 0:
        tail = stderr.decode(errors="replace")[-1500:]
        raise RuntimeError(f"FFmpeg exited with code {return_code}:\n{tail}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Send me a video and I'll download it, then let you choose either the saved default FFmpeg command or a custom FFmpeg command that you paste.\n\n"
        "Default profile: H.264, CRF 23, slow preset.\n"
        "Use /status or /queue to check what is running.\n"
        "Tip: send large videos as a file/document to avoid Telegram shrinking them before I even see them."
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    had_pending = bool(get_pending_job(context)) or bool(context.user_data.get(AWAITING_CUSTOM_COMMAND_KEY))
    clear_pending_job(context)
    if had_pending:
        await update.message.reply_text("Cancelled the pending video job.")
    else:
        await update.message.reply_text("There is no pending video job to cancel.")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        await update.message.reply_text("Sorry, you're not authorized to use this bot.")
        return

    snapshot = await JOB_TRACKER.snapshot()
    await update.message.reply_text(format_status_text(snapshot))


async def queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        await update.message.reply_text("Sorry, you're not authorized to use this bot.")
        return

    snapshot = await JOB_TRACKER.snapshot()
    await update.message.reply_text(format_queue_text(snapshot))


async def process_pending_job(
    context: ContextTypes.DEFAULT_TYPE,
    pending: dict[str, Any],
    cmd: list[str],
    mode_label: str,
) -> None:
    input_path = Path(pending["input_path"])
    output_path = Path(pending["output_path"])
    original_size = pending["original_size"]
    duration_seconds = pending.get("duration_seconds")
    chat_id = pending["chat_id"]
    status_message_id = pending["status_message_id"]
    original_message_id = pending["original_message_id"]
    display_name = pending["display_name"]
    job_id = await JOB_TRACKER.enqueue(display_name)
    last_status_text: str | None = None

    async def update_status_message(text: str) -> None:
        nonlocal last_status_text
        if text == last_status_text:
            return
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_message_id,
            text=text,
        )
        last_status_text = text

    async def on_progress(progress: CompressionProgress) -> None:
        await update_status_message(format_progress_text(mode_label, progress))

    try:
        snapshot = await JOB_TRACKER.snapshot()
        ahead_of_you = snapshot.active_count + max(0, snapshot.waiting_count - 1)
        if ahead_of_you:
            await update_status_message(
                f"Queued for compression with the {mode_label} FFmpeg command. "
                f"Jobs ahead of you: {ahead_of_you}."
            )

        async with _job_semaphore:
            await JOB_TRACKER.start(job_id)
            await update_status_message(
                f"Compressing with the {mode_label} FFmpeg command...\nProgress: starting..."
            )
            await context.bot.send_chat_action(chat_id, ChatAction.UPLOAD_VIDEO)
            await run_ffmpeg(
                cmd,
                duration_seconds=duration_seconds,
                on_progress=on_progress,
            )

        compressed_size = output_path.stat().st_size
        saved = original_size - compressed_size
        pct = (saved / original_size * 100) if original_size else 0.0

        await update_status_message(
            f"Done. {human_size(original_size)} -> {human_size(compressed_size)} "
            f"({pct:+.1f}% size change). Uploading..."
        )

        with output_path.open("rb") as fh:
            await context.bot.send_document(
                chat_id=chat_id,
                document=fh,
                filename=output_path.name,
                reply_to_message_id=original_message_id,
                caption=(
                    f"Compressed with {mode_label} command: {human_size(original_size)} -> "
                    f"{human_size(compressed_size)} ({pct:+.1f}%)"
                ),
            )

        await context.bot.delete_message(chat_id=chat_id, message_id=status_message_id)

    except Exception as exc:  # noqa: BLE001 - report any failure back to the user
        logger.exception("Failed to process video")
        try:
            await update_status_message(f"Failed to process video.\n{exc}")
        except Exception:  # noqa: BLE001
            await context.bot.send_message(chat_id=chat_id, text=f"Failed to process video.\n{exc}")
    finally:
        await JOB_TRACKER.finish(job_id)
        clear_pending_job(context)


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        await update.message.reply_text("Sorry, you're not authorized to use this bot.")
        return

    clear_pending_job(context)

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

    status_message = await message.reply_text("Downloading...")
    workdir = Path(tempfile.mkdtemp(prefix="vidc_"))
    input_path = workdir / file_name
    output_path = workdir / f"{stem}_compressed.mp4"

    try:
        tg_file = await context.bot.get_file(media.file_id)
        await tg_file.download_to_drive(custom_path=str(input_path))
        original_size = input_path.stat().st_size
        duration_seconds = getattr(media, "duration", None) or await probe_duration_seconds(input_path)

        context.user_data[PENDING_JOB_KEY] = {
            "chat_id": message.chat_id,
            "original_message_id": message.message_id,
            "status_message_id": status_message.message_id,
            "workdir": str(workdir),
            "input_path": str(input_path),
            "output_path": str(output_path),
            "original_size": original_size,
            "display_name": file_name,
            "duration_seconds": duration_seconds,
        }
        context.user_data[AWAITING_CUSTOM_COMMAND_KEY] = False

        await status_message.edit_text(
            f"Downloaded ({human_size(original_size)}). Choose how to compress it.",
            reply_markup=command_choice_keyboard(),
        )

    except Exception as exc:  # noqa: BLE001 - report any failure back to the user
        shutil.rmtree(workdir, ignore_errors=True)
        logger.exception("Failed to prepare video")
        await status_message.edit_text(f"Failed to process video.\n{exc}")


async def handle_command_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    pending = get_pending_job(context)
    if not pending:
        await query.edit_message_text("That video is no longer pending. Please send it again.")
        context.user_data.pop(AWAITING_CUSTOM_COMMAND_KEY, None)
        return

    input_path = Path(pending["input_path"])
    output_path = Path(pending["output_path"])

    if query.data == DEFAULT_COMMAND_CALLBACK:
        context.user_data[AWAITING_CUSTOM_COMMAND_KEY] = False
        cmd = build_default_ffmpeg_command(input_path, output_path)
        await process_pending_job(context, pending, cmd, "default")
        return

    if query.data == CUSTOM_COMMAND_CALLBACK:
        context.user_data[AWAITING_CUSTOM_COMMAND_KEY] = True
        await query.edit_message_text(
            "Paste your custom FFmpeg command now.\n\n"
            "Rules:\n"
            "- It must start with ffmpeg\n"
            "- It must include both {input} and {output}\n"
            "- You may quote the placeholders, but you don't have to\n\n"
            "Example:\n"
            "ffmpeg -y -i \"{input}\" -vf scale=1280:-2 -c:v libx264 -crf 28 -preset medium -c:a aac -b:a 96k \"{output}\""
        )


async def handle_custom_command_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get(AWAITING_CUSTOM_COMMAND_KEY):
        return

    pending = get_pending_job(context)
    if not pending:
        context.user_data.pop(AWAITING_CUSTOM_COMMAND_KEY, None)
        await update.message.reply_text("That video is no longer pending. Please send it again.")
        return

    try:
        cmd = build_custom_ffmpeg_command(
            update.message.text,
            Path(pending["input_path"]),
            Path(pending["output_path"]),
        )
    except ValueError as exc:
        await update.message.reply_text(
            f"{exc}\n\n"
            "Example:\n"
            "ffmpeg -y -i \"{input}\" -vf scale=1280:-2 -c:v libx264 -crf 28 -preset medium -c:a aac -b:a 96k \"{output}\""
        )
        return

    context.user_data[AWAITING_CUSTOM_COMMAND_KEY] = False
    await process_pending_job(context, pending, cmd, "custom")


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
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("queue", queue))
    app.add_handler(CallbackQueryHandler(handle_command_choice, pattern=r"^ffmpeg:"))
    app.add_handler(
        MessageHandler(filters.VIDEO | filters.Document.VIDEO, handle_video)
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_command_text))
    logger.info("Bot starting (api_base=%s, local_mode=%s)...", API_BASE, LOCAL_MODE)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
