#!/usr/bin/env python3
"""Telegram bot that compresses videos with FFmpeg and sends them back."""

import asyncio
from dataclasses import dataclass
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import Bot, Update
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

# How many videos may be compressed at the same time. Extra videos wait in a
# FIFO queue and start in the order they were received.
MAX_CONCURRENT_JOBS = max(1, int(os.environ.get("MAX_CONCURRENT_JOBS", "1")))

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

    async def jobs_ahead(self, job_id: int) -> int:
        """How many jobs will be compressed before this one, including the active job."""
        async with self._lock:
            if job_id in self._active:
                return 0
            try:
                waiting_index = list(self._waiting).index(job_id)
            except ValueError:
                return 0
            return len(self._active) + waiting_index


@dataclass
class QueuedVideo:
    bot: Bot
    chat_id: int
    original_message_id: int
    status_message_id: int
    file_id: str
    file_name: str
    media_duration: float | None
    job_id: int = 0
    workdir: str = ""
    input_path: str = ""
    output_path: str = ""
    original_size: int = 0
    duration_seconds: float | None = None
    last_status_text: str = ""


class VideoQueue:
    """Accept videos immediately and compress them in arrival order.

    One download worker saves files in the order they were received, including
    while a compression is running. Compression workers then pick up the saved
    files in that same order. With the default of one compression worker, the
    next video starts as soon as the current one finishes.
    """

    def __init__(self, max_concurrent_jobs: int) -> None:
        if max_concurrent_jobs < 1:
            raise ValueError("max_concurrent_jobs must be at least 1")
        self.max_concurrent_jobs = max_concurrent_jobs
        self.tracker = JobTracker(max_concurrent_jobs)
        self._incoming: asyncio.Queue[tuple[int, Any]] = asyncio.Queue()
        self._ready: asyncio.Queue[tuple[int, Any]] = asyncio.Queue()
        self._state_lock = asyncio.Lock()
        self._reserved: set[int] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._download: Callable[[Any], Awaitable[None]] | None = None
        self._compress: Callable[[Any], Awaitable[None]] | None = None
        self._on_downloaded: Callable[[int, Any], Awaitable[None]] | None = None
        self._on_download_error: Callable[[Any, BaseException], Awaitable[None]] | None = None
        self._cleanup: Callable[[Any], Awaitable[None]] | None = None

    def start(
        self,
        download: Callable[[Any], Awaitable[None]],
        compress: Callable[[Any], Awaitable[None]],
        *,
        on_downloaded: Callable[[int, Any], Awaitable[None]] | None = None,
        on_download_error: Callable[[Any, BaseException], Awaitable[None]] | None = None,
        cleanup: Callable[[Any], Awaitable[None]] | None = None,
    ) -> None:
        if self._tasks:
            raise RuntimeError("Video queue is already running")
        self._download = download
        self._compress = compress
        self._on_downloaded = on_downloaded
        self._on_download_error = on_download_error
        self._cleanup = cleanup
        self._tasks = [
            asyncio.create_task(self._download_loop(), name="video-download-queue"),
            *[
                asyncio.create_task(self._compress_loop(), name=f"video-compress-queue-{index}")
                for index in range(self.max_concurrent_jobs)
            ],
        ]
        logger.info("Video queue started with %s compression worker(s)", self.max_concurrent_jobs)

    async def reserve(self, name: str) -> tuple[int, int]:
        """Remember a video and return its id plus how many jobs are ahead of it.

        The job is not given to a worker until `activate`, so the caller can
        tell the user their place in line first.
        """
        async with self._state_lock:
            job_id = await self.tracker.enqueue(name)
            self._reserved.add(job_id)
            jobs_ahead = await self.tracker.jobs_ahead(job_id)
            return job_id, jobs_ahead

    async def activate(self, job_id: int, payload: Any) -> None:
        async with self._state_lock:
            if job_id not in self._reserved:
                raise RuntimeError(f"Job {job_id} is not reserved")
            self._reserved.remove(job_id)
            # Unbounded queue, so this does not wait. Keeping it inside the lock
            # preserves arrival order when two videos are accepted close together.
            self._incoming.put_nowait((job_id, payload))

    async def discard(self, job_id: int) -> None:
        """Drop a reservation that never became a queued job."""
        async with self._state_lock:
            if job_id not in self._reserved:
                return
            self._reserved.remove(job_id)
        await self.tracker.finish(job_id)

    async def submit(self, name: str, payload: Any) -> int:
        """Reserve and activate a job. Returns how many jobs are ahead of it."""
        job_id, jobs_ahead = await self.reserve(name)
        await self.activate(job_id, payload)
        return jobs_ahead

    async def wait_for_downloads(self) -> None:
        await self._incoming.join()

    async def wait_for_compression(self) -> None:
        await self._ready.join()

    async def stop(self) -> None:
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        await self._drain_unfinished(self._ready)
        await self._drain_unfinished(self._incoming)

    async def _drain_unfinished(self, queue: asyncio.Queue[tuple[int, Any]]) -> None:
        while not queue.empty():
            job_id, payload = queue.get_nowait()
            try:
                await self.tracker.finish(job_id)
                await self._run_cleanup(payload)
            finally:
                queue.task_done()

    async def _run_cleanup(self, payload: Any) -> None:
        if self._cleanup is None:
            return
        try:
            await self._cleanup(payload)
        except Exception:
            logger.exception("Failed to clean up a queued video")

    async def _download_loop(self) -> None:
        assert self._download is not None
        while True:
            job_id, payload = await self._incoming.get()
            handed_off = False
            try:
                try:
                    await self._download(payload)
                except Exception as exc:
                    try:
                        if self._on_download_error is not None:
                            await self._on_download_error(payload, exc)
                    except Exception:
                        logger.exception("Failed to report a download error")
                else:
                    if self._on_downloaded is not None:
                        try:
                            await self._on_downloaded(job_id, payload)
                        except Exception:
                            logger.exception("Failed to update queue status for job %s", job_id)
                    await self._ready.put((job_id, payload))
                    handed_off = True
            finally:
                if not handed_off:
                    await self.tracker.finish(job_id)
                    await self._run_cleanup(payload)
                self._incoming.task_done()

    async def _compress_loop(self) -> None:
        assert self._compress is not None
        while True:
            job_id, payload = await self._ready.get()
            try:
                await self.tracker.start(job_id)
                try:
                    await self._compress(payload)
                except Exception:
                    logger.exception("Compression job failed")
                finally:
                    await self.tracker.finish(job_id)
                    await self._run_cleanup(payload)
            finally:
                self._ready.task_done()


VIDEO_QUEUE = VideoQueue(MAX_CONCURRENT_JOBS)


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


def videos_ahead_text(count: int) -> str:
    noun = "video" if count == 1 else "videos"
    return f"{count} {noun} ahead of you"


def format_acceptance_text(jobs_ahead: int) -> str:
    if jobs_ahead <= 0:
        return "Downloading..."
    return (
        f"Queued. {videos_ahead_text(jobs_ahead)}.\n"
        "Downloading now. Compression starts automatically when the earlier videos finish."
    )


def format_waiting_text(jobs_ahead: int) -> str:
    return (
        f"Downloaded. {videos_ahead_text(jobs_ahead)}.\n"
        "Compression starts automatically when the earlier videos finish."
    )


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


async def run_ffmpeg(
    cmd: list[str],
    *,
    duration_seconds: float | None = None,
    on_progress: JOB_PROGRESS_CALLBACK | None = None,
) -> None:
    """Run the compression command. Raises RuntimeError on failure."""
    progress_cmd = [cmd[0], "-progress", "pipe:1", "-nostats", *cmd[1:]]
    logger.info("Running: %s", " ".join(cmd))
    proc: asyncio.subprocess.Process | None = None
    try:
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
    finally:
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Send me a video and I'll compress it with the default profile and send it back.\n\n"
        "If I'm already compressing, new videos wait in line and start automatically, in the order you sent them.\n\n"
        "Default profile: H.264, CRF 23, slow preset.\n"
        "Use /status or /queue to check what is running.\n"
        "Tip: send large videos as a file/document to avoid Telegram shrinking them before I even see them."
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Videos are compressed automatically in the order they arrive. A job that is already queued can't be cancelled."
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        await update.message.reply_text("Sorry, you're not authorized to use this bot.")
        return

    snapshot = await VIDEO_QUEUE.tracker.snapshot()
    await update.message.reply_text(format_status_text(snapshot))


async def queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        await update.message.reply_text("Sorry, you're not authorized to use this bot.")
        return

    snapshot = await VIDEO_QUEUE.tracker.snapshot()
    await update.message.reply_text(format_queue_text(snapshot))


async def edit_job_status(job: QueuedVideo, text: str) -> None:
    if text == job.last_status_text:
        return
    await job.bot.edit_message_text(
        chat_id=job.chat_id,
        message_id=job.status_message_id,
        text=text,
    )
    job.last_status_text = text


async def cleanup_queued_video(job: QueuedVideo) -> None:
    if not job.workdir:
        return
    shutil.rmtree(job.workdir, ignore_errors=True)
    job.workdir = ""


async def download_queued_video(job: QueuedVideo) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="vidc_"))
    job.workdir = str(workdir)
    safe_name = Path(job.file_name).name or "video.mp4"
    input_path = workdir / safe_name
    output_path = workdir / f"{Path(safe_name).stem}_compressed.mp4"
    job.input_path = str(input_path)
    job.output_path = str(output_path)

    tg_file = await job.bot.get_file(job.file_id)
    await tg_file.download_to_drive(custom_path=str(input_path))
    job.original_size = input_path.stat().st_size
    job.duration_seconds = job.media_duration or await probe_duration_seconds(input_path)


async def announce_downloaded_video(job_id: int, job: QueuedVideo) -> None:
    jobs_ahead = await VIDEO_QUEUE.tracker.jobs_ahead(job_id)
    if jobs_ahead <= 0:
        return
    await edit_job_status(job, format_waiting_text(jobs_ahead))


async def report_download_error(job: QueuedVideo, exc: BaseException) -> None:
    logger.exception("Failed to download video", exc_info=exc)
    try:
        await edit_job_status(job, f"Failed to process video.\n{exc}")
    except Exception:
        logger.exception("Failed to report a download error")


async def compress_queued_video(job: QueuedVideo) -> None:
    output_path = Path(job.output_path)
    original_size = job.original_size
    mode_label = "default"

    async def on_progress(progress: CompressionProgress) -> None:
        await edit_job_status(job, format_progress_text(mode_label, progress))

    try:
        await edit_job_status(
            job,
            f"Compressing with the {mode_label} FFmpeg command...\nProgress: starting...",
        )
        await job.bot.send_chat_action(job.chat_id, ChatAction.UPLOAD_VIDEO)
        await run_ffmpeg(
            build_default_ffmpeg_command(Path(job.input_path), output_path),
            duration_seconds=job.duration_seconds,
            on_progress=on_progress,
        )

        compressed_size = output_path.stat().st_size
        saved = original_size - compressed_size
        pct = (saved / original_size * 100) if original_size else 0.0

        await edit_job_status(
            job,
            f"Done. {human_size(original_size)} -> {human_size(compressed_size)} "
            f"({pct:+.1f}% size change). Uploading...",
        )

        with output_path.open("rb") as fh:
            await job.bot.send_document(
                chat_id=job.chat_id,
                document=fh,
                filename=output_path.name,
                reply_to_message_id=job.original_message_id,
                caption=(
                    f"Compressed with {mode_label} command: {human_size(original_size)} -> "
                    f"{human_size(compressed_size)} ({pct:+.1f}%)"
                ),
            )

        try:
            await job.bot.delete_message(chat_id=job.chat_id, message_id=job.status_message_id)
        except Exception:
            logger.info("Could not delete the status message for %s", job.file_name, exc_info=True)
    except Exception as exc:  # noqa: BLE001 - report any failure back to the user
        logger.exception("Failed to process video")
        try:
            await edit_job_status(job, f"Failed to process video.\n{exc}")
        except Exception:  # noqa: BLE001
            await job.bot.send_message(chat_id=job.chat_id, text=f"Failed to process video.\n{exc}")


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
    job = QueuedVideo(
        bot=context.bot,
        chat_id=message.chat_id,
        original_message_id=message.message_id,
        status_message_id=0,
        file_id=media.file_id,
        file_name=file_name,
        media_duration=getattr(media, "duration", None),
    )

    job_id: int | None = None
    activated = False
    status_message = None
    try:
        job_id, jobs_ahead = await VIDEO_QUEUE.reserve(file_name)
        job.job_id = job_id
        acceptance_text = format_acceptance_text(jobs_ahead)
        status_message = await message.reply_text(acceptance_text)
        job.status_message_id = status_message.message_id
        job.last_status_text = acceptance_text
        await VIDEO_QUEUE.activate(job_id, job)
        activated = True
        logger.info("Queued %s with %s video(s) ahead", file_name, jobs_ahead)
    except Exception as exc:  # noqa: BLE001 - report any failure back to the user
        logger.exception("Failed to queue video")
        if status_message is not None:
            await status_message.edit_text(f"Failed to queue video.\n{exc}")
        else:
            await message.reply_text(f"Failed to queue video.\n{exc}")
    finally:
        if job_id is not None and not activated:
            await VIDEO_QUEUE.discard(job_id)


def build_application() -> Application:
    builder = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(False)
        .post_init(start_video_queue)
        .post_shutdown(stop_video_queue)
    )
    if API_BASE != "https://api.telegram.org":
        builder = builder.base_url(f"{API_BASE}/bot").base_file_url(f"{API_BASE}/file/bot")
        if LOCAL_MODE:
            builder = builder.local_mode(True)
    # Large uploads/downloads need generous timeouts.
    builder = builder.read_timeout(600).write_timeout(600).connect_timeout(60)
    return builder.build()


async def start_video_queue(application: Application) -> None:
    del application
    VIDEO_QUEUE.start(
        download_queued_video,
        compress_queued_video,
        on_downloaded=announce_downloaded_video,
        on_download_error=report_download_error,
        cleanup=cleanup_queued_video,
    )


async def stop_video_queue(application: Application) -> None:
    del application
    await VIDEO_QUEUE.stop()


def main() -> None:
    app = build_application()
    app.add_handler(CommandHandler(["start", "help"], start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("queue", queue))
    app.add_handler(
        MessageHandler(filters.VIDEO | filters.Document.VIDEO, handle_video)
    )
    logger.info("Bot starting (api_base=%s, local_mode=%s)...", API_BASE, LOCAL_MODE)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
