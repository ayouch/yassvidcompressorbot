import asyncio
import importlib
import os
import unittest
from pathlib import Path


os.environ.setdefault("BOT_TOKEN", "123:test")

bot = importlib.import_module("bot")


class FfmpegCommandTests(unittest.TestCase):
    def test_build_default_ffmpeg_command_uses_saved_profile(self):
        cmd = bot.build_default_ffmpeg_command(Path("/tmp/input.mov"), Path("/tmp/output.mp4"))

        self.assertEqual(
            cmd,
            [
                "ffmpeg",
                "-y",
                "-i",
                "/tmp/input.mov",
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
                "/tmp/output.mp4",
            ],
        )

class QueueReportingTests(unittest.IsolatedAsyncioTestCase):
    async def test_job_tracker_reports_waiting_and_active_jobs(self):
        tracker = bot.JobTracker(max_concurrent_jobs=2)

        first = await tracker.enqueue("first.mp4")
        second = await tracker.enqueue("second.mp4")
        await tracker.start(first)

        snapshot = await tracker.snapshot()

        self.assertEqual(snapshot.max_concurrent_jobs, 2)
        self.assertEqual(snapshot.active_count, 1)
        self.assertEqual(snapshot.waiting_count, 1)
        self.assertEqual(snapshot.active_jobs, ("first.mp4",))
        self.assertEqual(snapshot.waiting_jobs, ("second.mp4",))

        await tracker.finish(first)
        await tracker.finish(second)

    async def test_jobs_ahead_counts_active_job_and_earlier_waiters(self):
        tracker = bot.JobTracker(max_concurrent_jobs=1)
        first = await tracker.enqueue("first.mp4")
        second = await tracker.enqueue("second.mp4")
        third = await tracker.enqueue("third.mp4")
        await tracker.start(first)

        self.assertEqual(await tracker.jobs_ahead(first), 0)
        self.assertEqual(await tracker.jobs_ahead(second), 1)
        self.assertEqual(await tracker.jobs_ahead(third), 2)

    def test_format_status_text_reports_idle_and_busy(self):
        idle = bot.QueueSnapshot(
            max_concurrent_jobs=1,
            active_count=0,
            waiting_count=0,
            active_jobs=(),
            waiting_jobs=(),
        )
        busy = bot.QueueSnapshot(
            max_concurrent_jobs=1,
            active_count=1,
            waiting_count=2,
            active_jobs=("current.mp4",),
            waiting_jobs=("next.mp4", "later.mp4"),
        )

        idle_text = bot.format_status_text(idle)
        busy_text = bot.format_status_text(busy)

        self.assertIn("Status: idle", idle_text)
        self.assertIn("Active workers: 0/1", idle_text)
        self.assertIn("Waiting jobs: 0", idle_text)

        self.assertIn("Status: busy", busy_text)
        self.assertIn("Active workers: 1/1", busy_text)
        self.assertIn("Current job: current.mp4", busy_text)
        self.assertIn("Waiting jobs: 2", busy_text)

    def test_format_queue_text_lists_active_and_waiting_jobs(self):
        snapshot = bot.QueueSnapshot(
            max_concurrent_jobs=2,
            active_count=1,
            waiting_count=2,
            active_jobs=("current.mp4",),
            waiting_jobs=("next.mp4", "later.mp4"),
        )

        text = bot.format_queue_text(snapshot)

        self.assertIn("Queue overview", text)
        self.assertIn("Active now (1/2):", text)
        self.assertIn("- current.mp4", text)
        self.assertIn("Waiting (2):", text)
        self.assertIn("- next.mp4", text)
        self.assertIn("- later.mp4", text)


class ProgressReportingTests(unittest.TestCase):
    def test_build_progress_snapshot_computes_percent_and_eta(self):
        snapshot = bot.build_progress_snapshot(
            {"out_time_us": "25000000", "speed": "2.0x"},
            duration_seconds=100.0,
            elapsed_seconds=15.0,
        )

        self.assertIsNotNone(snapshot)
        self.assertAlmostEqual(snapshot.percent or 0.0, 25.0)
        self.assertAlmostEqual(snapshot.eta_seconds or 0.0, 37.5)
        self.assertEqual(snapshot.speed_text, "2.0x")

    def test_should_emit_progress_update_throttles_to_five_seconds(self):
        self.assertFalse(bot.should_emit_progress_update(10.0, 14.9))
        self.assertTrue(bot.should_emit_progress_update(10.0, 15.0))

    def test_format_progress_text_includes_percent_and_eta(self):
        progress = bot.CompressionProgress(
            percent=42.0,
            elapsed_seconds=30.0,
            eta_seconds=70.0,
            speed_text="1.4x",
        )

        text = bot.format_progress_text("default", progress)

        self.assertIn("Compressing with the default FFmpeg command", text)
        self.assertIn("Progress: 42.0%", text)
        self.assertIn("Elapsed: 00:30", text)
        self.assertIn("ETA: ~01:10", text)
        self.assertIn("Speed: 1.4x", text)


class QueueMessageTests(unittest.TestCase):
    def test_format_acceptance_text_describes_the_line(self):
        self.assertEqual(bot.format_acceptance_text(0), "Downloading...")
        self.assertIn("1 video ahead of you", bot.format_acceptance_text(1))
        self.assertIn("2 videos ahead of you", bot.format_acceptance_text(2))
        self.assertIn("Downloading now", bot.format_acceptance_text(2))

    def test_format_waiting_text_says_compression_will_start(self):
        text = bot.format_waiting_text(2)
        self.assertIn("Downloaded", text)
        self.assertIn("2 videos ahead of you", text)
        self.assertIn("automatically", text)


class VideoQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_videos_sent_during_compression_run_later_in_order(self):
        queue = bot.VideoQueue(max_concurrent_jobs=1)
        events: list[tuple[str, str]] = []
        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def download(payload: str) -> None:
            events.append(("download", payload))

        async def compress(payload: str) -> None:
            events.append(("compress-start", payload))
            if payload == "first":
                first_started.set()
                await release_first.wait()
            events.append(("compress-end", payload))

        queue.start(download, compress)
        try:
            ahead_first = await queue.submit("first.mp4", "first")
            await first_started.wait()
            ahead_second = await queue.submit("second.mp4", "second")
            ahead_third = await queue.submit("third.mp4", "third")
            await queue.wait_for_downloads()

            snapshot = await queue.tracker.snapshot()
            self.assertEqual(ahead_first, 0)
            self.assertEqual(ahead_second, 1)
            self.assertEqual(ahead_third, 2)
            self.assertEqual(snapshot.active_jobs, ("first.mp4",))
            self.assertEqual(snapshot.waiting_jobs, ("second.mp4", "third.mp4"))
            self.assertEqual(
                [item for item in events if item[0] == "compress-start"],
                [("compress-start", "first")],
            )
            self.assertIn(("download", "second"), events)
            self.assertIn(("download", "third"), events)

            release_first.set()
            await queue.wait_for_compression()
        finally:
            await queue.stop()

        self.assertEqual(
            [item for item in events if item[0].startswith("compress")],
            [
                ("compress-start", "first"),
                ("compress-end", "first"),
                ("compress-start", "second"),
                ("compress-end", "second"),
                ("compress-start", "third"),
                ("compress-end", "third"),
            ],
        )

    async def test_failed_download_does_not_block_later_videos(self):
        queue = bot.VideoQueue(max_concurrent_jobs=1)
        compressed: list[str] = []
        errors: list[str] = []
        cleaned: list[str] = []

        async def download(payload: str) -> None:
            if payload == "bad":
                raise RuntimeError("disk full")

        async def compress(payload: str) -> None:
            compressed.append(payload)

        async def on_error(payload: str, exc: BaseException) -> None:
            errors.append(f"{payload}: {exc}")

        async def cleanup(payload: str) -> None:
            cleaned.append(payload)

        queue.start(download, compress, on_download_error=on_error, cleanup=cleanup)
        try:
            await queue.submit("good-1.mp4", "good-1")
            await queue.submit("bad.mp4", "bad")
            await queue.submit("good-2.mp4", "good-2")
            await queue.wait_for_downloads()
            await queue.wait_for_compression()
            snapshot = await queue.tracker.snapshot()
        finally:
            await queue.stop()

        self.assertEqual(compressed, ["good-1", "good-2"])
        self.assertEqual(errors, ["bad: disk full"])
        self.assertCountEqual(cleaned, ["good-1", "bad", "good-2"])
        self.assertEqual(snapshot.active_count, 0)
        self.assertEqual(snapshot.waiting_count, 0)

    async def test_discard_removes_a_reservation_that_was_never_activated(self):
        queue = bot.VideoQueue(max_concurrent_jobs=1)
        job_id, ahead = await queue.reserve("ghost.mp4")

        self.assertEqual(ahead, 0)
        await queue.discard(job_id)
        snapshot = await queue.tracker.snapshot()

        self.assertEqual(snapshot.waiting_count, 0)
        self.assertEqual(snapshot.active_count, 0)


if __name__ == "__main__":
    unittest.main()
