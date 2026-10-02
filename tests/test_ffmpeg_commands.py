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

    def test_build_custom_ffmpeg_command_substitutes_input_and_output(self):
        cmd = bot.build_custom_ffmpeg_command(
            'ffmpeg -y -i "{input}" -vf scale=1280:-2 -c:v libx264 -crf 28 "{output}"',
            Path("/tmp/input file.mov"),
            Path("/tmp/output file.mp4"),
        )

        self.assertEqual(
            cmd,
            [
                "ffmpeg",
                "-y",
                "-i",
                "/tmp/input file.mov",
                "-vf",
                "scale=1280:-2",
                "-c:v",
                "libx264",
                "-crf",
                "28",
                "/tmp/output file.mp4",
            ],
        )

    def test_build_custom_ffmpeg_command_supports_unquoted_placeholders_with_spaces(self):
        cmd = bot.build_custom_ffmpeg_command(
            "ffmpeg -y -i {input} -c:v libx264 -crf 28 {output}",
            Path("/tmp/Guys and girls.mov"),
            Path("/tmp/output file.mp4"),
        )

        self.assertEqual(
            cmd,
            [
                "ffmpeg",
                "-y",
                "-i",
                "/tmp/Guys and girls.mov",
                "-c:v",
                "libx264",
                "-crf",
                "28",
                "/tmp/output file.mp4",
            ],
        )

    def test_build_custom_ffmpeg_command_requires_placeholders(self):
        with self.assertRaisesRegex(ValueError, r"\{input\}.*\{output\}"):
            bot.build_custom_ffmpeg_command(
                "ffmpeg -i input.mp4 -c:v libx264 output.mp4",
                Path("/tmp/input.mov"),
                Path("/tmp/output.mp4"),
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


if __name__ == "__main__":
    unittest.main()
