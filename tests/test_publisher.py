import contextlib
import copy
import io
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import dashboard
import live_clipper
import publisher


class PublisherTest(unittest.TestCase):
    def test_write_srt(self):
        words = [
            (0.0, 1.2, "hello"),
            (1.3, 2.0, "world"),
            (3.0, 3.5, "again"),
            (3.6, 4.0, "done"),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            srt_path = Path(temp_dir) / "captions.srt"
            self.assertTrue(live_clipper._write_srt(words, srt_path))
            text = srt_path.read_text(encoding="utf-8")
            self.assertIn("00:00:00,000 --> 00:00:02,000", text)
            self.assertIn("hello world", text)
            self.assertIn("00:00:03,000 --> 00:00:04,000", text)
            self.assertIn("again done", text)
            self.assertFalse(live_clipper._write_srt([], srt_path))

    def test_build_metadata_from_sidecar(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            clips_dir = Path(temp_dir) / "clips"
            (clips_dir / "metadata").mkdir(parents=True)
            (clips_dir / "captions").mkdir()
            video = clips_dir / "kaicenat_20260101_010101.mp4"
            video.write_bytes(b"video")
            words = [
                [0.0, 1.0, "hello"],
                [1.0, 2.0, "world"],
                [2.0, 3.0, "funny"],
                [3.0, 4.0, "moment"],
            ]
            sidecar = {
                "slug": "kaicenat",
                "streamer": "kaicenat",
                "platform": "youtube",
                "source_url": "https://www.youtube.com/watch?v=abc",
                "duration": 37.4,
                "caption_status": "ready",
                "words": words,
            }
            (clips_dir / "metadata" / f"{video.stem}.json").write_text(
                json.dumps(sidecar), encoding="utf-8"
            )
            (clips_dir / "captions" / f"{video.stem}.srt").write_text(
                "1\n00:00:00,000 --> 00:00:04,000\nhello world funny moment\n",
                encoding="utf-8",
            )
            config = copy.deepcopy(publisher.DEFAULT_CONFIG_DATA)
            config["streamers"] = {
                "kaicenat": {
                    "display_name": "Kai Cenat",
                    "hashtags": ["#KaiCenat"],
                }
            }
            info = publisher.build_metadata(video, clips_dir, config)
            self.assertEqual(info["streamer"], "Kai Cenat")
            self.assertEqual(info["platform"], "youtube")
            self.assertEqual(info["title"], "Kai Cenat Goes CRAZY Over GTA 6 😂🔥 #shorts")
            self.assertIn("INSANE", info["description"])
            self.assertIn("#KaiCenat", info["description"])
            self.assertIn("funny", info["tags"])
            self.assertTrue(info["srt_path"].endswith(".srt"))

    def test_scan_and_dry_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            clips_dir = Path(temp_dir) / "clips"
            clips_dir.mkdir()
            (clips_dir / "kaicenat_20260101_010101.mp4").write_bytes(b"video")
            config = copy.deepcopy(publisher.DEFAULT_CONFIG_DATA)
            config["clips_dir"] = str(clips_dir)
            connection = publisher.open_db(Path(temp_dir) / "state.sqlite3")
            log = lambda message: None
            self.assertEqual(
                publisher.scan_clips(connection, config, log, stable_seconds=0),
                1,
            )
            self.assertEqual(
                publisher.scan_clips(connection, config, log, stable_seconds=0),
                0,
            )
            rows = connection.execute("SELECT * FROM clips").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "pending_review")
            publisher.approve_clips(connection, [rows[0]["id"]])
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                publisher.publish_once(connection, config, log, dry_run=True)
            output = buffer.getvalue()
            self.assertIn('"platform": "youtube"', output)
            self.assertIn('"privacy": "unlisted"', output)
            row = connection.execute("SELECT status FROM clips WHERE id = 1").fetchone()
            self.assertEqual(row["status"], "approved")
            uploads = connection.execute("SELECT COUNT(*) AS count FROM uploads").fetchone()
            self.assertEqual(uploads["count"], 0)
            connection.close()

    def test_auto_approve_only_selected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            clips_dir = Path(temp_dir) / "clips"
            clips_dir.mkdir()
            (clips_dir / "kaicenat_20260101_010101.mp4").write_bytes(b"one")
            (clips_dir / "kaicenat_20260101_010102.mp4").write_bytes(b"two")
            config = copy.deepcopy(publisher.DEFAULT_CONFIG_DATA)
            config["clips_dir"] = str(clips_dir)
            connection = publisher.open_db(Path(temp_dir) / "state.sqlite3")
            log = lambda message: None
            count, ids = publisher.scan_clips(
                connection, config, log, stable_seconds=0, return_ids=True
            )
            self.assertEqual(count, 2)
            original = publisher.upload_youtube
            publisher.upload_youtube = lambda clip, settings, logger: {
                "remote_id": "video" + str(clip["id"]),
                "url": "https://www.youtube.com/shorts/video" + str(clip["id"]),
            }
            try:
                publisher.publish_once(
                    connection, config, log, auto_approve=True, auto_ids=[ids[0]]
                )
            finally:
                publisher.upload_youtube = original
            first = connection.execute(
                "SELECT status FROM clips WHERE id = ?", (ids[0],)
            ).fetchone()
            second = connection.execute(
                "SELECT status FROM clips WHERE id = ?", (ids[1],)
            ).fetchone()
            self.assertEqual(first["status"], "uploaded")
            self.assertEqual(second["status"], "pending_review")
            connection.close()

    def test_hook_caption(self):
        words = [
            (0.0, 1.0, "You"),
            (1.0, 2.0, "can't"),
            (2.0, 3.0, "steal"),
            (3.0, 4.0, "cars"),
            (4.0, 5.0, "in"),
            (5.0, 6.0, "GTA"),
            (6.0, 7.0, "6."),
        ]
        caption = live_clipper.generate_hook_caption(
            words, "IShowSpeed", [("viewer", "GTA 6 cars")], focus_time=4.0
        )
        self.assertIn("IShowSpeed", caption)
        self.assertIn("GTA 6", caption)

    def test_failed_upload_is_scheduled_for_retry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            clips_dir = Path(temp_dir) / "clips"
            clips_dir.mkdir()
            (clips_dir / "kaicenat_20260101_010101.mp4").write_bytes(b"video")
            config = copy.deepcopy(publisher.DEFAULT_CONFIG_DATA)
            config["clips_dir"] = str(clips_dir)
            connection = publisher.open_db(Path(temp_dir) / "state.sqlite3")
            log = lambda message: None
            count, ids = publisher.scan_clips(
                connection, config, log, stable_seconds=0, return_ids=True
            )
            self.assertEqual(count, 1)
            publisher.approve_clips(connection, ids)
            original = publisher.upload_youtube
            publisher.upload_youtube = lambda clip, settings, logger: (
                (_ for _ in ()).throw(RuntimeError("temporary failure"))
            )
            try:
                publisher.publish_once(connection, config, log)
            finally:
                publisher.upload_youtube = original
            row = connection.execute("SELECT * FROM clips WHERE id = ?", (ids[0],)).fetchone()
            upload = connection.execute(
                "SELECT * FROM uploads WHERE clip_id = ?", (ids[0],)
            ).fetchone()
            self.assertEqual(row["status"], "failed")
            self.assertEqual(upload["attempts"], 1)
            self.assertTrue(upload["next_retry_at"])
            connection.execute(
                "UPDATE uploads SET next_retry_at = ? WHERE clip_id = ?",
                ("2000-01-01T00:00:00+00:00", ids[0]),
            )
            publisher.upload_youtube = lambda clip, settings, logger: {
                "remote_id": "retry",
                "url": "https://www.youtube.com/shorts/retry",
            }
            try:
                publisher.publish_once(connection, config, log)
            finally:
                publisher.upload_youtube = original
            row = connection.execute("SELECT status FROM clips WHERE id = ?", (ids[0],)).fetchone()
            self.assertEqual(row["status"], "uploaded")
            connection.close()

    def test_clipper_command(self):
        command = dashboard.clipper_command(
            {
                "url": "https://www.youtube.com/watch?v=abc",
                "mult": "1.5",
                "pre": "20",
                "no_captions": "on",
            }
        )
        self.assertTrue(Path(command[1]).name == "live_clipper.py")
        self.assertIn("--mult", command)
        self.assertIn("1.5", command)
        self.assertIn("--pre", command)
        self.assertIn("20", command)
        self.assertIn("--no-captions", command)

    def test_make_clip_writes_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            buffer_dir = Path(temp_dir) / "buffer"
            out_dir = Path(temp_dir) / "clips"
            buffer_dir.mkdir()
            out_dir.mkdir()
            for name in ("seg_000.ts", "seg_001.ts"):
                subprocess.run(
                    [
                        "ffmpeg", "-y", "-loglevel", "error",
                        "-f", "lavfi", "-i", "color=c=black:s=320x180:d=1",
                        "-c:v", "libx264", "-t", "1", str(buffer_dir / name),
                    ],
                    check=True,
                    capture_output=True,
                )
            now = time.time()
            for name in ("seg_000.ts", "seg_001.ts"):
                path = buffer_dir / name
                os.utime(path, (now - 9, now - 9))
            metadata = live_clipper.make_clip(
                buffer_dir,
                out_dir,
                "tester",
                now - 10,
                now - 5,
                captions=False,
                source_url="https://example.test/live",
                platform="youtube",
            )
            self.assertIsNotNone(metadata)
            self.assertEqual(metadata["platform"], "youtube")
            self.assertEqual(metadata["caption_status"], "disabled")
            sidecar_path = out_dir / "metadata" / f"{Path(metadata['path']).stem}.json"
            self.assertTrue(sidecar_path.exists())
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            self.assertEqual(sidecar["source_url"], "https://example.test/live")


if __name__ == "__main__":
    unittest.main()
