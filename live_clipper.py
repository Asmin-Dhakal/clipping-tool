"""
live_clipper.py - Real-time chat-spike clipper (Twitch + YouTube)

What it does:
  1. Records the live stream into a rolling buffer of short segments (streamlink + ffmpeg).
  2. Reads the channel's live chat (Twitch anonymously, YouTube via yt-dlp).
  3. When chat activity spikes far above its normal level, it waits a few seconds,
     cuts the moment out of the buffer, and saves a vertical 9:16 MP4 in ./clips
  4. Optionally burns in captions of the streamer's voice only. It learns the
     streamer's speaker profile from the first two clips and ignores game dialogue.

Requirements:
  - Python 3.9+
  - ffmpeg and streamlink installed and on PATH
      winget install Gyan.FFmpeg
      winget install Streamlink.Streamlink   (or: pip install streamlink)
  - YouTube support also needs yt-dlp:  pip install yt-dlp
  - Captions need:  pip install demucs faster-whisper
  - Streamer-only voice filtering also needs:  pip install pyannote.audio scikit-learn

Usage:
  python live_clipper.py channelname
  python live_clipper.py https://www.twitch.tv/channelname --min-msgs 10 --mult 2.5
  python live_clipper.py https://www.youtube.com/watch?v=VIDEO_ID
  python live_clipper.py channelname --no-speaker-filter   # caption every voice

The first two clips of a new streamer are used to learn their voice, so they are
saved without captions; captions start with the next clip. The learned profile is
stored as a hidden .speaker_profile_<name>.npz file in the output folder.

Only use this on streamers who allow clipping or run an official clipping campaign.
"""

import argparse
import json
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

SEG_LEN = 4            # seconds per buffer segment
BUFFER_SECONDS = 240   # how much video history to keep on disk
BASELINE_SECONDS = 300 # how far back to measure "normal" chat activity
WARMUP_SECONDS = 60    # collect chat this long before allowing triggers

CAPTION_MODEL = "small.en"  # faster-whisper model for speech recognition
CAPTION_FONT = "DejaVu Sans"
DEMUCS_MODEL = "htdemucs"   # source separation model (vocals vs background)

SPEAKER_MODEL = "pyannote/wespeaker-voxceleb-resnet34-LM"
SPEAKER_THRESHOLD = 0.30    # cosine similarity needed to match the streamer profile
SPEAKER_MIN_SECONDS = 15.0  # profile speech needed before captions are trusted
SPEAKER_WINDOW = 5.0        # seconds per speaker-embedding window
SPEAKER_STEP = 2.5          # seconds between windows
SPEAKER_BOOTSTRAP_CLIPS = 2 # clips used to learn the streamer's voice

DEFAULT_YT_FORMAT = (
    "bv*[height<=1080][vcodec^=avc1][acodec=none]+ba[acodec^=mp4a]/"
    "bv*[height<=1080][acodec=none]+ba/"
    "best[height<=1080][vcodec^=avc1]/best[height<=1080]/best"
)
TOP_PANEL_HEIGHT = 720
BOTTOM_PANEL_HEIGHT = 1200

CLIP_LOCK = threading.Lock()  # serialize heavy clip/caption processing
_PROFILE_LOCK = threading.Lock()
_SPEAKER_INFERENCE = None
_SPEAKER_LOCK = threading.Lock()


# --------------------------------------------------------------------------- chat
class BaseChatMonitor(threading.Thread):
    """Shared chat buffer and spike statistics for Twitch / YouTube."""

    def __init__(self):
        super().__init__(daemon=True)
        self.msgs = deque()
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.running = True

    def _add(self, author, text=""):
        with self.lock:
            self.msgs.append((time.time(), author, str(text or "")))

    def stats(self, window):
        """Return (messages_in_window, unique_chatters_in_window, baseline_per_window)."""
        now = time.time()
        with self.lock:
            while self.msgs and self.msgs[0][0] < now - BASELINE_SECONDS:
                self.msgs.popleft()
            recent = [u for t, u, _ in self.msgs if t >= now - window]
            older = sum(1 for t, _, _ in self.msgs if t < now - window)
        span = min(now - self.started_at, BASELINE_SECONDS) - window
        baseline = (older / span * window) if span > 0 else 0.0
        return len(recent), len(set(recent)), baseline

    def messages(self, window):
        now = time.time()
        with self.lock:
            return [
                (author, text)
                for stamp, author, text in self.msgs
                if stamp >= now - window
            ]

    def stop(self):
        self.running = False


class TwitchChatMonitor(BaseChatMonitor):
    """Connects to Twitch chat anonymously and logs (timestamp, username) per message."""

    def __init__(self, channel):
        super().__init__()
        self.channel = channel

    def run(self):
        while self.running:
            try:
                self._session()
            except Exception as exc:
                print(f"[chat] connection problem ({exc}); retrying in 3s")
                time.sleep(3)

    def _session(self):
        sock = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=30)
        sock.settimeout(120)
        sock.sendall(b"PASS SCHMOOPIIE\r\n")
        sock.sendall(b"NICK justinfan48291\r\n")
        sock.sendall(f"JOIN #{self.channel}\r\n".encode())
        print(f"[chat] connected to #{self.channel}")
        buf = ""
        try:
            while self.running:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    sock.sendall(b"PING :tmi.twitch.tv\r\n")
                    continue
                if not data:
                    raise ConnectionError("server closed connection")
                buf += data.decode("utf-8", errors="ignore")
                while "\r\n" in buf:
                    line, buf = buf.split("\r\n", 1)
                    try:
                        self._handle(line, sock)
                    except Exception:
                        pass
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def _handle(self, line, sock):
        if line.startswith("PING"):
            sock.sendall(b"PONG :tmi.twitch.tv\r\n")
            return
        if " PRIVMSG " in line and line.startswith(":"):
            try:
                user = line[1:line.index("!")]
            except ValueError:
                return
            text = ""
            parts = line.split(" PRIVMSG ", 1)
            if len(parts) == 2 and " :" in parts[1]:
                text = parts[1].split(" :", 1)[1].strip()
            self._add(user, text)


YT_CHAT_RENDERERS = (
    "liveChatTextMessageRenderer",
    "liveChatPaidMessageRenderer",
    "liveChatMembershipItemRenderer",
    "liveChatPaidStickerRenderer",
    "liveChatSponsorshipsGiftPurchaseAnnouncementRenderer",
)


def _yt_author(renderer):
    name = renderer.get("authorName")
    if not isinstance(name, dict):
        return None
    if "simpleText" in name:
        return name["simpleText"]
    text = "".join(part.get("text", "") for part in name.get("runs", []))
    return text or None


def _yt_text(value):
    if isinstance(value, dict):
        if "simpleText" in value:
            return value["simpleText"]
        return "".join(part.get("text", "") for part in value.get("runs", []))
    return ""


def _yt_authors(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in YT_CHAT_RENDERERS:
                author = _yt_author(value)
                if author:
                    yield author
            else:
                yield from _yt_authors(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _yt_authors(item)


def _yt_chat_items(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in YT_CHAT_RENDERERS:
                author = _yt_author(value)
                if author:
                    text = (
                        _yt_text(value.get("content"))
                        or _yt_text(value.get("message"))
                        or _yt_text(value.get("headerText"))
                    )
                    yield author, text
            else:
                yield from _yt_chat_items(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _yt_chat_items(item)


class YouTubeChatMonitor(BaseChatMonitor):
    """Reads YouTube live chat through yt-dlp's live_chat subtitle stream."""

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.work_dir = Path(tempfile.mkdtemp(prefix="ytchat_"))
        self.proc = None
        self._last_offset = -1

    def _chat_file(self):
        files = [
            f for f in self.work_dir.glob("*.live_chat.json*")
            if "-Frag" not in f.name
        ]
        parts = [f for f in files if f.name.endswith(".part")]
        return parts[0] if parts else (files[0] if files else None)

    @staticmethod
    def _same_file(a, b):
        if a is None or b is None:
            return False
        return str(a).replace(".part", "") == str(b).replace(".part", "")

    @staticmethod
    def _max_offset(path, end):
        """Largest video timestamp already in the file (used to ignore backlog)."""
        best = -1
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                fh.seek(max(0, end - 262144))
                for line in fh:
                    if fh.tell() > end:
                        break
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    try:
                        best = max(best, int(data.get("videoOffsetTimeMsec")))
                    except (TypeError, ValueError):
                        pass
        except OSError:
            pass
        return best

    def _handle_line(self, line):
        line = line.strip()
        if not line:
            return
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            return
        offset = None
        try:
            offset = int(data.get("videoOffsetTimeMsec"))
        except (TypeError, ValueError):
            pass
        if offset is not None:
            if offset < self._last_offset:
                return
            self._last_offset = offset
        for author, text in _yt_chat_items(data):
            self._add(author, text)

    def _session(self):
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--skip-download", "--write-subs",
            "--sub-langs", "live_chat", "--sub-format", "json",
            "--no-progress", "--quiet",
            "-o", str(self.work_dir / "%(id)s"), self.url,
        ]
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        print("[chat] connecting to YouTube live chat...")

        chat_file = None
        for _ in range(120):
            if not self.running:
                return
            chat_file = self._chat_file()
            if chat_file:
                break
            if self.proc.poll() is not None:
                raise ConnectionError("yt-dlp exited before chat started")
            time.sleep(0.5)
        if chat_file is None:
            raise ConnectionError("timed out waiting for YouTube chat")

        print("[chat] connected to YouTube live chat")
        time.sleep(5)  # let the initial backlog flush, then ignore it
        try:
            pos = chat_file.stat().st_size
        except OSError:
            pos = 0
        self._last_offset = self._max_offset(chat_file, pos)

        current = chat_file
        buf = ""
        while self.running and self.proc.poll() is None:
            found = self._chat_file()
            if found is not None and not self._same_file(found, current):
                current = found
                pos = 0
            elif found is not None:
                current = found
            try:
                with open(current, "r", encoding="utf-8", errors="ignore") as fh:
                    fh.seek(pos)
                    chunk = fh.read()
                    pos = fh.tell()
            except OSError:
                chunk = ""
            if chunk:
                buf += chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    self._handle_line(line)
            else:
                time.sleep(0.5)
        raise ConnectionError("YouTube chat stream ended")

    def run(self):
        while self.running:
            try:
                self._session()
            except Exception as exc:
                if not self.running:
                    break
                print(f"[chat] YouTube chat problem ({exc}); retrying in 5s")
                time.sleep(5)
            finally:
                if self.proc is not None:
                    try:
                        self.proc.terminate()
                    except Exception:
                        pass
                    self.proc = None

    def stop(self):
        self.running = False
        if self.proc is not None:
            try:
                self.proc.terminate()
            except Exception:
                pass
        shutil.rmtree(self.work_dir, ignore_errors=True)


# ------------------------------------------------------------------------ recorder
def start_recorder(stream_url, quality, buf_dir, start_number=0):
    """streamlink -> ffmpeg segment muxer. Returns (streamlink_proc, ffmpeg_proc)."""
    sl = subprocess.Popen(
        ["streamlink", "--stdout", stream_url, quality],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    ff = subprocess.Popen(
        [
            "ffmpeg", "-loglevel", "error", "-i", "pipe:0",
            "-c", "copy", "-f", "segment",
            "-segment_time", str(SEG_LEN),
            "-segment_format", "mpegts",
            "-reset_timestamps", "1",
            "-segment_start_number", str(start_number),
            str(buf_dir / "seg_%06d.ts"),
        ],
        stdin=sl.stdout,
    )
    sl.stdout.close()
    return sl, ff


def start_recorder_ytdlp(stream_url, buf_dir, start_number=0,
                         video_format=DEFAULT_YT_FORMAT):
    ytdlp = subprocess.Popen(
        [
            sys.executable, "-m", "yt_dlp",
            "--no-warnings", "--no-progress",
            "--merge-output-format", "mp4",
            "-f", video_format,
            "-o", "-", stream_url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    ff = subprocess.Popen(
        [
            "ffmpeg", "-loglevel", "error", "-i", "pipe:0",
            "-c", "copy", "-f", "segment",
            "-segment_time", str(SEG_LEN),
            "-segment_format", "mpegts",
            "-reset_timestamps", "1",
            "-segment_start_number", str(start_number),
            str(buf_dir / "seg_%06d.ts"),
        ],
        stdin=ytdlp.stdout,
    )
    ytdlp.stdout.close()
    return ytdlp, ff


class Recorder(threading.Thread):
    """Keeps streamlink+ffmpeg alive, restarting after drops or while offline."""

    def __init__(self, stream_url, quality, buf_dir, max_retries=0,
                 platform="twitch", youtube_format=DEFAULT_YT_FORMAT):
        super().__init__(daemon=True)
        self.stream_url = stream_url
        self.quality = quality
        self.buf_dir = buf_dir
        self.max_retries = max_retries
        self.platform = platform
        self.youtube_format = youtube_format
        self.stop_event = threading.Event()
        self.procs = (None, None)

    def _next_index(self):
        idx = 0
        for f in self.buf_dir.glob("seg_*.ts"):
            try:
                idx = max(idx, int(f.stem.split("_")[1]) + 1)
            except (ValueError, IndexError):
                pass
        return idx

    def run(self):
        backoff = 3
        failures = 0
        while not self.stop_event.is_set():
            sl = ff = None
            try:
                if self.platform == "youtube":
                    sl, ff = start_recorder_ytdlp(
                        self.stream_url,
                        self.buf_dir,
                        self._next_index(),
                        self.youtube_format,
                    )
                else:
                    sl, ff = start_recorder(
                        self.stream_url, self.quality, self.buf_dir, self._next_index()
                    )
                self.procs = (sl, ff)
                started = time.time()
                while not self.stop_event.is_set() and ff.poll() is None:
                    time.sleep(1)
                if self.stop_event.is_set():
                    break
                if time.time() - started > 60:
                    backoff = 3
                    failures = 0
                else:
                    failures += 1
                    if self.max_retries and failures > self.max_retries:
                        print("[rec] stream unavailable; giving up after repeated retries")
                        return
                print(f"[rec] recording stopped (offline/dropped); retrying in {backoff}s")
            except Exception as exc:
                print(f"[rec] recorder error ({exc}); retrying in {backoff}s")
                failures += 1
                if self.max_retries and failures > self.max_retries:
                    print("[rec] too many errors; giving up")
                    return
            finally:
                for proc in (ff, sl):
                    if proc is not None:
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                self.procs = (None, None)
            self.stop_event.wait(backoff)
            backoff = min(backoff * 2, 30)

    def stop(self):
        self.stop_event.set()
        for proc in self.procs:
            if proc is not None:
                try:
                    proc.terminate()
                except Exception:
                    pass


def cleanup_buffer(buf_dir):
    cutoff = time.time() - BUFFER_SECONDS
    for f in buf_dir.glob("seg_*.ts"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
        except OSError:
            pass


# ----------------------------------------------------------------------- captions
def _ff_escape(path):
    p = str(path)
    for ch in ("\\", ":", "'", ",", "[", "]"):
        p = p.replace(ch, "\\" + ch)
    return p


def _extract_audio(list_file, out_wav):
    subprocess.run(
        [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-vn", "-ac", "2", "-ar", "44100", str(out_wav),
        ],
        check=True,
    )


def _separate_vocals(wav_path, work_dir, model_name):
    """Use Demucs to keep the voice stem and drop game/music background."""
    out_dir = work_dir / "sep"
    result = subprocess.run(
        [
            sys.executable, "-m", "demucs",
            "--two-stems=vocals", "-n", model_name, "-j", "1",
            "-o", str(out_dir), str(wav_path),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        tail = (result.stderr or result.stdout).strip().splitlines()
        raise RuntimeError(tail[-1] if tail else "demucs failed")
    vocals = sorted(out_dir.rglob("vocals.wav"))
    if not vocals:
        raise RuntimeError("demucs produced no vocals track")
    return vocals[0]


_WHISPER_MODELS = {}
_WHISPER_LOCK = threading.Lock()


def _load_vocals16(vocals_path, work_dir):
    """Decode the vocals stem to a 16 kHz mono float32 array (cached per clip)."""
    import numpy as np

    raw = work_dir / "vocals16.raw"
    if not raw.exists():
        subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error", "-i", str(vocals_path),
                "-ac", "1", "-ar", "16000", "-f", "f32le",
                "-acodec", "pcm_f32le", str(raw),
            ],
            check=True,
        )
    return np.fromfile(raw, dtype=np.float32)


def _transcribe_words(vocals_path, work_dir, model_size):
    import numpy as np
    from faster_whisper import WhisperModel

    audio = _load_vocals16(vocals_path, work_dir)
    if audio.size == 0:
        return []

    with _WHISPER_LOCK:
        model = _WHISPER_MODELS.get(model_size)
        if model is None:
            model = WhisperModel(model_size, device="cpu", compute_type="int8")
            _WHISPER_MODELS[model_size] = model

    segments, _ = model.transcribe(
        audio,
        language="en",
        vad_filter=True,
        word_timestamps=True,
        beam_size=1,
        condition_on_previous_text=False,
    )
    words = []
    for seg in segments:
        for word in (seg.words or []):
            text = word.word.strip()
            if text:
                words.append((word.start, word.end, text))
    return words


def _filter_words(words, intervals):
    """Keep only words whose midpoint falls inside a streamer-speech interval."""
    if not intervals:
        return []
    kept = []
    for start, end, text in words:
        mid = (start + end) / 2.0
        for a, b in intervals:
            if a <= mid <= b:
                kept.append((start, end, text))
                break
    return kept


def _speaker_inference():
    global _SPEAKER_INFERENCE
    with _SPEAKER_LOCK:
        if _SPEAKER_INFERENCE is None:
            from pyannote.audio import Inference, Model

            model = Model.from_pretrained(SPEAKER_MODEL)
            _SPEAKER_INFERENCE = Inference(
                model,
                window="sliding",
                duration=SPEAKER_WINDOW,
                step=SPEAKER_STEP,
            )
        return _SPEAKER_INFERENCE


def _load_profile(path):
    import numpy as np

    empty = {"entries": [], "clips_seen": 0}
    if not path.exists():
        return empty
    try:
        data = np.load(path)
        centroids = data["centroids"]
        durations = data["durations"]
        weights = data["weights"] if "weights" in data else durations
        hits = (
            data["hits"]
            if "hits" in data
            else np.ones(len(durations), dtype=np.float32)
        )
        speaker_ids = (
            data["speaker_ids"]
            if "speaker_ids" in data
            else np.full(len(durations), -1, dtype=np.int32)
        )
        clips_seen = int(data["clips_seen"]) if "clips_seen" in data else 0
        entries = []
        for i in range(len(durations)):
            entries.append(
                {
                    "centroid": np.asarray(centroids[i], dtype=np.float32),
                    "duration": float(durations[i]),
                    "weight": float(weights[i]),
                    "hits": float(hits[i]),
                    "speaker_id": int(speaker_ids[i]),
                }
            )
        return {"entries": entries, "clips_seen": clips_seen}
    except Exception:
        return empty


def _save_profile(path, entries, clips_seen):
    import numpy as np

    if not entries:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    centroids = np.stack([e["centroid"] for e in entries]).astype(np.float32)
    durations = np.asarray([e["duration"] for e in entries], dtype=np.float32)
    weights = np.asarray([e["weight"] for e in entries], dtype=np.float32)
    hits = np.asarray([e.get("hits", 1.0) for e in entries], dtype=np.float32)
    speaker_ids = np.asarray([e["speaker_id"] for e in entries], dtype=np.int32)
    tmp = path.with_name(path.stem + "_tmp.npz")
    np.savez_compressed(
        tmp,
        centroids=centroids,
        durations=durations,
        weights=weights,
        hits=hits,
        speaker_ids=speaker_ids,
        clips_seen=np.asarray(clips_seen, dtype=np.int32),
    )
    tmp.replace(path)


def _merge_intervals(intervals):
    if not intervals:
        return []
    intervals = sorted(intervals)
    merged = [list(intervals[0])]
    for start, end in intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def _update_entry(entry, cluster):
    import numpy as np
    from sklearn.preprocessing import normalize

    total = entry["duration"] + cluster["duration"]
    if total > 0:
        blended = (
            entry["centroid"] * entry["duration"]
            + cluster["centroid"] * cluster["duration"]
        ) / total
        entry["centroid"] = normalize(blended.reshape(1, -1))[0]
    entry["duration"] = total
    entry["weight"] += cluster["weight"]
    entry["hits"] = entry.get("hits", 1.0) + 1.0


def _diarize_streamer(vocals_path, work_dir, profile_path, threshold=SPEAKER_THRESHOLD):
    """Return streamer-only speech intervals, or [] when the streamer is silent.

    Returns None if speaker filtering cannot run, so callers can fall back to
    captioning every voice.
    """
    import numpy as np
    import torch
    from faster_whisper.vad import VadOptions, get_speech_timestamps
    from sklearn.cluster import AgglomerativeClustering
    from sklearn.preprocessing import normalize

    audio = _load_vocals16(vocals_path, work_dir)
    if audio.size < 16000 * 2:
        return []

    vad = VadOptions(
        threshold=0.4, min_speech_duration_ms=250, min_silence_duration_ms=300
    )
    mask = np.zeros(len(audio), dtype=np.float32)
    for stamp in get_speech_timestamps(audio, vad_options=vad):
        start = max(0, int(stamp["start"]))
        end = min(len(audio), int(stamp["end"]))
        mask[start:end] = 1.0
    speech = audio[mask > 0]
    if speech.size == 0:
        return []
    speech_rms = float(np.sqrt(np.mean(speech * speech)))
    gain = min(0.05 / max(speech_rms, 1e-8), 20.0)
    normalized = np.clip(audio * gain, -1.0, 1.0)

    inference = _speaker_inference()
    with _SPEAKER_LOCK:
        emb = inference(
            {
                "waveform": torch.from_numpy(normalized).unsqueeze(0),
                "sample_rate": 16000,
            }
        )
    X = normalize(np.asarray(emb.data, dtype=np.float32))
    segs = [emb.sliding_window[i] for i in range(X.shape[0])]

    keep, orig_rms = [], []
    for i, seg in enumerate(segs):
        sl = slice(int(seg.start * 16000), int(seg.end * 16000))
        keep.append(float(mask[sl].mean()) >= 0.5 if len(mask[sl]) else False)
        part = audio[sl]
        orig_rms.append(float(np.sqrt(np.mean(part * part))) if len(part) else 0.0)
    kept = np.where(keep)[0]
    if len(kept) < 2:
        return []

    X = X[kept]
    orig_rms = np.asarray(orig_rms)[kept]
    segs = [segs[i] for i in kept]

    count = len(X)
    k = 2 if count >= 4 else 1
    if k == 1:
        labels = np.zeros(count, dtype=int)
    else:
        labels = AgglomerativeClustering(
            n_clusters=k, metric="cosine", linkage="average"
        ).fit_predict(X)

    clip_duration = len(audio) / 16000.0
    clusters = []
    for label in range(k):
        idx = np.where(labels == label)[0]
        if not len(idx):
            continue
        centroid = normalize(X[idx].mean(axis=0, keepdims=True))[0]
        duration = min(clip_duration, len(idx) * SPEAKER_STEP)
        rms = float(orig_rms[idx].mean())
        clusters.append(
            {
                "indices": idx,
                "centroid": centroid,
                "duration": float(duration),
                "rms": rms,
                "weight": float(duration) * max(rms, 1e-6) ** 2,
                "entry": None,
            }
        )
    if not clusters:
        return []

    with _PROFILE_LOCK:
        profile = _load_profile(profile_path)
        entries = profile["entries"]
        clips_seen = profile["clips_seen"]
        bootstrapped = any(e["speaker_id"] == 0 for e in entries)

        if not bootstrapped:
            for cluster in clusters:
                if entries:
                    sims = [float(cluster["centroid"] @ e["centroid"]) for e in entries]
                    best = int(np.argmax(sims))
                    if sims[best] >= threshold:
                        _update_entry(entries[best], cluster)
                        cluster["entry"] = entries[best]
                        continue
                entry = {
                    "centroid": cluster["centroid"],
                    "duration": cluster["duration"],
                    "weight": cluster["weight"],
                    "hits": 1.0,
                    "speaker_id": -1,
                }
                entries.append(entry)
                cluster["entry"] = entry

            clips_seen += 1
            if clips_seen >= SPEAKER_BOOTSTRAP_CLIPS:
                top = max(entries, key=lambda e: (e.get("hits", 1.0), e["weight"]))
                for entry in entries:
                    entry["speaker_id"] = 0 if entry is top else 1
                bootstrapped = True
            _save_profile(profile_path, entries, clips_seen)
            if not bootstrapped:
                print(
                    f"[clip] learning streamer voice ({clips_seen}/"
                    f"{SPEAKER_BOOTSTRAP_CLIPS} clips); captions start after this"
                )
                return []
        else:
            for cluster in clusters:
                sims = [float(cluster["centroid"] @ e["centroid"]) for e in entries]
                best = int(np.argmax(sims))
                if sims[best] >= threshold:
                    _update_entry(entries[best], cluster)
                    cluster["entry"] = entries[best]
                    if entries[best]["speaker_id"] == 0:
                        variant = {
                            "centroid": cluster["centroid"],
                            "duration": cluster["duration"],
                            "weight": cluster["weight"],
                            "hits": 1.0,
                            "speaker_id": 0,
                        }
                        entries.append(variant)
                        cluster["entry"] = variant
                else:
                    entry = {
                        "centroid": cluster["centroid"],
                        "duration": cluster["duration"],
                        "weight": cluster["weight"],
                        "hits": 1.0,
                        "speaker_id": 1,
                    }
                    entries.append(entry)
                    cluster["entry"] = entry

            if len(entries) > 16:
                streamer = [e for e in entries if e["speaker_id"] == 0]
                others = sorted(
                    (e for e in entries if e["speaker_id"] != 0),
                    key=lambda e: e["weight"],
                    reverse=True,
                )
                entries = streamer[:12] + others[: max(0, 16 - min(len(streamer), 12))]
            _save_profile(profile_path, entries, clips_seen)

    if not bootstrapped:
        return []

    selected = [
        c for c in clusters
        if c["entry"] is not None and c["entry"]["speaker_id"] == 0
    ]
    if not selected:
        return []
    intervals = []
    for cluster in selected:
        for i in cluster["indices"]:
            intervals.append((segs[i].start, segs[i].end))
    return _merge_intervals(intervals)


def _ass_ts(seconds):
    seconds = max(0.0, seconds)
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}:{minutes:02d}:{seconds % 60:05.2f}"


def _ass_text(text):
    return (
        text.replace("\\", "\\\\")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("\n", "\\N")
    )


def _caption_groups(words, max_words=4, max_gap=0.8, max_duration=3.0):
    groups, current = [], []
    for start, end, text in words:
        if current and (
            len(current) >= max_words
            or start - current[-1][1] > max_gap
            or end - current[0][0] > max_duration
        ):
            groups.append(current)
            current = []
        current.append((start, end, text))
    if current:
        groups.append(current)
    return groups


def _write_ass(words, ass_path, font):
    groups = _caption_groups(words)
    if not groups:
        return False

    header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,{font},78,&H00FFFFFF,&H000000FF,&H00000000,&H99000000,-1,0,0,0,100,100,0,0,1,6,3,2,70,70,300,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    lines = []
    for group in groups:
        start = group[0][0]
        end = max(group[-1][1], start + 0.5)
        text = _ass_text(" ".join(w[2] for w in group).upper())
        lines.append(
            f"Dialogue: 0,{_ass_ts(start)},{_ass_ts(end)},Default,,0,0,0,,{text}"
        )
    ass_path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
    return True


def _srt_ts(seconds):
    total_ms = max(0, int(round(seconds * 1000)))
    hours, rem = divmod(total_ms, 3600000)
    minutes, rem = divmod(rem, 60000)
    secs, millis = divmod(rem, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def _atomic_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _write_srt(words, srt_path):
    groups = _caption_groups(words)
    if not groups:
        return False

    lines = []
    for index, group in enumerate(groups, 1):
        start = group[0][0]
        end = max(group[-1][1], start + 0.5)
        text = " ".join(w[2] for w in group).strip()
        lines.append(
            f"{index}\n{_srt_ts(start)} --> {_srt_ts(end)}\n{text}\n"
        )
    _atomic_write(srt_path, "\n".join(lines) + "\n")
    return True


HOOK_STOPWORDS = set(
    "about after again because been before being between could didn't does doing "
    "don't going just like made more much not only other really said say says "
    "should some stuff than that their them then there these they this those "
    "through very want wanted was were what when where which while with would "
    "yeah you your".split()
)

EMOTION_WORDS = set(
    "angry crazy damn dumb epic funny hate love mad amazed shocked upset wild "
    "can't cannot bro wait stop why what no way we're you're".split()
)


def _sentences_from_words(words):
    sentences, current = [], []
    for item in words:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue
        try:
            start, end = float(item[0]), float(item[1])
        except (TypeError, ValueError):
            continue
        text = str(item[2]).strip()
        if not text:
            continue
        if current and start - current[-1][1] > 1.1:
            sentences.append(
                (current[0][0], current[-1][1], " ".join(w[2] for w in current))
            )
            current = []
        current.append((start, end, text))
        if text.endswith((".", "!", "?")):
            sentences.append(
                (current[0][0], current[-1][1], " ".join(w[2] for w in current))
            )
            current = []
    if current:
        sentences.append(
            (current[0][0], current[-1][1], " ".join(w[2] for w in current))
        )
    return sentences


def _chat_keywords(messages, limit=12):
    counts = {}
    for author, text in messages or []:
        for token in re.findall(r"[A-Za-z']+", str(text).lower()):
            if len(token) < 4 or token in HOOK_STOPWORDS:
                continue
            counts[token] = counts.get(token, 0) + 1
    ordered = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    return {word for word, _ in ordered[:limit]}


def generate_hook_caption(words, streamer="", chat_messages=None, focus_time=None):
    sentences = _sentences_from_words(words)
    if not sentences:
        counts = {}
        for _, text in chat_messages or []:
            clean = re.sub(r"\s+", " ", str(text or "")).strip(" ,-")
            if 4 <= len(clean) <= 110 and not clean.startswith("!"):
                counts[clean] = counts.get(clean, 0) + 1
        if not counts:
            return ""
        best_chat = max(counts.items(), key=lambda pair: (pair[1], len(pair[0])))[0]
        lead = "reacted"
        return (
            f"{streamer} {lead}: {best_chat}" if streamer
            else f"{lead}: {best_chat}"
        )
    focus = float(focus_time) if focus_time is not None else (
        sum((start + end) / 2 for start, end, _ in sentences) / len(sentences)
    )
    keywords = _chat_keywords(chat_messages)
    best = None
    best_score = float("-inf")
    for start, end, sentence in sentences:
        text = re.sub(r"\s+", " ", sentence).strip(" ,-")
        if len(text) < 3:
            continue
        lower = text.lower()
        score = -abs((start + end) / 2 - focus) / 8.0
        if "?" in text or "!" in text:
            score += 1.5
        if any(word in lower for word in EMOTION_WORDS):
            score += 2.0
        score += min(sum(1 for word in keywords if word in lower) * 0.8, 4.0)
        if len(text) > 120:
            score -= 1.0
        if score > best_score:
            best_score = score
            best = text
    if not best:
        return ""
    if len(best) > 110:
        best = best[:107].rstrip() + "..."
    lower = best.lower()
    if "?" in best:
        lead = "asked"
    elif any(word in lower for word in EMOTION_WORDS):
        lead = "reacted"
    else:
        lead = "said"
    return f"{streamer} {lead}: {best}" if streamer else f"{lead}: {best}"


def _wrap_text(text, width=38):
    lines, current = [], ""
    for word in str(text).split():
        candidate = f"{current} {word}".strip()
        if current and len(candidate) > width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return "\\N".join(lines)


def _write_hook_ass(caption, ass_path, duration, font=CAPTION_FONT):
    if not caption:
        return False
    header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 2
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Hook,{font},54,&H00000000,&H00000000,&H00FFFFFF,&H00FFFFFF,-1,0,0,0,100,100,0,0,3,4,0,5,80,80,80,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    text = _wrap_text(_ass_text(caption))
    end = max(1.0, float(duration))
    line = (
        f"Dialogue: 0,0:00:00.50,{_ass_ts(end)},Hook,,0,0,0,,"
        f"{{\\pos(540,900)}}{text}"
    )
    _atomic_write(ass_path, header + line + "\n")
    return True


def _video_size(source, concat=False):
    command = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height", "-of", "csv=s=x:p=0",
    ]
    if concat:
        command.extend(["-f", "concat", "-safe", "0"])
    command.extend(["-i", str(source)])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    width, height = result.stdout.strip().split("x")[:2]
    return int(width), int(height)


def _extract_frame(source, offset, out_path, concat=False):
    command = ["ffmpeg", "-y", "-loglevel", "error"]
    if offset > 0:
        command.extend(["-ss", str(max(0.0, offset))])
    if concat:
        command.extend(["-f", "concat", "-safe", "0"])
    command.extend(["-i", str(source), "-frames:v", "1", str(out_path)])
    subprocess.run(command, check=True, capture_output=True)


def _parse_webcam_crop(spec, width, height):
    if not spec:
        return None
    parts = str(spec).split(":")
    if len(parts) != 4:
        return None

    def value(token, size):
        token = token.strip()
        if token.endswith("%"):
            return int(float(token[:-1]) / 100.0 * size)
        return int(float(token))

    x, y, crop_w, crop_h = (
        value(parts[0], width),
        value(parts[1], height),
        value(parts[2], width),
        value(parts[3], height),
    )
    crop_w = max(1, min(width, crop_w))
    crop_h = max(1, min(height, crop_h))
    x = max(0, min(width - crop_w, x))
    y = max(0, min(height - crop_h, y))
    return f"crop={crop_w}:{crop_h}:{x}:{y}"


def _streamer_webcam_crop(slug):
    config_path = Path(__file__).resolve().parent / "publisher.json"
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    return str(data.get("streamers", {}).get(slug, {}).get("webcam_crop", ""))


def _cached_webcam_crop(out_dir, slug, width, height):
    path = Path(out_dir) / f".webcam_crop_{slug}.txt"
    try:
        header, crop = path.read_text(encoding="utf-8").strip().split("|", 1)
    except (OSError, ValueError):
        return ""
    return crop if header == f"{width}x{height}" else ""


def _save_webcam_crop(out_dir, slug, width, height, crop):
    path = Path(out_dir) / f".webcam_crop_{slug}.txt"
    path.write_text(f"{width}x{height}|{crop}\n", encoding="utf-8")


def _default_webcam_crop(width, height):
    crop_w = max(1, int(width * 0.28))
    crop_h = max(1, int(crop_w * 0.75))
    x = 0
    y = max(0, height - crop_h - int(height * 0.05))
    return f"crop={crop_w}:{crop_h}:{x}:{y}"


def _detect_webcam_crop(frame_path, width, height):
    return _detect_face_crop(frame_path, width, height)


def _detect_webcam_crop_multi(list_file, width, height, work_dir,
                              focus_offset, source_duration):
    offsets = [
        0.0,
        source_duration * 0.10,
        source_duration * 0.25,
        focus_offset,
        source_duration * 0.75,
        source_duration * 0.90,
    ]
    limit = max(0.0, source_duration - 0.2) if source_duration else 0.0
    groups = {}

    def crop_key(crop):
        match = re.match(r"crop=(\d+):(\d+):(\d+):(\d+)", crop)
        if not match:
            return crop
        crop_w, crop_h, x, y = (int(value) for value in match.groups())
        return (
            round(x / 80.0),
            round(y / 80.0),
            round(crop_w / 80.0),
            round(crop_h / 80.0),
        )

    for index, offset in enumerate(offsets):
        frame = work_dir / f"webcam_sample_{index}.jpg"
        adjusted = min(offset, limit) if limit > 0 else max(0.0, offset)
        try:
            _extract_frame(list_file, adjusted, frame, concat=True)
            crop = _detect_webcam_crop(frame, width, height)
        except Exception:
            crop = None
        if not crop:
            continue
        key = crop_key(crop)
        if key not in groups:
            groups[key] = [0, crop]
        groups[key][0] += 1
    if not groups:
        return None
    return max(groups.values(), key=lambda item: item[0])[1]


def _detect_face_crop(frame_path, width, height, top_height=TOP_PANEL_HEIGHT):
    import cv2

    image = cv2.imread(str(frame_path))
    if image is None:
        return None
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    cascade_path = Path(cv2.data.haarcascades) / "haarcascade_frontalface_default.xml"
    cascade = cv2.CascadeClassifier(str(cascade_path))
    faces = cascade.detectMultiScale(
        gray, scaleFactor=1.1, minNeighbors=4, minSize=(20, 20)
    )
    if not len(faces):
        return None

    candidates = []
    for x, y, face_w, face_h in faces:
        area_ratio = float(face_w * face_h) / float(width * height)
        near_corner = (
            x < width * 0.30
            or x + face_w > width * 0.70
            or y < height * 0.30
            or y + face_h > height * 0.70
        )
        webcam_like = area_ratio < 0.08 and near_corner
        score = (8.0 if webcam_like else 0.0) * max(
            0.0, 1.0 - min(area_ratio / 0.02, 1.0)
        )
        if near_corner:
            score += 1.0
        if x + face_w / 2 < width * 0.35 and y + face_h / 2 > height * 0.45:
            score += 2.0
        candidates.append(
            (score, webcam_like, x + face_w / 2, y + face_h / 2, face_w, face_h)
        )

    usable = [item for item in candidates if item[1]]
    if not usable:
        return None
    _, _, center_x, center_y, face_w, face_h = max(usable, key=lambda item: item[0])
    ratio = 1080.0 / float(top_height)
    face_center_x = center_x
    face_center_y = center_y
    shift_x = face_w * 0.1 if face_center_x < width / 2 else -face_w * 0.1
    shift_y = face_h * 0.6 if face_center_y > height * 0.40 else -face_h * 0.6
    center_x = face_center_x + shift_x
    center_y = face_center_y + shift_y
    crop_w = min(width * 0.45, face_w * 5.0)
    crop_h = crop_w / ratio
    crop_w = max(2, int(crop_w) - int(crop_w) % 2)
    crop_h = max(2, int(crop_h) - int(crop_h) % 2)
    x = int(max(0, min(width - crop_w, center_x - crop_w / 2)))
    y = int(max(0, min(height - crop_h, center_y - crop_h / 2)))
    return f"crop={crop_w}:{crop_h}:{x}:{y}"


def _probe_duration(path, concat=False):
    try:
        command = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1",
        ]
        if concat:
            command.extend(["-f", "concat", "-safe", "0"])
        command.append(str(path))
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )
        return float(result.stdout.strip())
    except (OSError, ValueError, subprocess.CalledProcessError):
        return None


# -------------------------------------------------------------------------- clips
def make_clip(buf_dir, out_dir, name, start, end, captions=True,
              whisper_model=CAPTION_MODEL, demucs_model=DEMUCS_MODEL,
              caption_font=CAPTION_FONT, speaker_filter=True,
              speaker_threshold=SPEAKER_THRESHOLD, source_url="",
              platform="", chat_messages=None, focus_time=None,
              caption_name="", layout="split", hook_caption_overlay=False,
              webcam_crop=""):
    files = sorted(buf_dir.glob("seg_*.ts"))
    files = files[:-1]  # newest segment may still be written to
    chosen = []
    for f in files:
        try:
            end_t = f.stat().st_mtime
        except OSError:
            continue
        if end_t >= start and (end_t - SEG_LEN) <= end:
            chosen.append(f)
    if not chosen:
        print("[clip] no video found for that moment (buffer too short?)")
        return None

    stamp = time.strftime("%Y%m%d_%H%M%S")
    list_file = out_dir / f".concat_{stamp}.txt"
    list_file.write_text("".join(f"file '{f.as_posix()}'\n" for f in chosen))
    out_file = out_dir / f"{name}_{stamp}.mp4"

    vf = (
        "[0:v]split=2[bg][fg];"
        "[bg]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,boxblur=20:5[bgb];"
        "[fg]scale=1080:-2[fgs];"
        "[bgb][fgs]overlay=(W-w)/2:(H-h)/2,format=yuv420p"
    )

    words = []
    captions_written = False
    speaker_filtered = False
    caption_status = "disabled" if not captions else "not_run"
    hook_caption = ""
    focus_at = focus_time if focus_time is not None else (start + end) / 2
    focus_offset = max(0.0, focus_at - start)
    layout_mode = str(layout or "split").lower()
    using_split = False
    webcam_crop_used = ""
    crop_source = ""

    work_dir = Path(tempfile.mkdtemp(prefix="clipcap_"))
    try:
        if layout_mode == "split":
            try:
                width, height = _video_size(list_file, concat=True)
                source_duration = _probe_duration(list_file, concat=True) or 0
                specified = str(webcam_crop or "").strip()
                top_crop = _parse_webcam_crop(specified, width, height) if specified else None
                if top_crop:
                    crop_source = "manual"
                if not top_crop:
                    top_crop = _detect_webcam_crop_multi(
                        list_file, width, height, work_dir,
                        focus_offset, source_duration,
                    )
                    if top_crop:
                        crop_source = "detected"
                        try:
                            _save_webcam_crop(out_dir, name, width, height, top_crop)
                        except OSError:
                            pass
                if not top_crop:
                    cached = _cached_webcam_crop(out_dir, name, width, height)
                    top_crop = _parse_webcam_crop(cached, width, height) if cached else None
                    if top_crop:
                        crop_source = "cache"
                if not top_crop:
                    configured = _streamer_webcam_crop(name)
                    top_crop = _parse_webcam_crop(configured, width, height) if configured else None
                    if top_crop:
                        crop_source = "config"
                if not top_crop:
                    top_crop = _default_webcam_crop(width, height)
                    crop_source = "default"
                vf = (
                    "[0:v]split=2[top][bot];"
                    f"[top]{top_crop},scale=1080:{TOP_PANEL_HEIGHT}:force_original_aspect_ratio=increase,crop=1080:{TOP_PANEL_HEIGHT},setsar=1[t];"
                    f"[bot]scale=1080:{BOTTOM_PANEL_HEIGHT}:force_original_aspect_ratio=increase,crop=1080:{BOTTOM_PANEL_HEIGHT},setsar=1[b];"
                    "[t][b]vstack=inputs=2,format=yuv420p"
                )
                using_split = True
                webcam_crop_used = top_crop
                print(f"[clip] webcam crop ({crop_source}): {top_crop}")
            except Exception as exc:
                print(f"[clip] split layout unavailable ({exc}); using blurred layout")

        if captions:
            try:
                wav = work_dir / "mixed.wav"
                _extract_audio(list_file, wav)
                vocals = _separate_vocals(wav, work_dir, demucs_model)

                streamer_windows = None
                if speaker_filter:
                    try:
                        streamer_windows = _diarize_streamer(
                            vocals,
                            work_dir,
                            out_dir / f".speaker_profile_{name}.npz",
                            speaker_threshold,
                        )
                    except Exception as exc:
                        print(
                            f"[clip] speaker filter unavailable ({exc}); "
                            "captioning all voices"
                        )
                        streamer_windows = None

                if streamer_windows is not None and not streamer_windows:
                    print(
                        "[clip] no captions for this clip "
                        "(streamer silent or voice profile still learning)"
                    )
                    words = []
                    caption_status = "no_streamer_voice"
                else:
                    words = _transcribe_words(vocals, work_dir, whisper_model)
                    if streamer_windows is not None:
                        words = _filter_words(words, streamer_windows)
                        speaker_filtered = True

                    ass_file = work_dir / "captions.ass"
                    if _write_ass(words, ass_file, caption_font):
                        vf += f",ass={_ff_escape(ass_file)}"
                        captions_written = True
                        caption_status = (
                            "ready" if speaker_filtered else "ready_all_voices"
                        )
                        print(f"[clip] captions ready ({len(words)} words)")
                    else:
                        print("[clip] no speech detected; saving without captions")
                        caption_status = "no_speech"
            except Exception as exc:
                print(f"[clip] captioning skipped ({exc})")
                caption_status = "error"

        hook_caption = generate_hook_caption(
            words, caption_name or name, chat_messages, focus_offset
        )
        if hook_caption_overlay and hook_caption:
            hook_file = work_dir / "hook.ass"
            if _write_hook_ass(
                hook_caption, hook_file, max(1.0, end - start), caption_font
            ):
                vf += f",ass={_ff_escape(hook_file)}"

        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-filter_complex", vf + "[v]", "-map", "[v]", "-map", "0:a?",
            "-c:v", "libx264", "-preset", "medium", "-crf", "18",
            "-pix_fmt", "yuv420p", "-profile:v", "high",
            "-c:a", "aac", "-b:a", "192k",
            "-movflags", "+faststart",
            str(out_file),
        ]
        result = subprocess.run(cmd)
    finally:
        list_file.unlink(missing_ok=True)
        shutil.rmtree(work_dir, ignore_errors=True)

    if result.returncode != 0:
        print("[clip] ffmpeg failed to build the clip")
        return None

    print(f"[clip] saved {out_file}")
    duration = _probe_duration(out_file)
    if duration is None:
        duration = max(0.0, end - start)

    stem = out_file.stem
    srt_file = None
    if captions_written:
        candidate = out_dir / "captions" / f"{stem}.srt"
        try:
            if _write_srt(words, candidate):
                srt_file = candidate
        except OSError as exc:
            print(f"[clip] could not write captions sidecar ({exc})")

    metadata = {
        "clip": out_file.name,
        "filename": out_file.name,
        "path": str(out_file.resolve()),
        "slug": name,
        "streamer": name,
        "platform": platform,
        "source_url": source_url,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "start": start,
        "end": end,
        "duration": round(duration, 3),
        "captions": captions_written,
        "caption_status": caption_status,
        "speaker_filtered": speaker_filtered,
        "word_count": len(words),
        "words": [
            [round(float(word_start), 3), round(float(word_end), 3), str(text)]
            for word_start, word_end, text in words
        ],
        "srt": str(srt_file.resolve()) if srt_file else None,
        "hook_caption": hook_caption,
        "layout": layout_mode if using_split else "blurred",
        "webcam_crop": webcam_crop_used,
        "webcam_crop_source": crop_source,
        "chat_messages": [str(text) for _, text in (chat_messages or [])[:40]],
    }
    try:
        _atomic_write(
            out_dir / "metadata" / f"{stem}.json",
            json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        )
    except OSError as exc:
        print(f"[clip] could not write metadata sidecar ({exc})")
    return metadata


# --------------------------------------------------------------------------- main
def parse_channel(text):
    text = text.strip().lower()
    text = re.sub(r"^https?://(www\.)?twitch\.tv/", "", text)
    return text.strip("/").split("/")[0].split("?")[0]


def youtube_slug(url):
    match = re.search(r"(?:v=|/live/|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})", url)
    if match:
        return "yt_" + match.group(1)
    match = re.search(r"/@([A-Za-z0-9_.-]+)", url)
    if match:
        return "yt_" + re.sub(r"[^A-Za-z0-9_.-]", "_", match.group(1))
    return "youtube"


def resolve_input(text):
    """Return (platform, stream_url, chat_target, filename_slug)."""
    raw = text.strip()
    url = raw if re.match(r"^https?://", raw, re.I) else "https://" + raw
    low = raw.lower()
    if "youtube.com" in low or "youtu.be" in low:
        return "youtube", url, url, youtube_slug(url)
    channel = parse_channel(raw)
    return "twitch", f"https://www.twitch.tv/{channel}", channel, channel


def main():
    p = argparse.ArgumentParser(description="Real-time chat-spike clipper for Twitch and YouTube")
    p.add_argument("channel", help="Twitch channel / URL, or YouTube live URL")
    p.add_argument("--pre", type=int, default=25, help="seconds of video before the spike (default 25)")
    p.add_argument("--post", type=int, default=12, help="seconds of video after the spike (default 12)")
    p.add_argument("--window", type=int, default=10, help="spike measuring window in seconds (default 10)")
    p.add_argument("--mult", type=float, default=3.0, help="spike must be this many times the baseline (default 3)")
    p.add_argument("--min-msgs", type=int, default=15, help="minimum messages in window to count as a spike (default 15)")
    p.add_argument("--min-unique", type=int, default=5, help="minimum unique chatters in window (default 5)")
    p.add_argument("--cooldown", type=int, default=45, help="seconds to wait between clips (default 45)")
    p.add_argument("--quality", default="1080p60,1080p,best", help="streamlink quality for Twitch (default 1080p60,1080p,best)")
    p.add_argument("--youtube-format", default=DEFAULT_YT_FORMAT, help="yt-dlp format selector for YouTube")
    p.add_argument("--out", default="clips", help="output folder (default ./clips)")
    p.add_argument("--retries", type=int, default=0, help="failed recorder restarts before giving up; 0 = retry forever (default 0)")
    p.add_argument("--no-captions", action="store_true", help="disable burned-in voice captions")
    p.add_argument("--no-speaker-filter", action="store_true", help="caption every voice, including game dialogue")
    p.add_argument("--speaker-threshold", type=float, default=SPEAKER_THRESHOLD, help=f"streamer voice match threshold (default {SPEAKER_THRESHOLD})")
    p.add_argument("--reset-speaker-profile", action="store_true", help="forget the learned streamer voice and relearn it")
    p.add_argument("--whisper-model", default=CAPTION_MODEL, help=f"faster-whisper model (default {CAPTION_MODEL})")
    p.add_argument("--demucs-model", default=DEMUCS_MODEL, help=f"Demucs voice-separation model (default {DEMUCS_MODEL})")
    p.add_argument("--caption-font", default=CAPTION_FONT, help=f"caption font name (default {CAPTION_FONT})")
    p.add_argument("--caption-name", default="", help="streamer name used in the title caption")
    p.add_argument("--layout", choices=("split", "blurred"), default="split", help="clip layout (default split)")
    p.add_argument("--webcam-crop", default="", help="manual webcam crop x:y:w:h (pixels or %%)")
    p.add_argument("--hook-caption", action="store_true", help="burn the white hook caption into the video")
    args = p.parse_args()

    platform, stream_url, chat_target, slug = resolve_input(args.channel)

    required = ["ffmpeg"] + (["streamlink"] if platform == "twitch" else [])
    for tool in required:
        if not shutil.which(tool):
            sys.exit(f"'{tool}' was not found on PATH. See the install notes at the top of this file.")

    if platform == "youtube":
        try:
            import yt_dlp  # noqa: F401
        except ImportError:
            sys.exit(
                "YouTube support needs 'yt-dlp'. Install it with: pip install yt-dlp"
            )

    if not args.no_captions:
        try:
            import demucs  # noqa: F401
            import faster_whisper  # noqa: F401
        except ImportError as exc:
            sys.exit(
                f"Captions need 'demucs' and 'faster-whisper' ({exc}). "
                "Install them or run with --no-captions."
            )

    speaker_filter = not args.no_speaker_filter
    if not args.no_captions and speaker_filter:
        try:
            import pyannote.audio  # noqa: F401
            import sklearn  # noqa: F401
        except ImportError as exc:
            print(
                f"[warn] speaker filter unavailable ({exc}); captioning all voices"
            )
            speaker_filter = False

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    buf_dir = Path(tempfile.mkdtemp(prefix="clipbuf_"))

    if args.reset_speaker_profile:
        profile_file = out_dir / f".speaker_profile_{slug}.npz"
        profile_file.unlink(missing_ok=True)
        print("[start] reset learned streamer voice profile")

    print(f"[start] platform: {platform}")
    print(f"[start] stream: {stream_url}")
    print(f"[start] buffer: {buf_dir}")
    print(f"[start] clips will be saved in: {out_dir.resolve()}")

    if platform == "youtube":
        chat = YouTubeChatMonitor(chat_target)
    else:
        chat = TwitchChatMonitor(chat_target)
    chat.start()
    recorder = Recorder(
        stream_url,
        args.quality,
        buf_dir,
        args.retries,
        platform=platform,
        youtube_format=args.youtube_format,
    )
    recorder.start()

    last_clip = 0.0
    last_cleanup = time.time()
    last_status = time.time()

    try:
        while True:
            time.sleep(1)

            if not recorder.is_alive():
                print("[rec] recorder stopped permanently. Exiting.")
                break

            now = time.time()
            if now - last_cleanup > 10:
                cleanup_buffer(buf_dir)
                last_cleanup = now

            count, unique, baseline = chat.stats(args.window)

            if now - last_status > 30:
                print(f"[status] chat: {count} msgs/{args.window}s, normal ~{baseline:.1f}")
                last_status = now

            if now - chat.started_at < WARMUP_SECONDS:
                continue
            if now - last_clip < args.cooldown:
                continue

            threshold = max(args.min_msgs, args.mult * max(baseline, 1.0))
            if count >= threshold and unique >= args.min_unique:
                last_clip = now
                print(f"[spike] {count} msgs from {unique} chatters (normal ~{baseline:.1f}) - clipping in {args.post}s")
                spike_chat = chat.messages(args.window)

                def worker(spike_time=now, spike_chat=spike_chat):
                    time.sleep(args.post)
                    with CLIP_LOCK:
                        make_clip(
                            buf_dir, out_dir, slug,
                            spike_time - args.pre, spike_time + args.post,
                            captions=not args.no_captions,
                            whisper_model=args.whisper_model,
                            demucs_model=args.demucs_model,
                            caption_font=args.caption_font,
                            speaker_filter=speaker_filter,
                            speaker_threshold=args.speaker_threshold,
                            source_url=args.channel,
                            platform=platform,
                            chat_messages=spike_chat,
                            focus_time=spike_time,
                            caption_name=args.caption_name or slug,
                            layout=args.layout,
                            hook_caption_overlay=args.hook_caption,
                            webcam_crop=args.webcam_crop,
                        )

                threading.Thread(target=worker, daemon=True).start()
    except KeyboardInterrupt:
        print("\n[stop] shutting down...")
    finally:
        chat.stop()
        recorder.stop()
        time.sleep(1)
        shutil.rmtree(buf_dir, ignore_errors=True)


if __name__ == "__main__":
    main()