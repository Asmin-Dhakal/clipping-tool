import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import live_clipper

DEFAULT_CONFIG = "publisher.json"
STABLE_SECONDS = 10
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]

DEFAULT_CONFIG_DATA = {
    "clips_dir": "clips",
    "state_db": "clips/.publisher.sqlite3",
    "log_file": "publisher.log",
    "watch_interval": 10,
    "defaults": {
        "platforms": ["youtube"],
        "privacy": "unlisted",
        "category_id": "20",
        "language": "en",
        "made_for_kids": False,
        "game": "GTA 6",
        "hashtags": [
            "#GTA6",
            "#GTA6Clips",
            "#YouTubeShorts",
            "#Gaming",
            "#FunnyMoments",
            "#ViralShorts",
            "#Shorts",
        ],
        "title_template": "{streamer} Goes CRAZY Over {game} 😂🔥 #shorts",
        "description_template": (
            "{streamer}'s reaction to {game} is absolutely INSANE! 😂🔥 "
            "From crazy moments to hilarious reactions, {streamer} never disappoints!\n\n"
            "Get ready for the funniest and most entertaining {streamer} {game} moments.\n\n"
            "Subscribe for more daily {streamer} clips, {game} moments, funny reactions, "
            "and viral gaming highlights!\n\n"
            "🎮 Featuring: {streamer}\n🔥 Game: {game}\n\n{hashtags}"
        ),
        "max_title": 100,
        "max_description": 5000,
        "max_tags": 15,
        "upload_captions": True,
        "max_daily_uploads": 100,
        "retry_backoff_base": 30,
        "retry_backoff_max": 300,
    },
    "streamers": {},
    "youtube": {
        "client_secret": "~/.config/clip-publisher/client_secret.json",
        "token": "~/.config/clip-publisher/token.json",
        "privacy": "unlisted",
        "category_id": "20",
        "upload_captions": True,
    },
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS clips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    sha256 TEXT NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    mtime REAL NOT NULL,
    streamer TEXT NOT NULL,
    platform TEXT NOT NULL,
    source_url TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    tags TEXT NOT NULL,
    srt_path TEXT,
    duration REAL NOT NULL DEFAULT 0,
    caption_status TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending_review',
    discovered_at TEXT NOT NULL,
    approved_at TEXT,
    rejected_at TEXT,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    clip_id INTEGER NOT NULL REFERENCES clips(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    remote_id TEXT,
    url TEXT,
    privacy TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TEXT,
    created_at TEXT NOT NULL,
    uploaded_at TEXT,
    UNIQUE(clip_id, platform)
);
CREATE TABLE IF NOT EXISTS quota (
    day TEXT NOT NULL,
    platform TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, platform)
);
CREATE INDEX IF NOT EXISTS idx_clips_status ON clips(status);
CREATE INDEX IF NOT EXISTS idx_uploads_clip ON uploads(clip_id);
"""

STOPWORDS = set(
    """
    a about above after again against all am an and any are aren't as at be because
    been before being below between both but by can can't cannot could couldn't did
    didn't do does doesn't doing don't down during each few for from further had
    hadn't has hasn't have haven't having he he'd he'll he's her here here's hers
    herself him himself his how how's i i'd i'll i'm i've if in into is isn't it
    it's its itself let's me more most mustn't my myself no nor not of off on once
    only or other ought our ours ourselves out over own same shan't she she'd she'll
    she's should shouldn't so some such than that that's the their theirs them
    themselves then there there's these they they'd they'll they're they've this
    those through to too under until up very was wasn't we we'd we'll we're we've
    were weren't what what's when when's where where's which while who who's whom
    why why's with won't would wouldn't you you'd you'll you're you've your yours
    yourself yourselves just like got get going gonna really know think yeah
    """.split()
)


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _next_retry(config, attempts):
    base = int(config["defaults"].get("retry_backoff_base", 30))
    maximum = int(config["defaults"].get("retry_backoff_max", 300))
    seconds = min(maximum, base * (2 ** max(0, attempts - 1)))
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(
        timespec="seconds"
    )


def _quota_used(connection, platform):
    row = connection.execute(
        "SELECT count FROM quota WHERE day = ? AND platform = ?",
        (_today(), platform),
    ).fetchone()
    return int(row["count"]) if row else 0


def _increment_quota(connection, platform):
    connection.execute(
        """
        INSERT INTO quota (day, platform, count) VALUES (?, ?, 1)
        ON CONFLICT(day, platform) DO UPDATE SET count = count + 1
        """,
        (_today(), platform),
    )


def _deep_merge(base, override):
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _resolve_path(value, base):
    path = Path(os.path.expandvars(str(value))).expanduser()
    return path if path.is_absolute() else base / path


def load_config(path):
    config_path = Path(path).expanduser()
    if config_path.exists():
        base = config_path.resolve().parent
        data = json.loads(config_path.read_text(encoding="utf-8"))
    else:
        base = Path.cwd()
        data = {}
    config = _deep_merge(DEFAULT_CONFIG_DATA, data)
    config["clips_dir"] = str(_resolve_path(config["clips_dir"], base))
    config["state_db"] = str(_resolve_path(config["state_db"], base))
    config["log_file"] = str(_resolve_path(config["log_file"], base))
    youtube = config.setdefault("youtube", {})
    youtube["client_secret"] = str(
        _resolve_path(youtube.get("client_secret", ""), base)
    )
    youtube["token"] = str(_resolve_path(youtube.get("token", ""), base))
    return config


class Logger:
    def __init__(self, path):
        self.path = Path(path)

    def __call__(self, message):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        print(line)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except OSError:
            pass


def open_db(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(SCHEMA)
    columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(uploads)")
    }
    if "next_retry_at" not in columns:
        connection.execute("ALTER TABLE uploads ADD COLUMN next_retry_at TEXT")
    quota_columns = connection.execute("PRAGMA table_info(quota)").fetchall()
    quota_keys = [row["name"] for row in quota_columns if row["pk"]]
    if quota_keys and quota_keys != ["day", "platform"]:
        connection.execute("ALTER TABLE quota RENAME TO quota_old")
        connection.execute(
            """
            CREATE TABLE quota (
                day TEXT NOT NULL,
                platform TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day, platform)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO quota (day, platform, count)
            SELECT day, platform, MAX(count) FROM quota_old GROUP BY day, platform
            """
        )
        connection.execute("DROP TABLE quota_old")
    connection.commit()
    return connection


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clip_prefix(stem):
    match = re.match(r"^(.*)_\d{8}_\d{6}$", stem)
    return match.group(1) if match else stem


def _keywords(words, limit=8):
    counts = {}
    for item in words:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue
        for token in re.findall(r"[A-Za-z']+", str(item[2]).lower()):
            if len(token) < 4 or token in STOPWORDS:
                continue
            counts[token] = counts.get(token, 0) + 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [word for word, _ in ordered[:limit]]


def _hook(words, max_words=10):
    text = " ".join(
        str(item[2])
        for item in words
        if isinstance(item, (list, tuple)) and len(item) >= 3
    )
    text = re.sub(r"\s+", " ", text).strip()
    return " ".join(text.split()[:max_words])


def _fallback_hook(stem):
    match = re.search(r"_(\d{8})_(\d{6})$", stem)
    if match:
        try:
            stamp = datetime.strptime(
                match.group(1) + match.group(2), "%Y%m%d%H%M%S"
            )
            return "clip " + stamp.strftime("· %b %d, %H:%M")
        except ValueError:
            pass
    return "clip"


def _format_template(template, values):
    try:
        return str(template).format(**values)
    except (KeyError, IndexError, ValueError):
        return ""


def _tag_list(values, limit):
    tags, seen = [], set()
    for value in values:
        tag = re.sub(r"[^A-Za-z0-9 _-]", "", str(value)).strip().lstrip("#")
        tag = re.sub(r"\s+", " ", tag)
        if not tag:
            continue
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        tags.append(tag)
        if len(tags) >= limit:
            break
    return tags


def _hashtag_list(values, limit):
    tags, seen = [], set()
    for value in values:
        text = str(value).strip()
        prefix = "#" if text.startswith("#") else ""
        text = prefix + re.sub(r"[^A-Za-z0-9_]", "", text)
        if len(text) <= 1:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        tags.append(text)
        if len(tags) >= limit:
            break
    return tags


def _load_sidecar(clips_dir, path):
    metadata_path = Path(clips_dir) / "metadata" / f"{path.stem}.json"
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def build_metadata(path, clips_dir, config):
    path = Path(path)
    clips_dir = Path(clips_dir)
    defaults = config["defaults"]
    sidecar = _load_sidecar(clips_dir, path)
    slug = sidecar.get("slug") or _clip_prefix(path.stem)
    streamer_config = config.get("streamers", {}).get(slug, {})
    streamer = (
        streamer_config.get("display_name")
        or sidecar.get("streamer")
        or slug
    )
    words = sidecar.get("words") or []
    hook = _hook(words) or _fallback_hook(path.stem)
    source_url = sidecar.get("source_url") or "Original stream"
    game = streamer_config.get("game") or defaults.get("game", "GTA 6")
    streamer_hashtags = _hashtag_list(
        streamer_config.get("hashtags") or [], 3
    )
    streamer_tag = streamer_hashtags[0] if streamer_hashtags else (
        "#" + re.sub(r"[^A-Za-z0-9]", "", streamer)
    )
    hashtags = []
    hashtags.extend(streamer_hashtags)
    hashtags.extend(defaults.get("hashtags") or [])
    hashtags = _hashtag_list(hashtags, 12)
    hashtag_text = " ".join(hashtags)
    values = {
        "streamer": streamer,
        "slug": slug,
        "hook": hook,
        "game": game,
        "streamer_tag": streamer_tag,
        "hashtags": hashtag_text,
        "source_url": source_url,
        "platform": sidecar.get("platform", ""),
    }
    title = _format_template(defaults.get("title_template"), values)
    if not title:
        title = str(sidecar.get("hook_caption") or "")
    if not title:
        title = f"{streamer}: {hook}"
    description = _format_template(
        defaults.get("description_template"), values
    )
    if not description:
        description = f"{hook}\n\n{source_url}\n\n{hashtag_text}"
    max_title = int(defaults.get("max_title", 100))
    max_description = int(defaults.get("max_description", 5000))
    if len(title) > max_title:
        title = title[: max(0, max_title - 1)].rstrip() + "…"
    if len(description) > max_description:
        description = description[: max(0, max_description - 1)].rstrip() + "…"
    tags = _tag_list(
        [streamer, slug, *(_keywords(words)), *hashtags, "shorts"],
        int(defaults.get("max_tags", 15)),
    )
    srt_path = Path(clips_dir) / "captions" / f"{path.stem}.srt"
    if not srt_path.exists():
        sidecar_srt = sidecar.get("srt")
        srt_path = Path(sidecar_srt) if sidecar_srt else None
    if srt_path is not None and not Path(srt_path).exists():
        srt_path = None
    return {
        "streamer": streamer,
        "slug": slug,
        "platform": sidecar.get("platform", ""),
        "source_url": sidecar.get("source_url", ""),
        "title": title,
        "description": description,
        "tags": tags,
        "srt_path": str(Path(srt_path).resolve()) if srt_path else None,
        "duration": float(sidecar.get("duration") or 0),
        "caption_status": sidecar.get("caption_status", ""),
    }


def recaption_clip(clip_path, clips_dir, slug=None, speaker_threshold=0.30,
                   whisper_model="small.en", demucs_model="htdemucs",
                   caption_name=None):
    path = Path(clip_path)
    clips_dir = Path(clips_dir)
    slug = slug or _clip_prefix(path.stem)
    work_dir = Path(tempfile.mkdtemp(prefix="recap_"))
    try:
        wav = work_dir / "mixed.wav"
        subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", str(path), "-vn", "-ac", "2", "-ar", "44100", str(wav),
            ],
            check=True,
            capture_output=True,
        )
        vocals = live_clipper._separate_vocals(wav, work_dir, demucs_model)
        profile_path = clips_dir / f".speaker_profile_{slug}.npz"
        streamer_windows = None
        if profile_path.exists():
            try:
                streamer_windows = live_clipper._diarize_streamer(
                    vocals, work_dir, profile_path, speaker_threshold
                )
            except Exception:
                streamer_windows = None
        if streamer_windows is not None and not streamer_windows:
            words = []
            status = "no_streamer_voice"
        else:
            words = live_clipper._transcribe_words(vocals, work_dir, whisper_model)
            if streamer_windows is not None:
                words = live_clipper._filter_words(words, streamer_windows)
                status = "ready"
            else:
                status = "ready_all_voices"
        srt_path = clips_dir / "captions" / f"{path.stem}.srt"
        written = live_clipper._write_srt(words, srt_path)
        if not written and status == "ready":
            status = "no_speech"
        sidecar = dict(_load_sidecar(clips_dir, path))
        hook_caption = live_clipper.generate_hook_caption(
            words,
            caption_name or slug,
            sidecar.get("chat_messages") or [],
        )
        sidecar.update(
            {
                "clip": path.name,
                "filename": path.name,
                "path": str(path.resolve()),
                "slug": slug,
                "streamer": slug,
                "platform": sidecar.get("platform", ""),
                "source_url": sidecar.get("source_url", ""),
                "duration": live_clipper._probe_duration(path) or 0,
                "captions": written,
                "caption_status": status,
                "speaker_filtered": streamer_windows is not None,
                "word_count": len(words),
                "words": [
                    [round(float(start), 3), round(float(end), 3), str(text)]
                    for start, end, text in words
                ],
                "srt": str(srt_path.resolve()) if written else None,
                "hook_caption": hook_caption,
            }
        )
        live_clipper._atomic_write(
            clips_dir / "metadata" / f"{path.stem}.json",
            json.dumps(sidecar, indent=2, ensure_ascii=False) + "\n",
        )
        return sidecar
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def recaption_clips(connection, config, log, ids=None, all_clips=False):
    clips_dir = Path(config["clips_dir"])
    if ids:
        placeholders = ",".join("?" for _ in ids)
        rows = connection.execute(
            f"SELECT * FROM clips WHERE id IN ({placeholders}) ORDER BY id",
            ids,
        ).fetchall()
    elif all_clips:
        rows = connection.execute("SELECT * FROM clips ORDER BY id").fetchall()
    else:
        rows = connection.execute(
            "SELECT * FROM clips WHERE srt_path IS NULL OR srt_path = '' ORDER BY id"
        ).fetchall()
    if not rows:
        log("no clips need recaptioning")
        return 0
    count = 0
    for row in rows:
        path = Path(row["path"])
        if not path.exists():
            log(f"clip {row['id']} is missing: {path}")
            continue
        try:
            slug = _clip_prefix(path.stem)
            display_name = config.get("streamers", {}).get(slug, {}).get(
                "display_name"
            ) or slug
            meta = recaption_clip(
                path,
                clips_dir,
                slug=slug,
                caption_name=display_name,
            )
            info = build_metadata(path, clips_dir, config)
        except Exception as exc:
            log(f"recaption failed for clip {row['id']}: {exc}")
            continue
        connection.execute(
            """
            UPDATE clips SET streamer = ?, source_url = ?, title = ?,
                description = ?, tags = ?, srt_path = ?, duration = ?,
                caption_status = ?
            WHERE id = ?
            """,
            (
                info["streamer"],
                meta.get("source_url", ""),
                info["title"],
                info["description"],
                json.dumps(info["tags"]),
                info["srt_path"],
                info["duration"],
                info["caption_status"],
                row["id"],
            ),
        )
        connection.commit()
        count += 1
        log(f"recaptioned clip {row['id']}: {info['title']}")
    return count


def scan_clips(connection, config, log, refresh=False,
               stable_seconds=STABLE_SECONDS, return_ids=False):
    clips_dir = Path(config["clips_dir"])
    if not clips_dir.exists():
        log(f"clips directory does not exist: {clips_dir}")
        return ([], []) if return_ids else 0
    added = 0
    added_ids = []
    for path in sorted(clips_dir.glob("*.mp4")):
        if path.name.startswith("."):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if time.time() - stat.st_mtime < stable_seconds:
            continue
        resolved = str(path.resolve())
        existing = connection.execute(
            "SELECT * FROM clips WHERE path = ?", (resolved,)
        ).fetchone()
        if existing:
            if refresh and existing["status"] in (
                "pending_review",
                "approved",
                "rejected",
                "failed",
            ):
                info = build_metadata(path, clips_dir, config)
                connection.execute(
                    """
                    UPDATE clips SET streamer = ?, platform = ?, source_url = ?,
                        title = ?, description = ?, tags = ?, srt_path = ?,
                        duration = ?, caption_status = ?
                    WHERE id = ?
                    """,
                    (
                        info["streamer"],
                        info["platform"],
                        info["source_url"],
                        info["title"],
                        info["description"],
                        json.dumps(info["tags"]),
                        info["srt_path"],
                        info["duration"],
                        info["caption_status"],
                        existing["id"],
                    ),
                )
            continue
        try:
            digest = sha256_file(path)
        except OSError as exc:
            log(f"could not read {path.name}: {exc}")
            continue
        duplicate = connection.execute(
            "SELECT id FROM clips WHERE sha256 = ?", (digest,)
        ).fetchone()
        if duplicate:
            log(f"skipping duplicate {path.name} (already queued as clip {duplicate['id']})")
            continue
        info = build_metadata(path, clips_dir, config)
        cursor = connection.execute(
            """
            INSERT INTO clips (
                path, filename, sha256, size, mtime, streamer, platform,
                source_url, title, description, tags, srt_path, duration,
                caption_status, status, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending_review', ?)
            """,
            (
                resolved,
                path.name,
                digest,
                stat.st_size,
                stat.st_mtime,
                info["streamer"],
                info["platform"],
                info["source_url"],
                info["title"],
                info["description"],
                json.dumps(info["tags"]),
                info["srt_path"],
                info["duration"],
                info["caption_status"],
                _now_iso(),
            ),
        )
        added += 1
        added_ids.append(cursor.lastrowid)
    connection.commit()
    if added:
        log(f"added {added} clip(s) to the review queue")
    if return_ids:
        return added, added_ids
    return added


def list_clips(connection, status=None, as_json=False):
    sql = "SELECT * FROM clips"
    params = ()
    if status:
        sql += " WHERE status = ?"
        params = (status,)
    sql += " ORDER BY id"
    rows = [dict(row) for row in connection.execute(sql, params).fetchall()]
    if as_json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
        return
    if not rows:
        print("no clips")
        return
    for row in rows:
        duration = row["duration"] or 0
        print(
            f"{row['id']:>4}  {row['status']:<14}  "
            f"{duration:>6.1f}s  {row['title']}  ({row['filename']})"
        )
        if row["last_error"]:
            print(f"      error: {row['last_error']}")


def _get_clip(connection, clip_id):
    return connection.execute(
        "SELECT * FROM clips WHERE id = ?", (clip_id,)
    ).fetchone()


def approve_clips(connection, clip_ids):
    for clip_id in clip_ids:
        row = _get_clip(connection, clip_id)
        if not row:
            print(f"clip {clip_id}: not found")
            continue
        if row["status"] == "uploaded":
            print(f"clip {clip_id}: already uploaded")
            continue
        if row["status"] == "uploading":
            print(f"clip {clip_id}: currently uploading")
            continue
        connection.execute(
            "UPDATE clips SET status = 'approved', approved_at = ?, last_error = NULL WHERE id = ?",
            (_now_iso(), clip_id),
        )
        print(f"clip {clip_id}: approved")
    connection.commit()


def reject_clips(connection, clip_ids):
    for clip_id in clip_ids:
        row = _get_clip(connection, clip_id)
        if not row:
            print(f"clip {clip_id}: not found")
            continue
        if row["status"] == "uploaded":
            print(f"clip {clip_id}: already uploaded")
            continue
        connection.execute(
            "UPDATE clips SET status = 'rejected', rejected_at = ? WHERE id = ?",
            (_now_iso(), clip_id),
        )
        print(f"clip {clip_id}: rejected")
    connection.commit()


def retry_clips(connection, clip_ids):
    for clip_id in clip_ids:
        row = _get_clip(connection, clip_id)
        if not row:
            print(f"clip {clip_id}: not found")
            continue
        if row["status"] == "uploaded":
            print(f"clip {clip_id}: already uploaded")
            continue
        connection.execute(
            "UPDATE clips SET status = 'approved', last_error = NULL WHERE id = ?",
            (clip_id,),
        )
        connection.execute(
            "UPDATE uploads SET status = 'pending', last_error = NULL, next_retry_at = NULL WHERE clip_id = ?",
            (clip_id,),
        )
        print(f"clip {clip_id}: queued for retry")
    connection.commit()


def status_report(connection):
    counts = {
        row["status"]: row["count"]
        for row in connection.execute(
            "SELECT status, COUNT(*) AS count FROM clips GROUP BY status"
        ).fetchall()
    }
    uploads = {
        f"{row['platform']}:{row['status']}": row["count"]
        for row in connection.execute(
            "SELECT platform, status, COUNT(*) AS count FROM uploads GROUP BY platform, status"
        ).fetchall()
    }
    print("clips:")
    if counts:
        for status, count in sorted(counts.items()):
            print(f"  {status}: {count}")
    else:
        print("  none")
    print("uploads:")
    if uploads:
        for status, count in sorted(uploads.items()):
            print(f"  {status}: {count}")
    else:
        print("  none")
    quota_rows = connection.execute(
        "SELECT platform, count FROM quota WHERE day = ?", (_today(),)
    ).fetchall()
    print("today:")
    if quota_rows:
        for row in quota_rows:
            print(f"  {row['platform']}: {row['count']}")
    else:
        print("  none")


def _platforms(config, platform=None):
    if platform:
        return [platform]
    return list(config["defaults"].get("platforms") or ["youtube"])


def _privacy(config):
    return config.get("youtube", {}).get(
        "privacy", config["defaults"].get("privacy", "unlisted")
    )


def _category_id(config):
    return str(
        config.get("youtube", {}).get(
            "category_id", config["defaults"].get("category_id", "20")
        )
    )


def _upload_row(connection, clip_id, platform):
    return connection.execute(
        "SELECT * FROM uploads WHERE clip_id = ? AND platform = ?",
        (clip_id, platform),
    ).fetchone()


def _mark_uploading(connection, clip_id, platform, privacy):
    connection.execute(
        """
        INSERT INTO uploads (
            clip_id, platform, status, privacy, attempts, created_at
        ) VALUES (?, ?, 'uploading', ?, 1, ?)
        ON CONFLICT(clip_id, platform) DO UPDATE SET
            status = 'uploading',
            privacy = excluded.privacy,
            attempts = uploads.attempts + 1,
            last_error = NULL,
            next_retry_at = NULL
        """,
        (clip_id, platform, privacy, _now_iso()),
    )
    connection.execute(
        "UPDATE clips SET status = 'uploading', last_error = NULL WHERE id = ?",
        (clip_id,),
    )
    connection.commit()


def _refresh_clip_status(connection, clip_id):
    rows = connection.execute(
        "SELECT status FROM uploads WHERE clip_id = ?", (clip_id,)
    ).fetchall()
    if not rows:
        return
    states = [row["status"] for row in rows]
    if all(state == "uploaded" for state in states):
        status = "uploaded"
    elif any(state == "failed" for state in states):
        status = "failed"
    elif any(state == "uploading" for state in states):
        status = "uploading"
    else:
        status = "approved"
    connection.execute(
        "UPDATE clips SET status = ? WHERE id = ?", (status, clip_id)
    )
    connection.commit()


def _payload(clip, config):
    return {
        "clip_id": clip["id"],
        "platform": "youtube",
        "video": clip["path"],
        "title": clip["title"],
        "description": clip["description"],
        "tags": json.loads(clip["tags"]),
        "privacy": _privacy(config),
        "category_id": _category_id(config),
        "srt": clip["srt_path"],
    }


def publish_once(connection, config, log, dry_run=False, limit=10,
                 platform=None, auto_approve=False, auto_ids=None):
    if auto_approve and not dry_run:
        params = [_now_iso()]
        sql = (
            "UPDATE clips SET status = 'approved', approved_at = ? "
            "WHERE status = 'pending_review'"
        )
        if auto_ids is not None:
            ids = sorted({int(value) for value in auto_ids if int(value) > 0})
            if not ids:
                ids = [-1]
            sql += f" AND id IN ({','.join('?' for _ in ids)})"
            params.extend(ids)
        cursor = connection.execute(sql, params)
        connection.commit()
        if cursor.rowcount:
            log(f"auto-approved {cursor.rowcount} clip(s)")
    rows = connection.execute(
        """
        SELECT c.* FROM clips c
        LEFT JOIN uploads u ON u.clip_id = c.id AND u.platform = 'youtube'
        WHERE c.status = 'approved'
           OR (c.status = 'failed' AND (
                u.next_retry_at IS NULL OR u.next_retry_at <= ?
           ))
        ORDER BY c.id LIMIT ?
        """,
        (_now_iso(), max(1, int(limit))),
    ).fetchall()
    if not rows:
        log("no approved clips")
        return 0
    published = 0
    processed = 0
    for clip in rows:
        for name in _platforms(config, platform):
            if name != "youtube":
                log(f"unsupported platform: {name}")
                continue
            if not dry_run:
                maximum = int(
                    config["defaults"].get("max_daily_uploads", 100)
                )
                used = _quota_used(connection, name)
                if used >= maximum:
                    log(f"daily {name} upload limit reached ({used}/{maximum})")
                    return processed if dry_run else published
            existing = _upload_row(connection, clip["id"], name)
            if existing and existing["status"] == "uploaded":
                continue
            payload = _payload(clip, config)
            if dry_run:
                print(json.dumps(payload, indent=2, ensure_ascii=False))
                processed += 1
                continue
            privacy = payload["privacy"]
            _mark_uploading(connection, clip["id"], name, privacy)
            log(f"uploading clip {clip['id']} to {name}: {clip['title']}")
            try:
                result = upload_youtube(clip, config, log)
            except Exception as exc:
                error = str(exc)[:2000]
                attempt_row = _upload_row(connection, clip["id"], name)
                attempts = int(attempt_row["attempts"]) if attempt_row else 1
                connection.execute(
                    """
                    UPDATE uploads SET status = 'failed', last_error = ?,
                        next_retry_at = ?
                    WHERE clip_id = ? AND platform = ?
                    """,
                    (error, _next_retry(config, attempts), clip["id"], name),
                )
                connection.execute(
                    "UPDATE clips SET status = 'failed', last_error = ? WHERE id = ?",
                    (error, clip["id"]),
                )
                connection.commit()
                log(f"upload failed for clip {clip['id']}: {error}")
                continue
            connection.execute(
                """
                UPDATE uploads SET status = 'uploaded', remote_id = ?, url = ?,
                    privacy = ?, uploaded_at = ?, last_error = NULL
                WHERE clip_id = ? AND platform = ?
                """,
                (
                    result["remote_id"],
                    result["url"],
                    privacy,
                    _now_iso(),
                    clip["id"],
                    name,
                ),
            )
            _increment_quota(connection, name)
            connection.commit()
            published += 1
            processed += 1
            log(f"uploaded clip {clip['id']}: {result['url']}")
        _refresh_clip_status(connection, clip["id"])
    return processed if dry_run else published


def auth_youtube(config, log):
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise SystemExit(
            "Install Google API packages: pip install --user "
            "google-api-python-client google-auth-oauthlib google-auth-httplib2"
        ) from exc
    client_secret = Path(config["youtube"]["client_secret"])
    token_path = Path(config["youtube"]["token"])
    if not client_secret.exists():
        raise SystemExit(f"missing OAuth client secret: {client_secret}")
    flow = InstalledAppFlow.from_client_secrets_file(
        str(client_secret), SCOPES
    )
    credentials = flow.run_local_server(port=0)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(credentials.to_json(), encoding="utf-8")
    os.chmod(token_path, 0o600)
    log(f"YouTube token saved to {token_path}")


def youtube_service(config, log):
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Install Google API packages: pip install --user "
            "google-api-python-client google-auth-oauthlib google-auth-httplib2"
        ) from exc
    token_path = Path(config["youtube"]["token"])
    if not token_path.exists():
        raise RuntimeError(
            f"YouTube token not found at {token_path}; run: "
            "python3 publisher.py auth youtube"
        )
    credentials = Credentials.from_authorized_user_file(
        str(token_path), SCOPES
    )
    if not credentials.valid:
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            token_path.write_text(credentials.to_json(), encoding="utf-8")
            os.chmod(token_path, 0o600)
        else:
            raise RuntimeError(
                "YouTube token is invalid; run: python3 publisher.py auth youtube"
            )
    return build("youtube", "v3", credentials=credentials)


def _upload_caption(service, video_id, srt_path, language, log):
    from googleapiclient.http import MediaFileUpload

    body = {
        "snippet": {
            "videoId": video_id,
            "language": language,
            "name": "English",
        }
    }
    media = MediaFileUpload(
        str(srt_path), mimetype="application/octet-stream", resumable=False
    )
    for attempt in range(3):
        try:
            service.captions().insert(
                part="snippet", body=body, media_body=media
            ).execute()
            log(f"uploaded captions for {video_id}")
            return
        except Exception as exc:
            if attempt == 2:
                raise
            log(f"caption upload retry {attempt + 1} for {video_id}: {exc}")
            time.sleep(5)


def upload_youtube(clip, config, log):
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload

    service = youtube_service(config, log)
    youtube_config = config.get("youtube", {})
    defaults = config["defaults"]
    body = {
        "snippet": {
            "title": clip["title"],
            "description": clip["description"],
            "tags": json.loads(clip["tags"]),
            "categoryId": _category_id(config),
        },
        "status": {
            "privacyStatus": _privacy(config),
            "selfDeclaredMadeForKids": bool(
                defaults.get("made_for_kids", False)
            ),
        },
    }
    media = MediaFileUpload(
        clip["path"], mimetype="video/mp4", chunksize=4 * 1024 * 1024,
        resumable=True
    )
    request = service.videos().insert(
        part="snippet,status", body=body, media_body=media
    )
    response = None
    while response is None:
        try:
            progress, response = request.next_chunk()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status == 403 and "quota" in str(exc).lower():
                raise RuntimeError(
                    "YouTube upload quota exceeded; retry after the quota resets"
                ) from exc
            raise
        if progress:
            log(f"upload progress {clip['id']}: {int(progress.progress() * 100)}%")
    video_id = response["id"]
    url = f"https://www.youtube.com/shorts/{video_id}"
    srt_path = clip["srt_path"]
    if (
        srt_path
        and Path(srt_path).exists()
        and youtube_config.get(
            "upload_captions", defaults.get("upload_captions", True)
        )
    ):
        try:
            _upload_caption(
                service,
                video_id,
                srt_path,
                defaults.get("language", "en"),
                log,
            )
        except Exception as exc:
            log(f"video uploaded but captions failed for {video_id}: {exc}")
    return {"remote_id": video_id, "url": url}


def build_parser():
    parser = argparse.ArgumentParser(
        description="Review and publish clipped videos."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth = subparsers.add_parser("auth", help="authorize a platform")
    auth.add_argument("platform", nargs="?", default="youtube", choices=["youtube"])

    scan = subparsers.add_parser("scan", help="add new clips to the queue")
    scan.add_argument("--all", action="store_true", help="refresh existing queue entries")

    listing = subparsers.add_parser("list", help="show queued clips")
    listing.add_argument("--status")
    listing.add_argument("--json", action="store_true")

    approve = subparsers.add_parser("approve", help="approve clips for publishing")
    approve.add_argument("ids", nargs="+", type=int)

    reject = subparsers.add_parser("reject", help="reject clips")
    reject.add_argument("ids", nargs="+", type=int)

    retry = subparsers.add_parser("retry", help="retry failed clips")
    retry.add_argument("ids", nargs="+", type=int)

    recap = subparsers.add_parser(
        "recaption", help="generate transcript/SRT sidecars for existing clips"
    )
    recap.add_argument("ids", nargs="*", type=int)
    recap.add_argument("--all", action="store_true", help="recaption every clip")

    publish = subparsers.add_parser("publish", help="publish approved clips")
    mode = publish.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--watch", action="store_true")
    publish.add_argument("--dry-run", action="store_true")
    publish.add_argument("--limit", type=int, default=10)
    publish.add_argument("--platform", choices=["youtube"])
    publish.add_argument(
        "--auto-approve",
        action="store_true",
        help="approve newly discovered clips automatically",
    )
    publish.add_argument(
        "--include-backlog",
        action="store_true",
        help="with --auto-approve, also approve clips already in the queue",
    )

    subparsers.add_parser("status", help="show queue counts")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    log = Logger(config["log_file"])

    if args.command == "auth":
        auth_youtube(config, log)
        return 0

    connection = open_db(config["state_db"])
    try:
        if args.command == "scan":
            added = scan_clips(connection, config, log, refresh=args.all)
            print(f"added {added} clip(s)")
        elif args.command == "list":
            list_clips(connection, args.status, args.json)
        elif args.command == "approve":
            approve_clips(connection, args.ids)
        elif args.command == "reject":
            reject_clips(connection, args.ids)
        elif args.command == "retry":
            retry_clips(connection, args.ids)
        elif args.command == "recaption":
            count = recaption_clips(
                connection, config, log, ids=args.ids, all_clips=args.all
            )
            print(f"recaptioned {count} clip(s)")
        elif args.command == "status":
            status_report(connection)
        elif args.command == "publish":
            _, new_ids = scan_clips(connection, config, log, return_ids=True)
            if args.watch:
                if args.auto_approve and args.include_backlog:
                    publish_once(
                        connection,
                        config,
                        log,
                        limit=args.limit,
                        platform=args.platform,
                        auto_approve=True,
                    )
                auto_ids = set()
                if args.auto_approve and args.include_backlog:
                    auto_ids.update(new_ids)
                while True:
                    _, new_ids = scan_clips(
                        connection, config, log, return_ids=True
                    )
                    if args.auto_approve:
                        auto_ids.update(new_ids)
                    publish_once(
                        connection,
                        config,
                        log,
                        limit=args.limit,
                        platform=args.platform,
                        auto_approve=args.auto_approve,
                        auto_ids=sorted(auto_ids),
                    )
                    time.sleep(float(config.get("watch_interval", 10)))
            else:
                auto_ids = None
                if args.auto_approve and not args.include_backlog:
                    auto_ids = new_ids
                publish_once(
                    connection,
                    config,
                    log,
                    dry_run=args.dry_run,
                    limit=args.limit,
                    platform=args.platform,
                    auto_approve=args.auto_approve,
                    auto_ids=auto_ids,
                )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nstopped")
