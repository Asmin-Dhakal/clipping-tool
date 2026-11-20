import argparse
import contextlib
import importlib.util
import io
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import live_clipper
import publisher

PROJECT_DIR = Path(__file__).resolve().parent
STATE_PATH = PROJECT_DIR / ".dashboard_processes.json"
LOG_DIR = PROJECT_DIR / "logs"
DEFAULT_PORT = 8765

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Clip Publisher Dashboard</title>
<style>
:root{color-scheme:dark;--bg:#0b0f17;--card:#151c2a;--line:#26324a;--text:#e8edf7;--muted:#9aa8c2;--accent:#6ea8fe;--good:#3ddc84;--bad:#ff6b6b;--warn:#ffd166}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{padding:18px 24px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between}
h1{margin:0;font-size:20px}
h2{margin:0 0 12px;font-size:16px}
main{padding:20px;max-width:1400px;margin:0 auto;display:grid;gap:18px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px}
.card .label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.06em}
.card .value{font-size:18px;margin-top:6px}
.running{color:var(--good)}.stopped{color:var(--muted)}.failed{color:var(--bad)}
label{display:block;color:var(--muted);font-size:12px;margin:10px 0 4px}
input,select,textarea{width:100%;background:#0e1420;border:1px solid var(--line);color:var(--text);border-radius:6px;padding:8px;font-family:inherit}
input[type=checkbox]{width:auto;margin-right:6px}
.row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}
.actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:14px}
button{background:var(--accent);border:0;border-radius:6px;color:#07101f;padding:8px 12px;font-weight:650;cursor:pointer}
button.secondary{background:#26324a;color:var(--text)}
button.danger{background:var(--bad);color:#170808}
button:disabled{opacity:.5;cursor:not-allowed}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-size:12px;text-transform:uppercase}
pre{background:#080c13;border:1px solid var(--line);border-radius:8px;padding:12px;height:220px;overflow:auto;white-space:pre-wrap;font:12px/1.4 ui-monospace,SFMono-Regular,Menlo,monospace}
.logs{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:14px}
.badge{display:inline-block;padding:2px 7px;border-radius:99px;background:#26324a;font-size:12px}
.badge.pending_review{background:#4a3a16;color:var(--warn)}
.badge.approved,.badge.uploaded{background:#173f2b;color:var(--good)}
.badge.failed{background:#4a1f22;color:var(--bad)}
#toast{position:fixed;right:18px;bottom:18px;background:#26324a;border:1px solid var(--line);padding:10px 14px;border-radius:8px;display:none;max-width:420px}
.small{color:var(--muted);font-size:12px}
code{background:#0e1420;border:1px solid var(--line);padding:2px 5px;border-radius:4px}
</style>
</head>
<body>
<header>
<h1>Clip Publisher Dashboard</h1>
<div class="small"><span id="autoLabel">live</span> · <button class="secondary" id="refreshBtn">Refresh</button></div>
</header>
<main>
<section class="grid" id="cards">
<div class="card"><div class="label">Clipper</div><div class="value" id="cardClipper">checking…</div></div>
<div class="card"><div class="label">Publisher watcher</div><div class="value" id="cardPublisher">checking…</div><div class="small" id="publisherDetail"></div></div>
<div class="card"><div class="label">Queue</div><div class="value" id="cardQueue">checking…</div><div class="small" id="queueDetail"></div></div>
<div class="card"><div class="label">Today's uploads</div><div class="value" id="cardQuota">checking…</div><div class="small" id="quotaDetail"></div></div>
<div class="card"><div class="label">YouTube OAuth</div><div class="value" id="cardOAuth">checking…</div><div class="small" id="oauthDetail"></div></div>
</section>

<section class="card">
<h2>Clipper</h2>
<form id="clipperForm">
<label>Stream URL or Twitch channel<input name="url" placeholder="https://www.youtube.com/watch?v=..." required></label>
<div class="row">
<label>Multiplier<input name="mult" type="number" step="0.1" value="1.5"></label>
<label>Min messages<input name="min_msgs" type="number" value="15"></label>
<label>Min unique chatters<input name="min_unique" type="number" value="5"></label>
<label>Window (s)<input name="window" type="number" value="10"></label>
<label>Before spike (s)<input name="pre" type="number" value="25"></label>
<label>After spike (s)<input name="post" type="number" value="12"></label>
<label>Cooldown (s)<input name="cooldown" type="number" value="45"></label>
<label>Quality<input name="quality" value="1080p60,1080p,best"></label>
<label>Caption name<input name="caption_name" placeholder="Streamer name"></label>
<label>Layout<select name="layout"><option value="split">Split cam + game</option><option value="blurred">Blurred full frame</option></select></label>
<label>Webcam crop (optional)<input name="webcam_crop" placeholder="auto-detect by default; x:y:w:h"></label>
<label>YouTube format<input name="youtube_format" placeholder="1080p H.264 (default)"></label>
<label>Output folder<input name="out" value="clips"></label>
<label>Speaker threshold<input name="speaker_threshold" type="number" step="0.01" value="0.30"></label>
</div>
<div class="actions">
<label><input name="no_captions" type="checkbox"> No captions</label>
<label><input name="no_speaker_filter" type="checkbox"> Caption all voices</label>
<label><input name="reset_speaker_profile" type="checkbox"> Reset voice profile</label>
<label><input name="hook_caption" type="checkbox"> Burn hook caption into video</label>
</div>
<div class="actions">
<button type="submit">Start clipper</button>
<button type="button" class="danger" id="stopClipper">Stop clipper</button>
</div>
</form>
</section>

<section class="card">
<h2>Review queue</h2>
<div class="row">
<label>Search<input id="queueSearch" placeholder="title, file, streamer"></label>
<label>Status<select id="queueFilter"><option value="all">All</option><option value="pending_review">Pending review</option><option value="approved">Approved</option><option value="uploaded">Uploaded</option><option value="failed">Failed</option><option value="rejected">Rejected</option></select></label>
</div>
<div class="actions">
<button id="scanBtn" class="secondary">Scan clips</button>
<button id="recaptionSelected" class="secondary">Recaption selected</button>
<button id="recaptionAll" class="secondary">Recaption all missing</button>
<button id="approveSelected" class="secondary">Approve selected</button>
<button id="rejectSelected" class="secondary">Reject selected</button>
<button id="retrySelected" class="secondary">Retry selected failed</button>
</div>
<table>
<thead><tr><th></th><th>ID</th><th>Status</th><th>Title</th><th>File</th><th>Duration</th><th>Error</th><th>Action</th></tr></thead>
<tbody id="queueBody"></tbody>
</table>
</section>

<section class="card">
<h2>Publisher</h2>
<div class="actions">
<button id="dryRunBtn" class="secondary">Dry run once</button>
<button id="publishOnceBtn">Publish approved once</button>
<button id="watchStartBtn">Start watcher</button>
<button id="fullAutoBtn">Start full auto</button>
<button id="watchStopBtn" class="danger">Stop watcher</button>
<button id="authBtn" class="secondary">Run YouTube OAuth setup</button>
<label><input id="includeBacklog" type="checkbox"> Include existing backlog in full auto</label>
</div>
<p class="small">Full auto publishes only new clips unless backlog is included. First run: <code>pip install --user google-api-python-client google-auth-oauthlib google-auth-httplib2</code>, place your Desktop OAuth client JSON at <code>~/.config/clip-publisher/client_secret.json</code>, then click OAuth setup. Default upload privacy is <code>unlisted</code>.</p>
</section>

<section class="card">
<h2>Settings</h2>
<form id="settingsForm">
<div class="row">
<label>Streamer slug<input name="slug" placeholder="yt_qSyo5d-dVzA"></label>
<label>Display name<input name="display_name" placeholder="IShowSpeed"></label>
<label>Streamer hashtag<input name="streamer_hashtag" placeholder="#IShowSpeed"></label>
<label>Game<input name="game" placeholder="GTA 6"></label>
<label>Streamer game override<input name="streamer_game" placeholder="optional"></label>
<label>Manual webcam crop (optional)<input name="webcam_crop" placeholder="auto-detect by default; crop=600:450:0:560"></label>
<label>Privacy<select name="privacy"><option value="unlisted">Unlisted</option><option value="public">Public</option><option value="private">Private</option></select></label>
<label>Daily upload limit<input name="max_daily_uploads" type="number" value="100"></label>
</div>
<label>Global hashtags<input name="hashtags" placeholder="#GTA6, #Shorts"></label>
<label>Title template<input name="title_template"></label>
<label>Description template<textarea name="description_template" rows="6"></textarea></label>
<div class="actions"><button type="submit">Save settings</button><button type="button" class="secondary" id="reloadSettings">Reload</button></div>
</form>
</section>

<section class="logs">
<div class="card"><h2>Clipper log</h2><pre id="clipperLog"></pre></div>
<div class="card"><h2>Publisher log</h2><pre id="publisherLog"></pre></div>
<div class="card"><h2>OAuth log</h2><pre id="authLog"></pre></div>
</section>
</main>
<div id="toast"></div>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(path,method='GET',body=null){
  const opts={method,headers:{}};
  if(body!==null){opts.headers['Content-Type']='application/json';opts.body=JSON.stringify(body);}
  const res=await fetch(path,opts);
  const text=await res.text();
  let data;
  try{data=JSON.parse(text);}catch{data={text};}
  if(!res.ok||data.error)throw new Error(data.error||('HTTP '+res.status));
  return data;
}
function toast(message){const el=$('toast');el.textContent=message;el.style.display='block';clearTimeout(toast.t);toast.t=setTimeout(()=>el.style.display='none',5000);}
function selectedIds(){return [...document.querySelectorAll('input[data-id]:checked')].map(x=>Number(x.dataset.id));}
function stateText(info){return info.running?`running · pid ${info.pid}`:'stopped';}
function stateClass(info){return info.running?'running':'stopped';}
async function refresh(){
  try{
    const status=await api('/api/status');
    $('cardClipper').textContent=stateText(status.clipper);$('cardClipper').className='value '+stateClass(status.clipper);
    const watch=status.publisher_watch;
    $('cardPublisher').textContent=watch.running?`running${watch.auto?' · full auto':''} · pid ${watch.pid}`:'stopped';
    $('cardPublisher').className='value '+(watch.running?'running':'stopped');
    $('publisherDetail').textContent=`recaption: ${stateText(status.recaption)}`;
    $('cardQueue').textContent=Object.entries(status.queue).map(([k,v])=>`${k}: ${v}`).join(' · ');
    $('queueDetail').textContent=`retries due: ${status.due_retries}`;
    const q=($('queueSearch').value||'').toLowerCase();
    const filter=$('queueFilter').value;
    const clips=status.clips.filter(c=>(filter==='all'||c.status===filter)&&(!q||`${c.title} ${c.filename}`.toLowerCase().includes(q)));
    const rows=clips.map(c=>{
      let action='';
      if(c.status==='uploaded'&&c.url)action=`<a href="${esc(c.url)}" target="_blank">Open</a>`;
      else action=`<button class="secondary" data-action="approve" data-id="${c.id}">✓</button> <button class="secondary" data-action="reject" data-id="${c.id}">✕</button> ${c.status==='failed'?`<button class="secondary" data-action="retry" data-id="${c.id}">↻</button>`:''}`;
      return `<tr>
      <td><input type="checkbox" data-id="${c.id}"></td>
      <td>${c.id}</td>
      <td><span class="badge ${esc(c.status)}">${esc(c.status)}</span></td>
      <td>${esc(c.title)}</td>
      <td>${esc(c.filename)}</td>
      <td>${Number(c.duration||0).toFixed(1)}s</td>
      <td>${esc(c.last_error||'')}</td>
      <td>${action}</td></tr>`;
    }).join('');
    $('queueBody').innerHTML=rows||'<tr><td colspan="8">no matching clips</td></tr>';
    const quota=status.quota||{used:0,limit:100};
    $('cardQuota').textContent=`${quota.used} / ${quota.limit}`;
    $('quotaDetail').textContent=`remaining: ${Math.max(0,quota.limit-quota.used)}`;
    const o=status.oauth;$('cardOAuth').textContent=o.google_packages?(o.token?'authorized':'needs auth'):'packages missing';
    $('cardOAuth').className='value '+(o.token?'running':'failed');
    $('oauthDetail').textContent=`packages: ${o.google_packages?'yes':'no'} · secret: ${o.client_secret?'yes':'no'} · token: ${o.token?'yes':'no'}`;
    for(const [name,id] of [['clipper','clipperLog'],['publisher','publisherLog'],['auth','authLog']]){
      const text=await api('/api/log?name='+name+'&lines=200');
      const el=$(id);const atBottom=el.scrollTop+el.clientHeight>=el.scrollHeight-20;
      el.textContent=text.text;if(atBottom)el.scrollTop=el.scrollHeight;
    }
  }catch(e){toast(e.message);}
}
$('queueBody').onclick=async e=>{
  const button=e.target.closest('button[data-action]');
  if(!button)return;
  try{await api('/api/clips/action','POST',{action:button.dataset.action,ids:[Number(button.dataset.id)]});toast(`${button.dataset.action} clip ${button.dataset.id}`);await refresh();}
  catch(err){toast(err.message);}
};
$('clipperForm').addEventListener('submit',async e=>{
  e.preventDefault();const fd=new FormData(e.target);const body={};
  for(const [k,v] of fd.entries())body[k]=v;
  for(const k of ['no_captions','no_speaker_filter','reset_speaker_profile','hook_caption'])body[k]=fd.has(k);
  try{await api('/api/clipper/start','POST',body);toast('clipper started');await refresh();}catch(err){toast(err.message);}
});
$('stopClipper').onclick=async()=>{try{await api('/api/clipper/stop','POST',{});toast('clipper stopped');await refresh();}catch(e){toast(e.message);}};
$('refreshBtn').onclick=refresh;
async function loadSettings(){
  const s=await api('/api/settings');const d=s.defaults||{};const f=$('settingsForm');
  f.title_template.value=d.title_template||'';
  f.description_template.value=d.description_template||'';
  f.game.value=d.game||'';
  f.privacy.value=d.privacy||'unlisted';
  f.max_daily_uploads.value=d.max_daily_uploads||100;
  f.hashtags.value=(d.hashtags||[]).join(' ');
  const slug=f.slug.value||Object.keys(s.streamers||{})[0]||'';
  f.slug.value=slug;
  const entry=(s.streamers||{})[slug]||{};
  f.display_name.value=entry.display_name||'';
  f.streamer_hashtag.value=(entry.hashtags||[])[0]||'';
  f.webcam_crop.value=entry.webcam_crop||'';
  f.streamer_game.value=entry.game||'';
}
$('settingsForm').addEventListener('submit',async e=>{
  e.preventDefault();const f=e.target;const body=Object.fromEntries(new FormData(f).entries());
  try{await api('/api/settings','POST',body);toast('settings saved');await loadSettings();await refresh();}
  catch(err){toast(err.message);}
});
$('reloadSettings').onclick=async()=>{try{await loadSettings();toast('settings reloaded');}catch(e){toast(e.message);}};
$('scanBtn').onclick=async()=>{try{const r=await api('/api/scan','POST',{all:false});toast(`added ${r.added} clip(s)`);await refresh();}catch(e){toast(e.message);}};
async function bulk(action){const ids=selectedIds();if(!ids.length){toast('select clips first');return;}
  try{await api('/api/clips/action','POST',{action,ids});toast(`${action} ${ids.length} clip(s)`);await refresh();}catch(e){toast(e.message);}}
$('recaptionSelected').onclick=async()=>{const ids=selectedIds();if(!ids.length){toast('select clips first');return;}
  try{await api('/api/recaption','POST',{ids});toast('recaption started');await refresh();}catch(e){toast(e.message);}};
$('recaptionAll').onclick=async()=>{try{await api('/api/recaption','POST',{all:true});toast('recaption started');await refresh();}catch(e){toast(e.message);}};
$('approveSelected').onclick=()=>bulk('approve');
$('rejectSelected').onclick=()=>bulk('reject');
$('retrySelected').onclick=()=>bulk('retry');
$('dryRunBtn').onclick=async()=>{try{const r=await api('/api/publish','POST',{mode:'dry-run',limit:10});toast(r.count?`dry run prepared ${r.count} clip(s)`:'no approved clips');await refresh();}catch(e){toast(e.message);}};
$('publishOnceBtn').onclick=async()=>{try{const r=await api('/api/publish','POST',{mode:'once',limit:10});toast(`published ${r.published} clip(s)`);await refresh();}catch(e){toast(e.message);}};
$('watchStartBtn').onclick=async()=>{try{await api('/api/publish','POST',{mode:'watch-start',limit:10});toast('watcher started');await refresh();}catch(e){toast(e.message);}};
$('fullAutoBtn').onclick=async()=>{try{await api('/api/publish','POST',{mode:'full-auto-start',limit:10,include_backlog:$('includeBacklog').checked});toast('full auto started');await refresh();}catch(e){toast(e.message);}};
$('watchStopBtn').onclick=async()=>{try{await api('/api/publish','POST',{mode:'watch-stop'});toast('watcher stopped');await refresh();}catch(e){toast(e.message);}};
$('authBtn').onclick=async()=>{try{await api('/api/auth/start','POST',{});toast('oauth setup started; check the OAuth log');await refresh();}catch(e){toast(e.message);}};
loadSettings().then(refresh);setInterval(()=>{if(!document.hidden)refresh();},4000);
</script>
</body>
</html>
"""


def _tail(path, lines):
    path = Path(path)
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return "".join(deque(handle, maxlen=lines))


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes", "on")


def _as_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ProcessManager:
    def __init__(self, state_path, project_dir):
        self.state_path = Path(state_path)
        self.project_dir = Path(project_dir)
        self.lock = threading.RLock()
        self.state = self._load()

    def _load(self):
        if not self.state_path.exists():
            return {}
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.state, indent=2), encoding="utf-8")
        tmp.replace(self.state_path)

    @staticmethod
    def _pid_alive(pid):
        try:
            os.kill(int(pid), 0)
            return True
        except (OSError, TypeError, ValueError):
            return False

    @staticmethod
    def _pid_matches(pid, marker):
        if not marker:
            return True
        try:
            raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
            command = raw.decode("utf-8", "replace").replace("\x00", " ")
        except (OSError, ValueError):
            return True
        return marker in command

    def info(self, name):
        with self.lock:
            entry = self.state.get(name)
            if not entry:
                return {"running": False}
            pid = entry.get("pid")
            if not self._pid_alive(pid) or not self._pid_matches(pid, entry.get("marker", "")):
                self.state.pop(name, None)
                self._save()
                return {"running": False}
            result = dict(entry)
            result["running"] = True
            result["pid"] = pid
            return result

    def start(self, name, command, log_name):
        with self.lock:
            current = self.state.get(name)
            if current and self._pid_alive(current.get("pid")) and self._pid_matches(
                current.get("pid"), current.get("marker", "")
            ):
                raise RuntimeError(f"{name} is already running")
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            log_path = LOG_DIR / f"{log_name}.log"
            handle = log_path.open("ab", buffering=0)
            process = subprocess.Popen(
                command,
                cwd=str(self.project_dir),
                stdin=subprocess.DEVNULL,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            handle.close()
            marker = Path(command[1]).name if len(command) > 1 else ""
            self.state[name] = {
                "pid": process.pid,
                "marker": marker,
                "log": str(log_path),
                "command": " ".join(command),
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            self._save()
            return self.info(name)

    def stop(self, name):
        with self.lock:
            entry = self.state.get(name)
            if not entry:
                return False
            pid = entry.get("pid")
            if not self._pid_alive(pid) or not self._pid_matches(pid, entry.get("marker", "")):
                self.state.pop(name, None)
                self._save()
                return False
            try:
                os.killpg(os.getpgid(int(pid)), signal.SIGTERM)
            except OSError:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except OSError:
                    pass
            for _ in range(50):
                if not self._pid_alive(pid):
                    break
                time.sleep(0.1)
            if self._pid_alive(pid):
                try:
                    os.killpg(os.getpgid(int(pid)), signal.SIGKILL)
                except OSError:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                    except OSError:
                        pass
            self.state.pop(name, None)
            self._save()
            return True

    def cleanup(self):
        for name in list(self.state):
            self.info(name)


def config():
    return publisher.load_config(PROJECT_DIR / "publisher.json")


def raw_config():
    path = PROJECT_DIR / "publisher.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_raw_config(data):
    path = PROJECT_DIR / "publisher.json"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(path)


def public_settings():
    raw = raw_config()
    defaults = publisher._deep_merge(
        publisher.DEFAULT_CONFIG_DATA["defaults"], raw.get("defaults", {})
    )
    return {
        "defaults": defaults,
        "streamers": raw.get("streamers", {}),
    }


def _hashtag_input(value):
    tags = []
    for token in re.split(r"[,\n]", str(value or "")):
        tag = token.strip()
        if not tag:
            continue
        tags.append(tag if tag.startswith("#") else "#" + tag)
    return tags


def queue_counts(connection):
    rows = connection.execute(
        "SELECT status, COUNT(*) AS count FROM clips GROUP BY status"
    ).fetchall()
    return {row["status"]: row["count"] for row in rows}


def recent_clips(connection, limit=100):
    rows = connection.execute(
        """
        SELECT c.id, c.status, c.title, c.filename, c.duration,
               c.caption_status, c.last_error, u.url
        FROM clips c
        LEFT JOIN uploads u ON u.clip_id = c.id AND u.platform = 'youtube'
        ORDER BY c.id DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def oauth_status(settings):
    packages = ["googleapiclient", "google_auth_oauthlib", "google.oauth2.credentials"]
    installed = all(importlib.util.find_spec(name) for name in packages)
    youtube = settings["youtube"]
    return {
        "google_packages": installed,
        "client_secret": Path(youtube["client_secret"]).exists(),
        "token": Path(youtube["token"]).exists(),
    }


def capture(callable_):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = callable_()
    text = buffer.getvalue().strip()
    if text:
        settings = config()
        publisher.Logger(settings["log_file"])(text)
    return result


def clipper_command(body):
    url = str(body.get("url", "")).strip()
    if not url:
        raise RuntimeError("stream URL is required")
    command = [sys.executable, str(PROJECT_DIR / "live_clipper.py"), url]
    numeric = {
        "mult": ("--mult", _as_float, 3.0),
        "min_msgs": ("--min-msgs", _as_int, 15),
        "min_unique": ("--min-unique", _as_int, 5),
        "window": ("--window", _as_int, 10),
        "pre": ("--pre", _as_int, 25),
        "post": ("--post", _as_int, 12),
        "cooldown": ("--cooldown", _as_int, 45),
        "retries": ("--retries", _as_int, 0),
        "speaker_threshold": ("--speaker-threshold", _as_float, 0.30),
    }
    for key, (flag, converter, default) in numeric.items():
        if body.get(key) not in (None, ""):
            command.extend([flag, str(converter(body.get(key), default))])
    text_flags = {
        "quality": "--quality",
        "caption_name": "--caption-name",
        "layout": "--layout",
        "webcam_crop": "--webcam-crop",
        "youtube_format": "--youtube-format",
        "out": "--out",
        "whisper_model": "--whisper-model",
        "demucs_model": "--demucs-model",
        "caption_font": "--caption-font",
    }
    for key, flag in text_flags.items():
        value = str(body.get(key, "")).strip()
        if value:
            command.extend([flag, value])
    for key, flag in (
        ("no_captions", "--no-captions"),
        ("no_speaker_filter", "--no-speaker-filter"),
        ("reset_speaker_profile", "--reset-speaker-profile"),
        ("hook_caption", "--hook-caption"),
    ):
        if _as_bool(body.get(key)):
            command.append(flag)
    return command


class Handler(BaseHTTPRequestHandler):
    server_version = "ClipDashboard/1.0"
    manager = None

    def _send(self, status, payload, content_type="application/json"):
        data = payload if isinstance(payload, bytes) else str(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _json(self, payload, status=200):
        self._send(status, json.dumps(payload, ensure_ascii=False))

    def _error(self, message, status=400):
        self._json({"error": str(message)}, status)

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send(200, PAGE, "text/html; charset=utf-8")
            return
        if parsed.path == "/api/status":
            settings = config()
            connection = publisher.open_db(settings["state_db"])
            try:
                watcher = self.manager.info("publisher_watch")
                watcher["auto"] = "--auto-approve" in str(watcher.get("command", ""))
                due = connection.execute(
                    "SELECT COUNT(*) AS count FROM uploads WHERE status = 'failed' AND next_retry_at <= ?",
                    (publisher._now_iso(),),
                ).fetchone()["count"]
                payload = {
                    "clipper": self.manager.info("clipper"),
                    "publisher_watch": watcher,
                    "recaption": self.manager.info("recaption"),
                    "youtube_auth": self.manager.info("youtube_auth"),
                    "queue": queue_counts(connection),
                    "due_retries": due,
                    "quota": {
                        "used": publisher._quota_used(connection, "youtube"),
                        "limit": int(
                            settings["defaults"].get("max_daily_uploads", 100)
                        ),
                    },
                    "clips": recent_clips(connection),
                    "oauth": oauth_status(settings),
                }
            finally:
                connection.close()
            self._json(payload)
            return
        if parsed.path == "/api/log":
            query = parse_qs(parsed.query)
            name = query.get("name", [""])[0]
            lines = min(1000, max(20, _as_int(query.get("lines", ["200"])[0], 200)))
            logs = {
                "clipper": [LOG_DIR / "clipper_dashboard.log", PROJECT_DIR / "clipper.log"],
                "publisher": [
                    PROJECT_DIR / "publisher.log",
                    LOG_DIR / "publisher_watch.log",
                    LOG_DIR / "recaption.log",
                ],
                "auth": [LOG_DIR / "youtube_auth.log"],
            }
            if name not in logs:
                self._error("unknown log", 404)
                return
            text = "".join(_tail(path, lines) for path in logs[name])
            self._json({"text": text})
            return
        if parsed.path == "/api/settings":
            self._json(public_settings())
            return
        self._error("not found", 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            body = self._body()
            settings = config()
            if path == "/api/clipper/start":
                command = clipper_command(body)
                info = self.manager.start("clipper", command, "clipper_dashboard")
                self._json(info)
                return
            if path == "/api/clipper/stop":
                self._json({"stopped": self.manager.stop("clipper")})
                return
            if path == "/api/scan":
                connection = publisher.open_db(settings["state_db"])
                try:
                    added = publisher.scan_clips(
                        connection,
                        settings,
                        publisher.Logger(settings["log_file"]),
                        refresh=_as_bool(body.get("all")),
                    )
                finally:
                    connection.close()
                self._json({"added": added})
                return
            if path == "/api/settings":
                data = raw_config()
                data_defaults = data.setdefault("defaults", {})
                data_youtube = data.setdefault("youtube", {})
                for key in (
                    "title_template",
                    "description_template",
                    "game",
                    "privacy",
                ):
                    if key in body:
                        data_defaults[key] = str(body[key])
                if "hashtags" in body:
                    data_defaults["hashtags"] = _hashtag_input(body["hashtags"])
                if "max_daily_uploads" in body:
                    data_defaults["max_daily_uploads"] = _as_int(
                        body["max_daily_uploads"], 100
                    )
                data_youtube["privacy"] = str(
                    body.get("privacy", data_defaults.get("privacy", "unlisted"))
                )
                slug = str(body.get("slug", "")).strip()
                if slug:
                    streamers = data.setdefault("streamers", {})
                    entry = streamers.setdefault(slug, {})
                    if "display_name" in body:
                        entry["display_name"] = str(body["display_name"])
                    if "streamer_hashtag" in body:
                        entry["hashtags"] = _hashtag_input(body["streamer_hashtag"])
                    if "webcam_crop" in body:
                        entry["webcam_crop"] = str(body["webcam_crop"]).strip()
                    if body.get("streamer_game"):
                        entry["game"] = str(body["streamer_game"])
                    elif "streamer_game" in body:
                        entry.pop("game", None)
                save_raw_config(data)
                updated = config()
                connection = publisher.open_db(updated["state_db"])
                try:
                    publisher.scan_clips(
                        connection,
                        updated,
                        publisher.Logger(updated["log_file"]),
                        refresh=True,
                    )
                finally:
                    connection.close()
                self._json({"saved": True})
                return
            if path == "/api/clips/action":
                action = str(body.get("action", ""))
                ids = [_as_int(value, 0) for value in body.get("ids", [])]
                ids = [value for value in ids if value]
                if action not in ("approve", "reject", "retry"):
                    raise RuntimeError("unknown clip action")
                connection = publisher.open_db(settings["state_db"])
                try:
                    if action == "approve":
                        capture(lambda: publisher.approve_clips(connection, ids))
                    elif action == "reject":
                        capture(lambda: publisher.reject_clips(connection, ids))
                    else:
                        capture(lambda: publisher.retry_clips(connection, ids))
                finally:
                    connection.close()
                self._json({"updated": len(ids)})
                return
            if path == "/api/recaption":
                command = [
                    sys.executable,
                    str(PROJECT_DIR / "publisher.py"),
                    "recaption",
                ]
                if _as_bool(body.get("all")):
                    command.append("--all")
                else:
                    ids = [_as_int(value, 0) for value in body.get("ids", [])]
                    command.extend(str(value) for value in ids if value)
                self._json(self.manager.start("recaption", command, "recaption"))
                return
            if path == "/api/publish":
                mode = str(body.get("mode", "once"))
                limit = _as_int(body.get("limit"), 10)
                if mode in ("watch-start", "full-auto-start"):
                    command = [
                        sys.executable,
                        str(PROJECT_DIR / "publisher.py"),
                        "publish",
                        "--watch",
                        "--limit",
                        str(limit),
                    ]
                    if mode == "full-auto-start":
                        command.append("--auto-approve")
                        if _as_bool(body.get("include_backlog")):
                            command.append("--include-backlog")
                    self._json(self.manager.start("publisher_watch", command, "publisher_watch"))
                    return
                if mode == "watch-stop":
                    self._json({"stopped": self.manager.stop("publisher_watch")})
                    return
                if mode not in ("once", "dry-run"):
                    raise RuntimeError("unknown publish mode")
                connection = publisher.open_db(settings["state_db"])
                try:
                    capture(lambda: publisher.scan_clips(connection, settings, publisher.Logger(settings["log_file"])))
                    published = capture(
                        lambda: publisher.publish_once(
                            connection,
                            settings,
                            publisher.Logger(settings["log_file"]),
                            dry_run=(mode == "dry-run"),
                            limit=limit,
                        )
                    )
                finally:
                    connection.close()
                if mode == "dry-run":
                    self._json({"published": 0, "count": published})
                else:
                    self._json({"published": published, "count": published})
                return
            if path == "/api/auth/start":
                command = [
                    sys.executable,
                    str(PROJECT_DIR / "publisher.py"),
                    "auth",
                    "youtube",
                ]
                self._json(self.manager.start("youtube_auth", command, "youtube_auth"))
                return
            self._error("not found", 404)
        except (ValueError, RuntimeError) as exc:
            self._error(exc)
        except Exception as exc:
            self._error(f"internal error: {exc}", 500)

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(description="Local dashboard for the clipper and publisher.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    Handler.manager = ProcessManager(STATE_PATH, PROJECT_DIR)
    Handler.manager.cleanup()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"dashboard running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping dashboard")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
