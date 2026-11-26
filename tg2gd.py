"""
Telegram -> (Render/PC disk) -> Google Drive   |  multi-account, resumable, live UI

Features:
- Full Channel mode (start msg ID se auto-scan + resumable)
- File List / Queue mode (per-file Telegram links ya specific channel ki Msg ID range se list banakar upload)
- CSV Import mode (id_link, NEW TITLE, PATH, TYPE, size_bytes, INDEX ID wali CSV se auto subfolder tree, rename, aur Continue from Specific INDEX ID)
- Per-Account Pause / Resume (kisi bhi account ko live pause/resume karo bina baaki accounts roke)
- Google Drive Root State (tg2gd_state.txt — Render restart safe, no 4KB limit, built-in online State Editor in UI)
- Google Drive Folder Picker & Creator Pop-up (link paste karna zaruri nahi, seedha Drive se select ya create karo)
- Built-in Google Drive OAuth Re-Login (token expire / invalid_grant hone par seedha Web UI se ya CLI `--get-token` se wapis login karke naya token banao)
- Automated 7z Store Mode Encryption (per-file .7z, no compression, simple password + header lock; native 7z CLI ya py7zr hybrid)

Chalane ke liye (Windows / Linux / Render):
    python tg2gd.py                    (normal server + Web UI)
    python tg2gd.py --get-token        (sirf naya Google Drive token banana ho, default PORT)
    python tg2gd.py --get-token 8085   (alag port par token banana ho)

Missing packages khud install ho jaate hain. Same folder me accounts.json + token.json rakho
(ya env: ACCOUNTS_JSON / TOKEN_JSON / GOOGLE_CREDENTIALS_JSON, Render Secret Files: /etc/secrets/).

Env (sab optional):
    PORT=8080  DATA_DIR=./data  APP_PASSWORD=808177  STATE_INTERVAL=10  STATE_FILE_NAME=tg2gd_state.txt
    CONN=1 (har account ke max parallel connections)  MAX_MBPS=2 (har account ki max download speed, MB/s; 0 = unlimited)
    OAUTH_REDIRECT_URI= (optional custom OAuth redirect URI)
"""
import os, sys, re, io, csv, json, glob, time, asyncio, secrets, logging, collections, shutil, subprocess, importlib, threading
from urllib.parse import urlparse, parse_qs


def ensure_deps():
    need = {
        "aiohttp": "aiohttp",
        "telethon": "telethon",
        "googleapiclient": "google-api-python-client",
        "google.auth": "google-auth",
        "google_auth_oauthlib": "google-auth-oauthlib",
        "py7zr": "py7zr",
    }
    miss = []
    for mod, pkg in need.items():
        try:
            importlib.import_module(mod)
        except ImportError:
            miss.append(pkg)
    if miss:
        print(f"[setup] install ho raha hai: {', '.join(miss)}", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *miss])
        importlib.invalidate_caches()
    try:
        importlib.import_module("cryptg")
        return True
    except ImportError:
        print("[setup] cryptg install try kar raha hoon (tez decrypt)...", flush=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "cryptg"], capture_output=True)
        try:
            importlib.invalidate_caches()
            importlib.import_module("cryptg")
            return True
        except ImportError:
            print("[setup] cryptg nahi laga (optional) - script chalegi, speed thodi kam", flush=True)
            return False


HAS_CRYPTG = ensure_deps()

HERE = os.path.dirname(os.path.abspath(__file__))
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
os.environ.setdefault("OAUTHLIB_RELAX_TOKEN_SCOPE", "1")


def env_files():
    fs = [os.path.join(HERE, ".env")] + sorted(glob.glob(os.path.join(HERE, "*.env")))
    out = []
    for f in fs:
        if os.path.exists(f) and f not in out:
            out.append(f)
    return out


def read_env_value(key):
    for f in env_files():
        for line in open(f, encoding="utf-8-sig"):
            line = line.strip()
            if line.startswith(key + "="):
                v = line.split("=", 1)[1].strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                    v = v[1:-1]
                return v
    return None


def load_dotenv():
    """Script ke folder ki .env aur har *.env file (jaise TELE-SERVICE-DRIVE.env) padh kar os.environ me daalta hai
    (pehle se set env vars ko override nahi karta)."""
    for p in env_files():
        for line in open(p, encoding="utf-8-sig"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                v = v[1:-1]
            if v:
                os.environ.setdefault(k, v)


load_dotenv()

from aiohttp import web
from telethon import TelegramClient
from telethon.errors import FloodWaitError
try:
    from telethon.errors import FloodPremiumWaitError   # non-premium account throttle (GetFileRequest)
except ImportError:
    FloodPremiumWaitError = FloodWaitError
FLOOD = (FloodWaitError, FloodPremiumWaitError)          # dono ko ek jaisa FloodWait maana jaayega
from telethon.sessions import StringSession
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

logging.basicConfig(level=logging.ERROR)
PORT = int(os.environ.get("PORT", 8080))
PASSWORD = os.environ.get("APP_PASSWORD", "808177")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.getcwd(), "data"))
STATE_INTERVAL = float(os.environ.get("STATE_INTERVAL", 10))
STATE_FILE_NAME = os.environ.get("STATE_FILE_NAME", "tg2gd_state.txt")
CONN = int(os.environ.get("CONN", 1))                    # har account ke liye max parallel connections (default 1)
MAX_MBPS = float(os.environ.get("MAX_MBPS", 2))          # har account ki max download speed (MB/s), 0 = unlimited
MAX_BPS = int(MAX_MBPS * 1024 * 1024)
_NEXT = {}                                               # per-account throttle clock
PRIMARY_SMALL = os.environ.get("PRIMARY_SMALL", "0") == "1"   # 1 = chhoti files sirf primary se (default band)
SEG = 8 * 1024 * 1024            # 1 segment = 8 MB (accounts me baante jaate hain)
REQ = 1024 * 1024                # telegram request size
DEAD = {"AuthKeyUnregisteredError", "SessionRevokedError", "UserDeactivatedError",
        "UserDeactivatedBanError", "AuthKeyDuplicatedError", "SessionExpiredError"}
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    _t = os.path.join(DATA_DIR, ".w")
    open(_t, "w").close()
    os.remove(_t)
except OSError:
    import tempfile
    print(f"[setup] DATA_DIR '{DATA_DIR}' use nahi ho sakta (disk nahi / permission) -> temp folder use hoga", flush=True)
    DATA_DIR = os.path.join(tempfile.gettempdir(), "tg2gd")
    os.makedirs(DATA_DIR, exist_ok=True)

FOLDER_CACHE = {}
FOLDER_LOCK = threading.Lock()
TOKEN_LOCK = threading.Lock()
_LIVE_TOKEN = None
_CACHED_CREDS = None
OAUTH_FLOWS = {}   # state -> {"flow": Flow, "redirect_uri": str, "ts": float, "manual": bool}


class _AccPaused(Exception):
    """Internal signal jab koi account mid-segment pause kiya jaaye."""


def load_json(env, fname):
    v = os.environ.get(env)
    if v:
        return json.loads(v)
    for p in (fname, os.path.join(HERE, fname), os.path.join("/etc/secrets", fname)):
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    raise SystemExit(f"{env} ya {fname} nahi mila")


def empty_csv_staged():
    return {"filename": "", "items": [], "skipped": 0, "categories": {}, "folders_count": 0, "channels": [], "total_bytes": 0}


# ------------------------------------------------------------------ global state
S = {
    "status": "idle", "phase": "", "mode": "channel",
    "channel": "", "channel_title": "", "folder": "", "folder_name": "", "start_id": 0,
    "queue": [], "done_keys": [],
    "csv_staged": empty_csv_staged(),
    "csv_opts": {"use_path": True, "use_title": True, "cats": [], "start_index_id": ""},
    "last_done": 0, "last_index_id": "", "done_count": 0, "total_bytes": 0,
    "scan": {"files": 0, "bytes": 0},
    "plan": {"files": 0, "bytes": 0, "dl_files": 0, "dl_bytes": 0, "up_files": 0, "up_bytes": 0},
    "started_at": 0, "ended_at": 0, "speed": 0, "state_ts": 0, "state_file_id": "",
    "enc_enabled": False, "enc_pass": "",
    "drive_ok": True, "drive_expired": False, "drive_email": "", "drive_err": "",
    "dl": None, "ul": None, "next": [],
    "accs": {}, "log": collections.deque(maxlen=300), "hist": collections.deque(maxlen=120),
    "stop": False, "failed": False,
}
CLIENTS = {}
PRIMARY = None
PRIMARY_AID = None
STATE_FILE_ID = None
STATE_LOCK = None
cur_state = {"cur": None}
CUR_OWNER = [bytearray()]
CUR_TASKS = []
JOB = None


def log(m, lv=""):
    S["log"].append([time.strftime("%H:%M:%S"), lv, m])
    print(m, flush=True)


def fmt(n):
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


def is_dead(e):
    return type(e).__name__ in DEAD


# ------------------------------------------------------------------ Google Drive OAuth & Token management (merged get_token)
def is_token_error(e):
    msg = str(e).lower()
    name = type(e).__name__.lower()
    return (
        "refresherror" in name
        or "invalid_grant" in msg
        or "token has been expired" in msg
        or "token has been revoked" in msg
        or "unauthorized_client" in msg
        or "token.json nahi mila" in msg
        or "refresh_token" in msg
        or "invalid_client" in msg
    )


def load_token_dict():
    """Live in-memory token, phir DATA_DIR/token.json, phir TOKEN_JSON env, phir token.json files."""
    if _LIVE_TOKEN:
        return dict(_LIVE_TOKEN)
    data_tok = os.path.join(DATA_DIR, "token.json")
    if os.path.exists(data_tok):
        try:
            with open(data_tok, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    v = os.environ.get("TOKEN_JSON")
    if v:
        return json.loads(v)
    for p in ("token.json", os.path.join(HERE, "token.json"), os.path.join("/etc/secrets", "token.json")):
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    raise RuntimeError("TOKEN_JSON ya token.json nahi mila — Google Drive login karo")


def update_env_token(token_json_str):
    """Env file (.env / *.env) me TOKEN_JSON line replace karta hai (backup .bak ke saath)."""
    target = None
    for f in env_files():
        try:
            for line in open(f, encoding="utf-8-sig"):
                if line.strip().startswith("TOKEN_JSON="):
                    target = f
                    break
        except OSError:
            continue
        if target:
            break
    if not target:
        fs = env_files()
        if not fs:
            return None
        target = fs[0]
    try:
        shutil.copy2(target, target + ".bak")
        lines = open(target, encoding="utf-8-sig").read().splitlines()
        new_line = "TOKEN_JSON='" + token_json_str + "'"
        done = False
        for i, line in enumerate(lines):
            if line.strip().startswith("TOKEN_JSON="):
                lines[i] = new_line
                done = True
                break
        if not done:
            lines.append(new_line)
        with open(target, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")
        return target
    except OSError:
        return None


def save_token_dict(out):
    """Naya token memory, env, DATA_DIR/token.json, HERE/token.json aur .env file me save karta hai."""
    global _LIVE_TOKEN, _CACHED_CREDS
    clean = {k: out.get(k) for k in ("token", "refresh_token", "token_uri", "client_id", "client_secret", "scopes")}
    clean["token_uri"] = clean.get("token_uri") or "https://oauth2.googleapis.com/token"
    clean["scopes"] = clean.get("scopes") or DRIVE_SCOPES
    if not clean.get("refresh_token"):
        raise RuntimeError("refresh_token nahi mila. https://myaccount.google.com/permissions se purani access hata kar dobara login karo.")
    compact = json.dumps(clean, separators=(",", ":"))
    with TOKEN_LOCK:
        _LIVE_TOKEN = clean
        _CACHED_CREDS = None
        os.environ["TOKEN_JSON"] = compact
    saved_paths = []
    for p in (os.path.join(HERE, "token.json"), os.path.join(DATA_DIR, "token.json")):
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(clean, f, indent=2)
            saved_paths.append(p)
        except OSError:
            pass
    env_updated = update_env_token(compact)
    with FOLDER_LOCK:
        FOLDER_CACHE.clear()
    return clean, compact, saved_paths, env_updated


def get_client_config():
    """OAuth client credentials dhoondhta hai:
    1. DATA_DIR / HERE / /etc/secrets me client_secret*.json ya credentials.json
    2. GOOGLE_CREDENTIALS_JSON env var
    3. Fallback: maujooda token.json / TOKEN_JSON ke client_id + client_secret
    Returns: (cfg_dict, source_label, client_kind)
    """
    search_dirs = [DATA_DIR, HERE, "/etc/secrets"]
    seen = set()
    for d in search_dirs:
        if not d or not os.path.isdir(d):
            continue
        for pat in ("client_secret*.json", "credentials.json"):
            for p in sorted(glob.glob(os.path.join(d, pat))):
                rp = os.path.abspath(p)
                if rp in seen:
                    continue
                seen.add(rp)
                try:
                    cfg = json.load(open(p, encoding="utf-8"))
                    if "web" in cfg or "installed" in cfg:
                        kind = "web" if "web" in cfg else "installed"
                        return cfg, os.path.basename(p), kind
                except Exception:
                    continue
    v = os.environ.get("GOOGLE_CREDENTIALS_JSON") or read_env_value("GOOGLE_CREDENTIALS_JSON")
    if v:
        cfg = json.loads(v)
        kind = "web" if "web" in cfg else "installed"
        return cfg, "GOOGLE_CREDENTIALS_JSON", kind
    try:
        t = load_token_dict()
        if t.get("client_id") and t.get("client_secret"):
            cfg = {
                "installed": {
                    "client_id": t["client_id"],
                    "client_secret": t["client_secret"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": t.get("token_uri") or "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost"],
                }
            }
            return cfg, "existing token.json (client_id/secret)", "installed"
    except Exception:
        pass
    raise RuntimeError("OAuth credentials nahi mile. Google Cloud se client_secret.json download karke folder me rakho ya UI me upload karo.")


def get_drive_creds():
    """Valid Google Drive Credentials return karta hai; expire hone par S['drive_expired']=True set karta hai."""
    global _CACHED_CREDS
    with TOKEN_LOCK:
        if _CACHED_CREDS is not None and _CACHED_CREDS.valid:
            return _CACHED_CREDS
    try:
        t = load_token_dict()
        creds = Credentials(
            token=t.get("token"),
            refresh_token=t.get("refresh_token"),
            token_uri=t.get("token_uri") or "https://oauth2.googleapis.com/token",
            client_id=t.get("client_id"),
            client_secret=t.get("client_secret"),
            scopes=t.get("scopes") or DRIVE_SCOPES,
        )
        if not creds.valid:
            creds.refresh(Request())
        with TOKEN_LOCK:
            _CACHED_CREDS = creds
            if _LIVE_TOKEN is not None:
                _LIVE_TOKEN["token"] = creds.token
        S["drive_ok"] = True
        S["drive_expired"] = False
        S["drive_err"] = ""
        return creds
    except Exception as e:
        if is_token_error(e):
            S["drive_ok"] = False
            S["drive_expired"] = True
            S["drive_err"] = str(e)[:220]
        raise


def verify_drive_connection_sync():
    """Drive token check karke user email aur status update karta hai."""
    try:
        em = drive_service().about().get(fields="user(emailAddress)").execute()["user"]["emailAddress"]
        S["drive_ok"] = True
        S["drive_expired"] = False
        S["drive_email"] = em
        S["drive_err"] = ""
        return True, em, ""
    except Exception as e:
        err = str(e)[:220]
        S["drive_ok"] = False
        S["drive_expired"] = is_token_error(e) or True
        S["drive_err"] = err
        return False, "", err


def _cleanup_oauth_flows():
    now = time.time()
    for st in [k for k, v in OAUTH_FLOWS.items() if now - v.get("ts", 0) > 900]:
        OAUTH_FLOWS.pop(st, None)


def pick_redirect_uri(req, cfg, kind, manual=False):
    """Request ke hisaab se best OAuth redirect URI choose karta hai."""
    if manual:
        return f"http://localhost:{PORT}/oauth/callback"
    env_uri = (os.environ.get("OAUTH_REDIRECT_URI") or "").strip()
    if env_uri:
        return env_uri
    proto = req.headers.get("X-Forwarded-Proto", req.scheme).split(",")[0].strip()
    host = req.headers.get("X-Forwarded-Host", req.host).split(",")[0].strip()
    origin = f"{proto}://{host}"
    if kind == "web":
        uris = cfg.get("web", {}).get("redirect_uris") or []
        # Pehle exact /oauth/callback match dhoondho, phir koi bhi same-origin URI (jaise http://localhost:8080/)
        for u in uris:
            if u.rstrip("/") == f"{origin}/oauth/callback":
                return u
        for u in uris:
            if u.startswith(origin):
                return u
    return f"{origin}/oauth/callback"


def build_oauth_url(req, manual=False, custom_redirect=""):
    _cleanup_oauth_flows()
    cfg, src, kind = get_client_config()
    redirect_uri = (custom_redirect or "").strip() or pick_redirect_uri(req, cfg, kind, manual=manual)
    flow = Flow.from_client_config(cfg, scopes=DRIVE_SCOPES, redirect_uri=redirect_uri)
    state = secrets.token_urlsafe(24)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
        state=state,
    )
    OAUTH_FLOWS[state] = {
        "flow": flow,
        "redirect_uri": redirect_uri,
        "ts": time.time(),
        "manual": bool(manual),
    }
    return auth_url, state, redirect_uri, src, kind


def exchange_oauth_code_sync(code, state="", redirect_uri=""):
    """Authorization code ko Google se exchange karke token save karta hai."""
    _cleanup_oauth_flows()
    code = (code or "").strip()
    if not code:
        raise RuntimeError("Authorization code khaali hai")
    entry = OAUTH_FLOWS.pop(state, None) if state else None
    if entry is None and OAUTH_FLOWS:
        # Agar user ne sirf raw code paste kiya (bina state ke), sabse recent flow use karo
        latest_st = max(OAUTH_FLOWS, key=lambda k: OAUTH_FLOWS[k]["ts"])
        entry = OAUTH_FLOWS.pop(latest_st)
    if entry is not None:
        flow = entry["flow"]
        r_uri = entry["redirect_uri"]
    else:
        cfg, _, _ = get_client_config()
        r_uri = (redirect_uri or "").strip() or f"http://localhost:{PORT}/oauth/callback"
        flow = Flow.from_client_config(cfg, scopes=DRIVE_SCOPES, redirect_uri=r_uri)
    if r_uri.startswith("http://"):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    flow.fetch_token(code=code)
    creds = flow.credentials
    d = json.loads(creds.to_json())
    clean, compact, saved_paths, env_updated = save_token_dict(d)
    ok, email, err = verify_drive_connection_sync()
    if not ok:
        raise RuntimeError(f"Token save hua par Drive verify fail: {err}")
    return clean, compact, email, saved_paths, env_updated


def run_cli_get_token(port=None):
    """Terminal se `python tg2gd.py --get-token [port]` chalane par seedha browser login se token banata hai."""
    use_port = int(port) if port else PORT
    cfg, src, kind = get_client_config()
    print(f"[info] credentials source: {src} (type: {kind})", flush=True)
    if kind == "web":
        print(
            f"\n[dhyan] Aapka client type 'Web application' hai. Google Cloud Console me is client ke\n"
            f"        'Authorized redirect URIs' me ye add hona chahiye:\n"
            f"        http://localhost:{use_port}/\n"
            f"        (aakhir ka / bhi ho, aur http hona chahiye. Console me jo likha ho wahi exact match hona chahiye.)\n"
            f"        (Ya 'Desktop app' type ka naya OAuth client bana kar uski client_secret.json yahan rakho.)\n",
            flush=True,
        )
    flow = InstalledAppFlow.from_client_config(cfg, DRIVE_SCOPES)
    creds = flow.run_local_server(
        port=use_port,
        access_type="offline",
        prompt="consent",
        open_browser=True,
        authorization_prompt_message="Browser me login karo: {url}\n",
        success_message="Done! Ye tab band kar sakte ho.",
    )
    if not creds.refresh_token:
        raise SystemExit("refresh_token nahi mila. https://myaccount.google.com/permissions se purani access hata kar dobara chalao.")
    d = json.loads(creds.to_json())
    _, _, saved_paths, env_updated = save_token_dict(d)
    print(f"\n[ok] token.json ban gaya ({', '.join(saved_paths)})", flush=True)
    if env_updated:
        print(f"[ok] {os.path.basename(env_updated)} me TOKEN_JSON update ho gaya (backup: {os.path.basename(env_updated)}.bak)", flush=True)
    print("\nAb python tg2gd.py chalao.", flush=True)


async def flood_safe(fn, who="", tries=10):
    """Control-plane calls (messages fetch etc.) FloodWait par ruk kar dobara try karte hain."""
    for _ in range(tries):
        try:
            return await fn()
        except FLOOD as e:
            log(f"[{who}] FloodWait {e.seconds}s - ruk raha hoon", "warn")
            for _ in range(e.seconds + 1):
                if S["stop"]:
                    raise
                await asyncio.sleep(1)
    return await fn()


# ------------------------------------------------------------------ Google Drive root state (tg2gd_state.txt)
def state_dict():
    paused_accs = [aid for aid, a in S["accs"].items() if a.get("paused")]
    return {
        "mode": S["mode"],
        "channel": S["channel"],
        "folder": S["folder"],
        "folder_name": S["folder_name"],
        "start_id": S["start_id"],
        "last_done": S["last_done"],
        "last_index_id": S["last_index_id"],
        "done_count": S["done_count"],
        "total_bytes": S["total_bytes"],
        "running": S["status"] == "running",
        "cur": cur_state["cur"],
        "paused_accs": paused_accs,
        "csv_opts": S["csv_opts"],
        "queue": S["queue"],
        "done_keys": S["done_keys"],
        "csv_staged": S["csv_staged"],
        "enc_enabled": S["enc_enabled"],
        "enc_pass": S["enc_pass"],
    }


def drive_find_state_file_sync(svc=None):
    svc = svc or drive_service()
    safe_n = STATE_FILE_NAME.replace("\\", "\\\\").replace("'", "\\'")
    q = f"name = '{safe_n}' and 'root' in parents and trashed = false"
    res = svc.files().list(
        q=q, spaces="drive", fields="files(id,name,modifiedTime)",
        orderBy="modifiedTime desc", pageSize=1,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def drive_load_state_sync():
    svc = drive_service()
    fid = drive_find_state_file_sync(svc)
    if not fid:
        return None, None, ""
    raw = svc.files().get_media(fileId=fid).execute()
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    data = json.loads(text) if text.strip() else {}
    return fid, data, text


def drive_save_state_sync(text, file_id=None):
    svc = drive_service()
    media = MediaInMemoryUpload(text.encode("utf-8"), mimetype="text/plain", resumable=False)
    fid = file_id or drive_find_state_file_sync(svc)
    if fid:
        try:
            r = svc.files().update(fileId=fid, media_body=media, fields="id").execute()
            return r["id"]
        except Exception as e:
            if "404" not in str(e) and "not found" not in str(e).lower():
                raise
    meta = {"name": STATE_FILE_NAME, "parents": ["root"], "mimeType": "text/plain"}
    r = svc.files().create(body=meta, media_body=media, fields="id").execute()
    return r["id"]


def apply_state_dict(d):
    """Drive state dict ko in-memory state S me load karta hai."""
    if not isinstance(d, dict):
        return
    mode = d.get("mode", "channel")
    if mode not in ("channel", "list", "csv"):
        mode = "channel"
    S.update(
        mode=mode,
        channel=str(d.get("channel") or ""),
        folder=str(d.get("folder") or ""),
        folder_name=str(d.get("folder_name") or ""),
        start_id=int(d.get("start_id") or 0),
        last_done=int(d.get("last_done") or 0),
        last_index_id=str(d.get("last_index_id") or ""),
        done_count=int(d.get("done_count") or 0),
        total_bytes=int(d.get("total_bytes") or 0),
        queue=d.get("queue") if isinstance(d.get("queue"), list) else [],
        done_keys=[str(x) for x in (d.get("done_keys") or [])],
        enc_enabled=bool(d.get("enc_enabled", False)),
        enc_pass=str(d.get("enc_pass") or ""),
    )
    if isinstance(d.get("csv_opts"), dict):
        co = d["csv_opts"]
        S["csv_opts"] = {
            "use_path": bool(co.get("use_path", True)),
            "use_title": bool(co.get("use_title", True)),
            "cats": [str(c) for c in (co.get("cats") or []) if str(c).strip()],
            "start_index_id": str(co.get("start_index_id") or "").strip(),
        }
    if isinstance(d.get("csv_staged"), dict) and isinstance(d["csv_staged"].get("items"), list):
        S["csv_staged"] = d["csv_staged"]
    else:
        S["csv_staged"] = empty_csv_staged()
    cur_state["cur"] = d.get("cur") if isinstance(d.get("cur"), dict) else None
    paused_set = set(d.get("paused_accs") or []) if isinstance(d.get("paused_accs"), list) else set()
    for aid, a in S["accs"].items():
        if a["status"] != "dead":
            a["paused"] = (aid in paused_set)


async def save_state(force=False):
    global STATE_FILE_ID, STATE_LOCK
    if STATE_LOCK is None:
        STATE_LOCK = asyncio.Lock()
    async with STATE_LOCK:
        d = state_dict()
        cmp_body = json.dumps(d, separators=(",", ":"), ensure_ascii=False)
        if cmp_body == getattr(save_state, "last", None) and not force:
            S["state_ts"] = time.time()
            return True
        payload = {**d, "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        txt = json.dumps(payload, indent=2, ensure_ascii=False)
        try:
            fid = await asyncio.to_thread(drive_save_state_sync, txt, STATE_FILE_ID)
            STATE_FILE_ID = fid
            S["state_file_id"] = fid or ""
            save_state.last = cmp_body
            S["state_ts"] = time.time()
            return True
        except Exception as e:
            log(f"drive state save err: {e}", "err")
            return False


async def state_loop():
    while True:
        await asyncio.sleep(STATE_INTERVAL)
        if S["status"] == "running":
            await save_state()


# ------------------------------------------------------------------ helpers
def safe_name(s):
    s = re.sub(r'[\\/:*?"<>|\r\n\t]', " ", s).strip(" .")
    return re.sub(r"\s+", " ", s)[:150]


def clean_drive_segment(s):
    """Google Drive folder/file name ke liye control chars hata kar clean karta hai (:, | wagairah Drive me valid hain)."""
    s = re.sub(r"[\x00-\x1f\x7f]+", " ", str(s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:400]


def drive_name(msg, custom_name=""):
    f = msg.file
    orig = (f.name or "") if f else ""
    ext = (f.ext or "") if f else ""
    if not ext and "." in orig:
        ext = "." + orig.rsplit(".", 1)[1]
    if custom_name:
        cn = clean_drive_segment(custom_name)
        if cn:
            if ext and not re.search(r"\.[A-Za-z0-9]{1,6}$", cn):
                cn = cn + ext
            return cn
    # poora caption (saari lines ek space se jud kar), jaisa hai waisa
    cap = re.sub(r"[\x00-\x1f\x7f]+", " ", msg.message or "")
    cap = re.sub(r"\s+", " ", cap).strip()
    if not cap:
        return safe_name(orig) or f"{msg.id}{ext}"
    max_cap = 400 - len(ext)
    if len(cap) > max_cap:
        cap = cap[:max_cap].rstrip()
    if ext and cap.lower().endswith(ext.lower()):
        return cap
    return cap + ext


def bm_get(bm, i):
    return bool(bm[i >> 3] & (1 << (i & 7)))


def bm_set(bm, i):
    bm[i >> 3] |= 1 << (i & 7)


def parse_folder(s):
    s = (s or "").strip()
    if not s or s.lower() == "root":
        return ""
    m = re.search(r"/folders/([\w-]+)", s) or re.search(r"[?&]id=([\w-]+)", s)
    return m.group(1) if m else s.split("?")[0]


def parse_channel(s):
    """('invite', hash) | ('id', int) | ('name', str)"""
    s = (s or "").strip()
    m = re.search(r"(?:t\.me|telegram\.me)/(?:joinchat/|\+)([\w-]+)", s)
    if m:
        return "invite", m.group(1)
    if s.startswith("@+") or (s.startswith("+") and not s[1:].isdigit()):
        return "invite", s.lstrip("@+")
    m = re.search(r"(?:t\.me|telegram\.me)/c/(\d+)", s)
    if m:
        return "id", int(f"-100{m.group(1)}")
    m = re.search(r"(?:t\.me|telegram\.me)/(?:s/)?([A-Za-z0-9_]+)", s)
    if m:
        return "name", "@" + m.group(1)
    if re.fullmatch(r"-?\d+", s):
        return "id", int(s)
    return "name", s if s.startswith("@") else "@" + s


def parse_msg_link(url):
    """Telegram message link se (channel_spec, msg_id) nikaalta hai.
    Supports:
      https://t.me/c/1234567890/456
      https://t.me/c/1234567890/12/456   (topic link)
      https://t.me/username/456
      https://t.me/s/username/456
      https://t.me/username/12/456
      telegram.me/... aur ?single / ?thread=... query params
    """
    u = (url or "").strip()
    if not u:
        return None
    u = u.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    m = re.search(r"(?:t\.me|telegram\.me)/c/(\d+)/(?:\d+/)?(\d+)$", u, re.I)
    if m:
        return f"-100{m.group(1)}", int(m.group(2))
    m = re.search(r"(?:t\.me|telegram\.me)/(?:s/)?([A-Za-z][A-Za-z0-9_]{3,})/(?:\d+/)?(\d+)$", u, re.I)
    if m:
        uname = m.group(1)
        if uname.lower() in ("c", "joinchat", "addlist", "addstickers", "proxy", "socks"):
            return None
        return "@" + uname, int(m.group(2))
    return None


def parse_csv_text(text, filename="upload.csv"):
    """NEW_PC.csv jaise format (OLD TITLE, id_link, size_bytes, NEW TITLE, TYPE, TOPIC, RESOURCE NAME, PATH ...)
    aur general Telegram-link CSV files ko parse karta hai."""
    text = (text or "").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    headers = [h for h in (reader.fieldnames or []) if h is not None]
    if not headers:
        raise ValueError("CSV me header row nahi mili")

    def find_col(exact_names, contains_names=()):
        for cand in exact_names:
            for h in headers:
                if h.strip() == cand:
                    return h
        low_map = {h.strip().lower(): h for h in headers}
        for cand in exact_names:
            if cand.lower() in low_map:
                return low_map[cand.lower()]
        for sub in contains_names:
            for h in headers:
                if sub in h.strip().lower():
                    return h
        return None

    col_link = find_col(["id_link", "link", "url", "telegram_link", "msg_link"], ["link", "url"])
    col_new_title = find_col(["NEW TITLE", "new_title", "filename", "file_name", "name", "title"])
    col_old_title = find_col(["OLD TITLE", "old_title"])
    col_path = find_col(["PATH", "path", "folder_path", "dir", "folder"])
    col_size = find_col(["size_bytes", "size", "bytes"])
    col_cat = find_col(["TYPE", "category", "cat"])
    if col_cat is None:
        col_cat = find_col(["type"])
    col_topic = find_col(["TOPIC", "topic", "chapter"])
    col_res = find_col(["RESOURCE NAME", "resource_name", "subject"])
    col_idx = find_col(["INDEX ID", "index_id", "index", "id"])

    if not col_link:
        raise ValueError("CSV me Telegram link column (jaise 'id_link') nahi mila")

    items = []
    skipped = 0
    categories = collections.Counter()
    folder_paths = set()
    channels = []
    ch_seen = set()
    total_bytes = 0

    for row in reader:
        raw_link = (row.get(col_link) or "").strip()
        parsed = parse_msg_link(raw_link)
        if not parsed:
            skipped += 1
            continue
        ch, mid = parsed
        if ch not in ch_seen:
            ch_seen.add(ch)
            channels.append(ch)

        new_title = clean_drive_segment(row.get(col_new_title) or "") if col_new_title else ""
        if not new_title and col_old_title:
            new_title = clean_drive_segment(row.get(col_old_title) or "")

        raw_path = (row.get(col_path) or "").strip() if col_path else ""
        folder_parts = []
        if raw_path:
            parts = [clean_drive_segment(p) for p in re.split(r"[/\\]+", raw_path) if p.strip()]
            if parts:
                last = parts[-1]
                has_ext = bool(re.search(r"\.[A-Za-z0-9]{1,6}$", last))
                if len(parts) >= 2 and (has_ext or (new_title and last == new_title)):
                    folder_parts = parts[:-1]
                    if not new_title:
                        new_title = last
                elif len(parts) == 1 and has_ext:
                    if not new_title:
                        new_title = last
                else:
                    folder_parts = parts

        if folder_parts:
            folder_paths.add("/".join(folder_parts))

        sz = 0
        if col_size:
            raw_sz = str(row.get(col_size) or "").strip().replace(",", "")
            if raw_sz.isdigit():
                sz = int(raw_sz)
        total_bytes += sz

        cat = (row.get(col_cat) or "").strip() if col_cat else ""
        if cat:
            categories[cat] += 1
        topic = (row.get(col_topic) or "").strip() if col_topic else ""
        res_name = (row.get(col_res) or "").strip() if col_res else ""
        idx_id = (row.get(col_idx) or "").strip() if col_idx else ""

        items.append({
            "kind": "csv",
            "channel": ch,
            "msg_id": mid,
            "name": new_title,
            "folder_parts": folder_parts,
            "size_hint": sz,
            "cat": cat,
            "topic": topic,
            "resource": res_name,
            "index_id": idx_id,
        })

    if not items:
        raise ValueError("CSV me koi valid Telegram link nahi mila")

    return {
        "filename": filename or "upload.csv",
        "items": items,
        "skipped": skipped,
        "categories": dict(categories),
        "folders_count": len(folder_paths),
        "channels": channels,
        "total_bytes": total_bytes,
    }


def filter_csv_items(items, cats=None, start_index_id=""):
    """CSV items ko start_index_id aur category filter ke hisaab se slice/filter karta hai."""
    res = list(items or [])
    sidx = str(start_index_id or "").strip()
    if sidx:
        exact_pos = None
        for i, it in enumerate(res):
            if str(it.get("index_id") or "").strip() == sidx:
                exact_pos = i
                break
        if exact_pos is not None:
            res = res[exact_pos:]
        elif sidx.isdigit():
            target = int(sidx)
            num_pos = None
            for i, it in enumerate(res):
                iv = str(it.get("index_id") or "").strip()
                if iv.isdigit() and int(iv) >= target:
                    num_pos = i
                    break
            res = res[num_pos:] if num_pos is not None else []
        else:
            res = []
    cat_set = set(c for c in (cats or []) if str(c).strip())
    if cat_set:
        res = [it for it in res if it.get("cat") in cat_set]
    return res


def csv_summary_view(staged):
    items = staged.get("items") or []
    return {
        "filename": staged.get("filename", ""),
        "total": len(items),
        "skipped": staged.get("skipped", 0),
        "categories": staged.get("categories", {}),
        "folders_count": staged.get("folders_count", 0),
        "channels": staged.get("channels", []),
        "total_bytes": staged.get("total_bytes", 0),
        "preview": [{
            "index_id": it.get("index_id", ""),
            "msg_id": it.get("msg_id", 0),
            "name": it.get("name", ""),
            "folder": "/".join(it.get("folder_parts") or []),
            "size_hint": it.get("size_hint", 0),
            "cat": it.get("cat", ""),
        } for it in items[:1500]],
    }


def normalize_queue(raw_queue):
    """Frontend se aayi queue ko validate/normalize karta hai."""
    out = []
    for it in (raw_queue or []):
        if not isinstance(it, dict):
            continue
        kind = it.get("kind")
        if kind in ("link", "csv"):
            ch = str(it.get("channel") or "").strip()
            mid = int(it.get("msg_id") or 0)
            if (not ch or mid <= 0) and it.get("raw"):
                parsed = parse_msg_link(it["raw"])
                if parsed:
                    ch, mid = parsed
            if ch and mid > 0:
                entry = {"kind": kind, "channel": ch, "msg_id": mid}
                if kind == "csv":
                    entry["name"] = str(it.get("name") or "")
                    entry["folder_parts"] = [str(p) for p in (it.get("folder_parts") or []) if str(p).strip()]
                    entry["size_hint"] = int(it.get("size_hint") or 0)
                    entry["cat"] = str(it.get("cat") or "")
                    entry["index_id"] = str(it.get("index_id") or "")
                out.append(entry)
        elif kind == "range":
            ch = str(it.get("channel") or "").strip()
            s_id = int(it.get("start_id") or 0)
            e_id = int(it.get("end_id") or 0)
            if ch and s_id > 0 and e_id > 0:
                if s_id > e_id:
                    s_id, e_id = e_id, s_id
                out.append({"kind": "range", "channel": ch, "start_id": s_id, "end_id": e_id})
    return out


async def resolve_channel(client, spec, who="", known_ent_id=None, known_title=None):
    from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
    from telethon.tl.types import ChatInviteAlready
    kind, val = parse_channel(spec)
    if kind == "invite":
        try:
            r = await client(CheckChatInviteRequest(val))
        except Exception as e:
            if type(e).__name__ in ("InviteHashExpiredError", "InviteHashInvalidError"):
                ent_id = known_ent_id or S.get("_ent_id")
                ch_title = known_title or S.get("channel_title")
                async for dlg in client.iter_dialogs():
                    if (ent_id and dlg.entity.id == ent_id) or (ch_title and dlg.name == ch_title):
                        log(f"[{who}] invite link expire, par channel dialogs me mil gaya: {dlg.name}", "warn")
                        return dlg.entity
                raise RuntimeError("Invite link expire/revoke ho gaya aur ye account channel me nahi hai. "
                                   "Naya invite link, @username ya -100... channel id daalo.")
            raise
        if isinstance(r, ChatInviteAlready):
            return r.chat
        log(f"[{who}] channel ka member nahi tha -> invite link se join kar raha hoon", "warn")
        try:
            u = await client(ImportChatInviteRequest(val))
            return u.chats[0]
        except FLOOD:
            raise
        except Exception as e:
            if "already" in str(e).lower():
                r = await client(CheckChatInviteRequest(val))
                return r.chat
            raise
    try:
        return await client.get_entity(val)
    except FLOOD:
        raise
    except Exception:
        await client.get_dialogs()
        return await client.get_entity(val)


async def get_peer(client, ent_id=None, channel_spec=None):
    from telethon.tl.types import PeerChannel
    eid = ent_id if ent_id is not None else S.get("_ent_id")
    spec = channel_spec if channel_spec is not None else S["channel"]
    if eid:
        try:
            return await client.get_input_entity(PeerChannel(int(eid)))
        except Exception:
            pass
    return await resolve_channel(client, spec, known_ent_id=eid)


# ------------------------------------------------------------------ account control
async def acc_flood(a, secs):
    """Account ko flood me daalo; uske saare connections saath me ruk kar ek hi baar wait karte hain."""
    until = time.time() + secs + 1
    if not (a["status"] == "flood" and a["flood_until"] >= until - 1):
        a["floods"] += 1
        a["status"] = "flood"
        a["flood_until"] = max(a["flood_until"], until)
        log(f"[{a['name']}] FloodWait {secs}s - ye account ruka, baaki chalte rahenge", "warn")
    while time.time() < a["flood_until"] and not S["stop"]:
        await asyncio.sleep(0.5)
    if a["status"] == "flood" and time.time() >= a["flood_until"] - 0.5:
        a["status"] = "ok"
        a["flood_until"] = 0


def flood_mark(a, secs):
    """Account ko flood mark karo, bina block kiye (baad me khud clear)."""
    a["floods"] += 1
    a["status"] = "flood"
    a["flood_until"] = time.time() + secs + 1
    log(f"[{a['name']}] FloodWait {secs}s - ye account ruka, baaki chalte rahenge", "warn")

    async def clear():
        await asyncio.sleep(secs + 1)
        if a["status"] == "flood" and time.time() >= a["flood_until"] - 0.5:
            a["status"], a["flood_until"] = "ok", 0
    asyncio.create_task(clear())


def acc_dead(a, e):
    a["status"] = "dead"
    a["last_err"] = f"{type(e).__name__}"
    log(f"[{a['name']}] session DEAD ({type(e).__name__}) - ye account hata diya", "err")


def set_account_paused(aid, paused):
    """Kisi ek account ko pause ya resume karta hai."""
    if aid not in S["accs"]:
        return False, "Account nahi mila"
    a = S["accs"][aid]
    if a["status"] == "dead":
        return False, f"{a['name']} dead hai"
    new_val = bool(paused)
    if a["paused"] != new_val:
        a["paused"] = new_val
        if new_val:
            a["speed"] = 0
        log(f"[{a['name']}] {'PAUSED (download/upload se roka gaya)' if new_val else 'RESUMED (wapas active)'}",
            "warn" if new_val else "ok")
    return True, ""


def pick_unpaused_client():
    """Photo ya single-client download ke liye koi bhi active (non-dead, non-paused) client chunta hai."""
    if PRIMARY_AID and S["accs"].get(PRIMARY_AID, {}).get("status") == "ok" and not S["accs"][PRIMARY_AID]["paused"]:
        return PRIMARY_AID, PRIMARY
    for aid, cl in CLIENTS.items():
        a = S["accs"][aid]
        if a["status"] != "dead" and not a["paused"]:
            return aid, cl
    return None, None


# ------------------------------------------------------------------ speed limiter (per account)
async def throttle(aid, n):
    """Is account ki total download speed MAX_BPS se upar nahi jaane deta."""
    if MAX_BPS <= 0 or n <= 0:
        return
    a = S["accs"].get(aid)
    now = time.monotonic()
    nxt = max(_NEXT.get(aid, 0), now) + n / MAX_BPS
    _NEXT[aid] = nxt
    d = nxt - now
    while d > 0 and not S["stop"]:
        if a and a.get("paused"):
            raise _AccPaused()
        await asyncio.sleep(min(d, 0.5))
        d -= 0.5


# ------------------------------------------------------------------ download (segments across accounts)
async def download_msg(msg, size, path, ent_id=None, channel_spec=None):
    """Success par real size return, stop par None."""
    dl = S["dl"]
    eid = ent_id if ent_id is not None else S.get("_ent_id")
    ch_spec = channel_spec if channel_spec is not None else S["channel"]

    if msg.photo and not msg.document:
        while not S["stop"]:
            aid, cl = pick_unpaused_client()
            if cl is not None:
                break
            await asyncio.sleep(0.5)
        if S["stop"]:
            return None
        who = S["accs"][aid]["name"]
        if cl is PRIMARY:
            await flood_safe(lambda: cl.download_media(msg, file=path), who)
        else:
            peer = await get_peer(cl, eid, ch_spec)
            m2 = await flood_safe(lambda: cl.get_messages(peer, ids=msg.id), who)
            await flood_safe(lambda: cl.download_media(m2, file=path), who)
        real = os.path.getsize(path)
        try:
            await throttle(aid, real)
        except _AccPaused:
            pass
        dl.update(done=real, size=real, nseg=1, segs_done=1)
        CUR_OWNER[0] = bytearray([S["accs"][aid]["idx"] + 1])
        S["accs"][aid]["bytes"] += real
        return real

    if PRIMARY_SMALL and size <= SEG:
        pa = S["accs"][PRIMARY_AID]
        for attempt in range(3):
            if pa["status"] != "ok" or pa["paused"]:
                break
            cur_state["cur"] = None
            CUR_OWNER[0] = bytearray([pa["idx"] + 1])
            dl.update(done=0, size=size, nseg=1, segs_done=0)
            base = [0]

            def cb(rec, tot):
                if pa["paused"]:
                    raise _AccPaused()
                d = rec - base[0]
                base[0] = rec
                pa["bytes"] += d
                dl["done"] += d

            t = asyncio.create_task(PRIMARY.download_media(msg, file=path, progress_callback=cb))
            CUR_TASKS[:] = [t]
            try:
                await t
                CUR_TASKS.clear()
                real = os.path.getsize(path)
                if real != size:
                    raise RuntimeError(f"size mismatch {real}/{size}")
                dl.update(done=real, segs_done=1)
                await throttle(PRIMARY_AID, real)
                return real
            except _AccPaused:
                CUR_TASKS.clear()
                pa["bytes"] -= base[0]
                dl["done"] -= base[0]
                break
            except asyncio.CancelledError:
                CUR_TASKS.clear()
                if S["stop"]:
                    return None
                raise
            except Exception as e:
                CUR_TASKS.clear()
                pa["bytes"] -= base[0]
                dl["done"] -= base[0]
                if isinstance(e, FLOOD):
                    flood_mark(pa, e.seconds)
                    break
                if is_dead(e):
                    acc_dead(pa, e)
                    break
                pa["errors"] += 1
                pa["last_err"] = str(e)[:90]
                log(f"[{pa['name']}] chhoti file retry {attempt + 1}: {e}", "warn")
                await asyncio.sleep(1 + attempt)
        dl.update(done=0)

    nseg = max(1, (size + SEG - 1) // SEG)
    bm = bytearray((nseg + 7) // 8)
    owner = bytearray(nseg)
    c = cur_state["cur"]
    same_ent = (c.get("ent") == eid) if (c and "ent" in c) else True
    if c and c["id"] == msg.id and same_ent and c["size"] == size and os.path.exists(path) \
            and os.path.getsize(path) == size and len(c["segs"]) == len(bm) * 2:
        bm = bytearray.fromhex(c["segs"])
        for i in range(nseg):
            if bm_get(bm, i):
                owner[i] = 255
        log(f"resume: {sum(1 for i in range(nseg) if bm_get(bm, i))}/{nseg} segments pehle se disk par hain", "ok")
    else:
        with open(path, "wb") as f:
            f.truncate(size)
    cur_state["cur"] = {"id": msg.id, "ent": eid, "size": size, "segs": bm.hex()}
    CUR_OWNER[0] = owner

    q = asyncio.Queue()
    for i in range(nseg):
        if not bm_get(bm, i):
            q.put_nowait((i, 0))
    remaining = [q.qsize()]
    dl.update(done=sum(min(SEG, size - i * SEG) for i in range(nseg) if bm_get(bm, i)),
              size=size, nseg=nseg, segs_done=nseg - remaining[0])
    fh = open(path, "r+b")
    wlock = asyncio.Lock()
    fatal = []
    t0 = time.time()

    async def worker(aid, client):
        a = S["accs"][aid]
        m = None

        async def fetch():
            peer = await get_peer(client, eid, ch_spec)
            r = await client.get_messages(peer, ids=msg.id)
            if not r or not r.media:
                raise RuntimeError("msg nahi mila (account channel me hai?)")
            return r

        while remaining[0] > 0 and not S["stop"] and not fatal and a["status"] != "dead":
            if a["status"] == "flood" or a["paused"]:
                await asyncio.sleep(0.5)
                continue
            if m is None:
                fetched = False
                for attempt in range(6):
                    if S["stop"] or fatal or a["paused"] or a["status"] == "dead":
                        break
                    try:
                        m = await fetch()
                        fetched = True
                        break
                    except FLOOD as e:
                        await acc_flood(a, e.seconds)
                    except Exception as e:
                        if is_dead(e):
                            acc_dead(a, e)
                            return
                        a["errors"] += 1
                        a["last_err"] = str(e)[:90]
                        log(f"[{a['name']}] msg fetch fail: {e}", "err")
                        await asyncio.sleep(2)
                if a["paused"]:
                    continue
                if not fetched or m is None:
                    log(f"[{a['name']}] is file me madad nahi kar paya - skip", "warn")
                    return
            try:
                i, tries = q.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.3)
                continue
            start = i * SEG
            end = min(size, start + SEG)
            pos = start
            try:
                async for chunk in client.iter_download(
                        m.media, offset=start, limit=(end - start + REQ - 1) // REQ,
                        request_size=REQ, file_size=size):
                    if a["paused"]:
                        raise _AccPaused()
                    if S["stop"]:
                        return
                    chunk = bytes(chunk)[: end - pos]
                    async with wlock:
                        fh.seek(pos)
                        fh.write(chunk)
                    pos += len(chunk)
                    a["bytes"] += len(chunk)
                    dl["done"] += len(chunk)
                    await throttle(aid, len(chunk))
                    if pos >= end:
                        break
                if pos < end:
                    raise RuntimeError("short read")
                async with wlock:
                    fh.flush()
                    await asyncio.to_thread(os.fsync, fh.fileno())
                bm_set(bm, i)
                owner[i] = S["accs"][aid]["idx"] + 1
                cur_state["cur"]["segs"] = bm.hex()
                a["segs"] += 1
                remaining[0] -= 1
                dl["segs_done"] += 1
            except _AccPaused:
                got = pos - start
                a["bytes"] -= got
                dl["done"] -= got
                q.put_nowait((i, tries))
                continue
            except Exception as e:
                got = pos - start
                a["bytes"] -= got
                dl["done"] -= got
                if isinstance(e, FLOOD):
                    q.put_nowait((i, tries))
                    await acc_flood(a, e.seconds)
                elif is_dead(e):
                    q.put_nowait((i, tries))
                    acc_dead(a, e)
                    return
                else:
                    a["errors"] += 1
                    a["last_err"] = str(e)[:90]
                    if "FILE_REFERENCE" in str(e).upper() or "FileReference" in type(e).__name__:
                        try:
                            m = await fetch()
                        except Exception:
                            pass
                    if tries >= 6:
                        fatal.append(f"seg {i}: {e}")
                        return
                    log(f"[{a['name']}] seg {i} retry {tries + 1} ({type(e).__name__}: {e})", "warn")
                    q.put_nowait((i, tries + 1))
                    await asyncio.sleep(1 + tries)

    tasks = []
    for aid, cl in CLIENTS.items():
        if S["accs"][aid]["status"] == "dead":
            continue
        for _ in range(S["accs"][aid]["conn"]):
            tasks.append(asyncio.create_task(worker(aid, cl)))
    CUR_TASKS[:] = tasks
    res = await asyncio.gather(*tasks, return_exceptions=True)
    CUR_TASKS.clear()
    for r in res:
        if isinstance(r, Exception):
            log(f"worker error: {type(r).__name__}: {r}", "err")
    fh.close()
    if S["stop"]:
        return None
    if fatal or remaining[0] > 0:
        raise RuntimeError("download incomplete: " + ("; ".join(fatal[:2]) or f"{remaining[0]} segments baaki (accounts khatam)"))
    log(f"download done {fmt(size)} in {time.time() - t0:.0f}s", "ok")
    return size


# ------------------------------------------------------------------ drive service & folder API
def drive_service():
    creds = get_drive_creds()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def ensure_drive_path_sync(root_folder, folder_parts):
    """Google Drive me root_folder ke andar nested folder_parts (jaise ['Batch', 'Subject', 'Ch 01', 'NOTES'])
    ko dhoondhta ya create karta hai (duplicate nahi banata, cache use karta hai)."""
    cur_parent = (root_folder or "").strip() or "root"
    if not folder_parts:
        return "" if cur_parent == "root" else cur_parent
    svc = None
    with FOLDER_LOCK:
        for raw_seg in folder_parts:
            seg = clean_drive_segment(raw_seg)
            if not seg:
                continue
            key = (cur_parent, seg)
            if key in FOLDER_CACHE:
                cur_parent = FOLDER_CACHE[key]
                continue
            if svc is None:
                svc = drive_service()
            safe_q = seg.replace("\\", "\\\\").replace("'", "\\'")
            q = (f"mimeType = 'application/vnd.google-apps.folder' and trashed = false "
                 f"and name = '{safe_q}' and '{cur_parent}' in parents")
            res = svc.files().list(
                q=q, spaces="drive", fields="files(id,name)", pageSize=5,
                includeItemsFromAllDrives=True, supportsAllDrives=True,
            ).execute()
            found = res.get("files", [])
            if found:
                fid = found[0]["id"]
            else:
                meta = {"name": seg, "mimeType": "application/vnd.google-apps.folder"}
                if cur_parent != "root":
                    meta["parents"] = [cur_parent]
                created = svc.files().create(body=meta, fields="id,name", supportsAllDrives=True).execute()
                fid = created["id"]
                log(f"Drive subfolder banaya: {seg}", "ok")
            FOLDER_CACHE[key] = fid
            cur_parent = fid
    return "" if cur_parent == "root" else cur_parent


def drive_list_folders_sync(parent="root", scope="mydrive", search=""):
    """Google Drive ke folders list karta hai (My Drive, Shared with me, Shared Drives, ya Search)."""
    svc = drive_service()
    if scope == "drives" and (not parent or parent == "drives_root"):
        res = svc.drives().list(pageSize=50, fields="drives(id,name)").execute()
        drives = [{"id": d["id"], "name": d["name"], "isDrive": True} for d in res.get("drives", [])]
        return {"parent": {"id": "drives_root", "name": "Shared Drives"}, "folders": drives}

    q_parts = ["mimeType = 'application/vnd.google-apps.folder'", "trashed = false"]
    if search:
        safe_q = search.replace("\\", "\\\\").replace("'", "\\'")
        q_parts.append(f"name contains '{safe_q}'")
    elif scope == "shared" and (not parent or parent == "shared_root"):
        q_parts.append("sharedWithMe = true")
    else:
        pid = parent or "root"
        q_parts.append(f"'{pid}' in parents")

    res = svc.files().list(
        q=" and ".join(q_parts),
        spaces="drive",
        fields="files(id,name,modifiedTime)",
        orderBy="name_natural",
        pageSize=100,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    folders = res.get("files", [])

    p_info = {"id": parent or "root", "name": "My Drive"}
    if search:
        p_info = {"id": parent or "root", "name": f"Search: {search}"}
    elif scope == "shared" and (not parent or parent == "shared_root"):
        p_info = {"id": "shared_root", "name": "Shared with me"}
    elif parent and parent not in ("root", "shared_root", "drives_root"):
        try:
            meta = svc.files().get(fileId=parent, fields="id,name", supportsAllDrives=True).execute()
            p_info = {"id": meta["id"], "name": meta["name"]}
        except Exception:
            pass
    return {"parent": p_info, "folders": folders}


def drive_create_folder_sync(name, parent="root"):
    """Google Drive me naya folder banata hai."""
    svc = drive_service()
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent and parent not in ("root", "shared_root", "drives_root"):
        meta["parents"] = [parent]
    f = svc.files().create(body=meta, fields="id,name", supportsAllDrives=True).execute()
    return {"id": f["id"], "name": f["name"]}


def drive_folder_info_sync(fid):
    if not fid or fid == "root":
        return {"id": "", "name": "My Drive (Root)"}
    svc = drive_service()
    f = svc.files().get(fileId=fid, fields="id,name,mimeType", supportsAllDrives=True).execute()
    return {"id": f["id"], "name": f["name"]}


class _Body:
    """File-like jo bhejte waqt live progress count karta hai (upload speed ke liye)."""
    def __init__(self, f, n, ul):
        self.f, self.left, self.n, self.ul = f, n, n, ul

    def __len__(self):
        return self.n

    def read(self, size=-1):
        if S["stop"]:
            raise RuntimeError("stopped")
        if self.left <= 0:
            return b""
        size = self.left if (size is None or size < 0) else min(size, self.left)
        b = self.f.read(min(size, 65536))
        self.left -= len(b)
        self.ul["done"] += len(b)
        return b


UP_CHUNK = 8 * 1024 * 1024        # 256 KB ka multiple


def _next_pos(r):
    rng = r.headers.get("Range")
    return int(rng.split("-")[1]) + 1 if rng else 0


def upload_blocking(path, name, folder, ul):
    from google.auth.transport.requests import AuthorizedSession
    creds = get_drive_creds()
    sess = AuthorizedSession(creds)
    total = os.path.getsize(path)
    meta = {"name": name}
    if folder and folder != "root":
        meta["parents"] = [folder]
    r = sess.post("https://www.googleapis.com/upload/drive/v3/files",
                  params={"uploadType": "resumable", "supportsAllDrives": "true", "fields": "id,name"},
                  json=meta, headers={"X-Upload-Content-Length": str(total)}, timeout=60)
    if r.status_code >= 400:
        if r.status_code == 401:
            S["drive_ok"] = False
            S["drive_expired"] = True
            S["drive_err"] = f"drive init 401: {r.text[:160]}"
        raise RuntimeError(f"drive init {r.status_code}: {r.text[:200]}")
    loc = r.headers["Location"]
    ul["done"] = 0
    if total == 0:
        r = sess.put(loc, data=b"", headers={"Content-Length": "0"}, timeout=60)
        return r.json()
    pos, fails = 0, 0
    with open(path, "rb") as f:
        while True:
            if S["stop"]:
                return None
            end = min(total, pos + UP_CHUNK)
            f.seek(pos)
            ul["done"] = pos
            code, err, r = -1, "", None
            try:
                r = sess.put(loc, data=_Body(f, end - pos, ul),
                             headers={"Content-Range": f"bytes {pos}-{end - 1}/{total}",
                                      "Content-Length": str(end - pos)}, timeout=(30, 600))
                code = r.status_code
            except Exception as e:
                if S["stop"]:
                    return None
                if is_token_error(e):
                    S["drive_ok"] = False
                    S["drive_expired"] = True
                    S["drive_err"] = str(e)[:200]
                    raise
                err = str(e)[:120]
            if code in (200, 201):
                ul["done"] = total
                return r.json()
            if code == 308:
                pos, fails = _next_pos(r), 0
                continue
            if code == -1 or code in (408, 429, 500, 502, 503, 504):
                fails += 1
                if fails > 8:
                    raise RuntimeError(f"drive upload fail ({code}) {err}")
                log(f"upload retry {fails}: {code} {err}", "warn")
                time.sleep(min(30, 2 * fails))
                try:
                    q = sess.put(loc, headers={"Content-Range": f"bytes */{total}", "Content-Length": "0"}, timeout=60)
                    if q.status_code in (200, 201):
                        ul["done"] = total
                        return q.json()
                    if q.status_code == 308:
                        pos = _next_pos(q)
                except Exception:
                    pass
                continue
            if code == 401:
                S["drive_ok"] = False
                S["drive_expired"] = True
                S["drive_err"] = f"drive upload 401: {r.text[:160]}"
            raise RuntimeError(f"drive upload {code}: {r.text[:200]}")


def find_7z_bin():
    """System me native 7z binary dhoondhta hai (Windows + Linux)."""
    for cand in ("7z", "7zz", "7za", r"C:\Program Files\7-Zip\7z.exe", r"C:\Program Files (x86)\7-Zip\7z.exe"):
        p = shutil.which(cand) if not os.path.isabs(cand) else (cand if os.path.exists(cand) else None)
        if p:
            return p
    return None


def encrypt_to_7z_sync(src_path, orig_name, password):
    """File ko 7z Store Mode (no compression) + AES-256 password + header encryption me pack karta hai.
    Hybrid: pehle native 7z CLI (-mx=0 -mhe=on), na milne par py7zr (FILTER_COPY)."""
    out_path = src_path + ".7z"
    if os.path.exists(out_path):
        try:
            os.remove(out_path)
        except OSError:
            pass
    # Archive ke andar ka naam Windows/Linux safe rakho taaki extract karte waqt error na aaye
    inner_name = safe_name(orig_name) or "file"
    seven_z = find_7z_bin()
    if seven_z:
        tmp_dir = src_path + "_enc_tmp"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir, exist_ok=True)
        link_path = os.path.join(tmp_dir, inner_name)
        moved = False
        try:
            try:
                os.link(src_path, link_path)
            except OSError:
                shutil.move(src_path, link_path)
                moved = True
            cmd = [seven_z, "a", "-t7z", "-mx=0", "-mhe=on", "-y", f"-p{password}", out_path, link_path]
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode != 0:
                raise RuntimeError(f"7z CLI error: {(r.stderr or r.stdout)[:180]}")
        finally:
            if moved and not os.path.exists(src_path) and os.path.exists(link_path):
                try:
                    shutil.move(link_path, src_path)
                except OSError:
                    pass
            shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        import py7zr
        filters = [{"id": py7zr.FILTER_COPY}]
        with py7zr.SevenZipFile(out_path, "w", password=password, filters=filters) as zf:
            zf.set_encrypted_header(True)
            zf.write(src_path, arcname=inner_name)
    up_name = orig_name if orig_name.lower().endswith(".7z") else (orig_name + ".7z")
    return out_path, up_name, os.path.getsize(out_path)


async def upload_file(msg_id, path, name, size, folder_parts=None, index_id=""):
    target_folder = S["folder"]
    if folder_parts:
        target_folder = await asyncio.to_thread(ensure_drive_path_sync, S["folder"], folder_parts)
    ul = {"id": msg_id, "name": name, "size": size, "done": 0, "speed": 0,
          "folder": "/".join(folder_parts) if folder_parts else "", "index_id": index_id}
    S["ul"] = ul
    try:
        r = await asyncio.to_thread(upload_blocking, path, name, target_folder, ul)
    finally:
        S["ul"] = None
    if r:
        prefix = f"{'/'.join(folder_parts)}/" if folder_parts else ""
        itag = f" [INDEX #{index_id}]" if index_id else ""
        log(f"Drive OK: {prefix}{name}{itag}", "ok")
    return bool(r)


# ------------------------------------------------------------------ job (channel mode + file list + csv mode)
async def scan_channel(ent, min_id):
    S["phase"] = "scanning"
    items, cursor, total = [], min_id, 0
    while not S["stop"]:
        cur = cursor
        batch = await flood_safe(lambda: PRIMARY.get_messages(ent, limit=100, min_id=cur, reverse=True), "primary")
        if not batch:
            break
        for m in batch:
            if m.media and m.file and m.file.size:
                items.append((m.id, m.file.size))
                total += m.file.size
        cursor = max(m.id for m in batch)
        S["scan"] = {"files": len(items), "bytes": total}
        if len(batch) < 100:
            break
    return items


async def scan_queue_list(queue):
    """File list / CSV entries ko scan karke downloadable items banata hai."""
    S["phase"] = "scanning"
    S["scan"] = {"files": 0, "bytes": 0}
    ents = {}
    titles = []
    unique_specs = []
    for it in queue:
        ch = it["channel"]
        if ch not in ents:
            ent = await flood_safe(lambda c=ch: resolve_channel(PRIMARY, c, "primary"), "primary")
            ents[ch] = ent
            unique_specs.append(ch)
            t = getattr(ent, "title", "") or str(ent.id)
            if t not in titles:
                titles.append(t)
    tag = f"CSV ({S['csv_staged'].get('filename') or 'batch'})" if S["mode"] == "csv" else "List"
    S["channel_title"] = f"{tag} · {', '.join(titles[:2])}{'…' if len(titles) > 2 else ''}"

    for ch in unique_specs:
        ent = ents[ch]
        ch_title = getattr(ent, "title", "") or str(ent.id)
        for aid, cl in CLIENTS.items():
            a = S["accs"][aid]
            if cl is PRIMARY or a["status"] == "dead":
                continue
            try:
                await flood_safe(lambda c=cl, s=ch, e=ent.id, t=ch_title, n=a["name"]:
                                 resolve_channel(c, s, n, known_ent_id=e, known_title=t), a["name"])
            except Exception as e:
                if is_dead(e):
                    acc_dead(a, e)
                else:
                    a["errors"] += 1
                    a["last_err"] = str(e)[:90]
                    log(f"[{a['name']}] channel resolve fail ({ch}): {e}", "err")

    done_set = set(S["done_keys"])
    seen = set()
    wanted_runs = []
    use_path = S["csv_opts"].get("use_path", True) if S["mode"] == "csv" else False
    use_title = S["csv_opts"].get("use_title", True) if S["mode"] == "csv" else False

    for it in queue:
        ch = it["channel"]
        ent = ents[ch]
        if it["kind"] in ("link", "csv"):
            cname = (it.get("name") or "") if (it["kind"] == "csv" and use_title) else ""
            fparts = (it.get("folder_parts") or []) if (it["kind"] == "csv" and use_path) else []
            idx_id = str(it.get("index_id") or "") if it["kind"] == "csv" else ""
            entries = [(it["msg_id"], cname, fparts, idx_id)]
        else:
            entries = [(mid, "", [], "") for mid in range(it["start_id"], it["end_id"] + 1)]
        filtered = []
        for mid, cname, fparts, idx_id in entries:
            key = f"{ent.id}:{mid}"
            if key in seen or key in done_set:
                continue
            seen.add(key)
            filtered.append((mid, cname, fparts, idx_id))
        if not filtered:
            continue
        if wanted_runs and wanted_runs[-1][0] == ch:
            wanted_runs[-1][1].extend(filtered)
        else:
            wanted_runs.append((ch, filtered))

    items, total = [], 0
    for ch, entries in wanted_runs:
        if S["stop"]:
            break
        ent = ents[ch]
        for i in range(0, len(entries), 100):
            if S["stop"]:
                break
            chunk = entries[i:i + 100]
            batch_ids = [x[0] for x in chunk]
            msgs = await flood_safe(lambda e=ent, b=batch_ids: PRIMARY.get_messages(e, ids=b), "primary")
            if not isinstance(msgs, list):
                msgs = [msgs]
            for idx_m, m in enumerate(msgs):
                _, cname, fparts, idx_id = chunk[idx_m]
                if m and m.media and m.file and m.file.size:
                    items.append({
                        "ch_spec": ch, "ent": ent, "ent_id": ent.id,
                        "msg_id": m.id, "size": m.file.size, "key": f"{ent.id}:{m.id}",
                        "custom_name": cname, "folder_parts": fparts, "index_id": idx_id,
                    })
                    total += m.file.size
            S["scan"] = {"files": len(items), "bytes": total}
    return items


async def run_once():
    S.update(stop=False, failed=False, status="running", phase="starting")
    upq = asyncio.Queue(maxsize=1)
    up_task = None
    try:
        c = cur_state["cur"]
        keep_names = set()
        if c:
            keep_names.add(f"{c['id']}.part")
            if c.get("ent") is not None:
                keep_names.add(f"{c['ent']}_{c['id']}.part")
        for fn in os.listdir(DATA_DIR):
            full_p = os.path.join(DATA_DIR, fn)
            if (fn.endswith(".part") and fn not in keep_names) or fn.endswith(".part.7z"):
                try:
                    os.remove(full_p)
                except OSError:
                    pass
            elif fn.endswith("_enc_tmp") and os.path.isdir(full_p):
                shutil.rmtree(full_p, ignore_errors=True)

        if S["mode"] in ("list", "csv"):
            if not S["queue"]:
                raise RuntimeError("Queue khaali hai — pehle links, range ya CSV upload karo")
            log(f"{S['mode']} mode: {len(S['queue'])} entries scan ho rahi hain", "ok")
            list_items = await scan_queue_list(S["queue"])
            S["plan"] = {"files": len(list_items), "bytes": sum(x["size"] for x in list_items),
                         "dl_files": 0, "dl_bytes": 0, "up_files": 0, "up_bytes": 0}
            S["started_at"] = time.time()
            S["ended_at"] = 0
            log(f"{S['mode']} scan done: {len(list_items)} files, {fmt(S['plan']['bytes'])}", "ok")
        else:
            ent = await flood_safe(lambda: resolve_channel(PRIMARY, S["channel"], "primary"), "primary")
            S["_ent_id"] = ent.id
            S["channel_title"] = getattr(ent, "title", "") or str(ent.id)
            for aid, cl in CLIENTS.items():
                a = S["accs"][aid]
                if cl is PRIMARY or a["status"] == "dead":
                    continue
                try:
                    await flood_safe(lambda c=cl, n=a["name"]: resolve_channel(c, S["channel"], n), a["name"])
                except Exception as e:
                    if is_dead(e):
                        acc_dead(a, e)
                    else:
                        a["errors"] += 1
                        a["last_err"] = str(e)[:90]
                        log(f"[{a['name']}] channel resolve fail: {e}", "err")
            min_id = max(S["last_done"], S["start_id"] - 1 if S["start_id"] else 0)
            log(f"channel: {S['channel_title']}  | {min_id} ke baad se scan", "ok")
            raw_items = await scan_channel(ent, min_id)
            list_items = [{
                "ch_spec": S["channel"], "ent": ent, "ent_id": ent.id,
                "msg_id": mid, "size": sz, "key": f"{ent.id}:{mid}",
                "custom_name": "", "folder_parts": [], "index_id": "",
            } for mid, sz in raw_items]
            S["plan"] = {"files": len(list_items), "bytes": sum(x["size"] for x in list_items),
                         "dl_files": 0, "dl_bytes": 0, "up_files": 0, "up_bytes": 0}
            S["started_at"] = time.time()
            S["ended_at"] = 0
            log(f"scan done: {len(list_items)} files, {fmt(S['plan']['bytes'])}", "ok")

        async def uploader():
            while True:
                it = await upq.get()
                if it is None:
                    return
                msg_id, path, name, size, key, folder_parts, index_id = it
                enc_path = None
                try:
                    up_path, up_name, up_size = path, name, size
                    if S["enc_enabled"] and S["enc_pass"]:
                        S["phase"] = "encrypting (7z)"
                        log(f"7z encrypt (store): {name}")
                        enc_path, up_name, up_size = await asyncio.to_thread(
                            encrypt_to_7z_sync, path, name, S["enc_pass"]
                        )
                        up_path = enc_path
                        try:
                            os.remove(path)
                        except OSError:
                            pass
                        S["phase"] = "transferring"
                    ok = await upload_file(msg_id, up_path, up_name, up_size, folder_parts=folder_parts, index_id=index_id)
                    if ok:
                        for p_del in (up_path, path):
                            if p_del and os.path.exists(p_del):
                                try:
                                    os.remove(p_del)
                                except OSError:
                                    pass
                        if S["mode"] in ("list", "csv"):
                            if key not in S["done_keys"]:
                                S["done_keys"].append(key)
                            if index_id:
                                S["last_index_id"] = index_id
                        else:
                            S["last_done"] = max(S["last_done"], msg_id)
                        S["done_count"] += 1
                        S["total_bytes"] += up_size
                        S["plan"]["up_files"] += 1
                        S["plan"]["up_bytes"] += size
                        cur = cur_state["cur"]
                        if cur and cur["id"] == msg_id:
                            cur_state["cur"] = None
                        await save_state(True)
                except Exception as e:
                    if enc_path and os.path.exists(enc_path):
                        try:
                            os.remove(enc_path)
                        except OSError:
                            pass
                    log(f"UPLOAD FAIL {name}: {e}", "err")
                    S["failed"] = True
                    S["stop"] = True

        up_task = asyncio.create_task(uploader())
        S["phase"] = "transferring"
        await save_state(True)

        idx = 0
        n_total = len(list_items)
        while idx < n_total and not S["stop"]:
            ch_spec = list_items[idx]["ch_spec"]
            ent = list_items[idx]["ent"]
            end_idx = idx
            while end_idx < n_total and end_idx - idx < 50 and list_items[end_idx]["ch_spec"] == ch_spec:
                end_idx += 1
            chunk = list_items[idx:end_idx]
            grp_ids = [x["msg_id"] for x in chunk]
            msgs = await flood_safe(lambda e=ent, g=grp_ids: PRIMARY.get_messages(e, ids=g), "primary")
            if not isinstance(msgs, list):
                msgs = [msgs]
            for k, msg in enumerate(msgs):
                if S["stop"]:
                    break
                meta = chunk[k]
                if not msg or not msg.media or not msg.file or not msg.file.size:
                    S["plan"]["files"] = max(0, S["plan"]["files"] - 1)
                    continue
                size = msg.file.size
                name = drive_name(msg, custom_name=meta.get("custom_name", ""))
                part_name = f"{meta['ent_id']}_{msg.id}.part" if S["mode"] in ("list", "csv") else f"{msg.id}.part"
                path = os.path.join(DATA_DIR, part_name)
                pos = idx + k
                S["next"] = [{"id": x["msg_id"], "size": x["size"], "name": x.get("custom_name", ""), "index_id": x.get("index_id", "")}
                             for x in list_items[pos + 1:pos + 6]]
                S["dl"] = {"id": msg.id, "name": name, "size": size, "done": 0, "speed": 0,
                           "nseg": 0, "segs_done": 0, "start": time.time(),
                           "folder": "/".join(meta.get("folder_parts") or []),
                           "index_id": meta.get("index_id", "")}
                itag = f" [INDEX #{meta['index_id']}]" if meta.get("index_id") else ""
                log(f"#{msg.id}{itag}  {name}  ({fmt(size)})")
                real = await download_msg(msg, size, path, ent_id=meta["ent_id"], channel_spec=ch_spec)
                if real is None:
                    break
                S["plan"]["dl_files"] += 1
                S["plan"]["dl_bytes"] += real
                S["dl"] = None
                await upq.put((msg.id, path, name, real, meta["key"], meta.get("folder_parts") or [], meta.get("index_id", "")))
            idx = end_idx

        S["next"] = []
        S["status"] = "stopped" if S["stop"] else "finished"
    except BaseException as e:
        if S["stop"] and not S["failed"]:
            S["status"] = "stopped"
        elif isinstance(e, Exception):
            log(f"JOB ERROR: {type(e).__name__}: {e}", "err")
            S["failed"] = True
            S["status"] = "error"
        else:
            raise
    finally:
        S["dl"] = None
        if up_task is not None:
            try:
                await upq.put(None)
                await up_task
            except Exception:
                pass
        if S["failed"]:
            S["status"] = "error"
        S["phase"] = {"finished": "complete", "stopped": "stopped"}.get(S["status"], "")
        S["ended_at"] = time.time()
        for a in S["accs"].values():
            a["speed"] = 0
        await save_state(True)


async def run_job():
    """Error aane par khud retry (state se resume), 5 baar bina progress ke fail ho to ruk jaata hai."""
    bad = 0
    while True:
        before = S["done_count"]
        await run_once()
        if S["status"] in ("finished", "stopped"):
            if S["status"] == "finished":
                log("sab files ho gayi", "ok")
            return
        if S["drive_expired"]:
            log("Google Drive token expire/invalid ho gaya — UI se 'Login Google Drive' karke wapis Start dabao", "err")
            return
        bad = 0 if S["done_count"] > before else bad + 1
        if bad >= 5:
            log("5 baar lagataar fail - ruk gaya, error dekh ke Start dabao", "err")
            return
        log(f"20s baad auto-retry ({bad}/5)", "warn")
        S["status"] = "running"
        S["phase"] = "retry wait"
        for _ in range(20):
            if S["stop"]:
                S["status"] = "stopped"
                return
            await asyncio.sleep(1)


# ------------------------------------------------------------------ speed / stats loop
async def speed_loop():
    last_t = time.time()
    last = {}
    last_up = [None, 0]
    while True:
        await asyncio.sleep(1)
        now = time.time()
        dt = max(0.2, now - last_t)
        last_t = now
        tot = 0
        for k, v in S["accs"].items():
            sp = (v["bytes"] - last.get(k, v["bytes"])) / dt
            last[k] = v["bytes"]
            if v.get("paused") or v["status"] == "dead":
                v["speed"] = 0
            else:
                v["speed"] = max(0, 0.5 * v["speed"] + 0.5 * sp)
                if v["speed"] < 1:
                    v["speed"] = 0
            tot += v["speed"]
        S["speed"] = tot
        S["hist"].append(int(tot))
        if S["dl"]:
            S["dl"]["speed"] = tot
            S["dl"]["map"] = CUR_OWNER[0].hex()
        u = S["ul"]
        if u:
            if last_up[0] != u["id"]:
                last_up[0], last_up[1] = u["id"], u["done"]
            u["speed"] = max(0.0, 0.5 * u["speed"] + 0.5 * (u["done"] - last_up[1]) / dt)
            last_up[1] = u["done"]


def status_json():
    now = time.time()
    p = S["plan"]
    dl = S["dl"]
    elapsed = 0
    if S["started_at"]:
        elapsed = (S["ended_at"] or now) - S["started_at"]
    dl_total = p["dl_bytes"] + (dl["done"] if dl else 0)
    avg = dl_total / elapsed if elapsed > 3 else 0
    rem_bytes = max(0, p["bytes"] - dl_total)
    h = list(S["hist"])[-60:]
    recent = sum(h) / len(h) if h else 0
    rate = (avg + recent) / 2 if avg > 0 and recent > 0 else (avg or recent)
    eta = rem_bytes / rate if rate > 0 and S["status"] == "running" else 0
    accs = []
    alive_cnt = 0
    paused_cnt = 0
    for aid, a in S["accs"].items():
        if a["status"] != "dead":
            alive_cnt += 1
            if a.get("paused"):
                paused_cnt += 1
        accs.append({**a, "id": aid, "flood_left": max(0, int(a["flood_until"] - now)) if a["status"] == "flood" else 0})
    all_paused = alive_cnt > 0 and paused_cnt == alive_cnt
    return {
        "status": S["status"], "phase": ("all accounts paused" if (S["status"] == "running" and all_paused) else S["phase"]),
        "mode": S["mode"],
        "stopping": bool(S["stop"] and S["status"] == "running"),
        "all_paused": all_paused,
        "channel": S["channel"], "folder": S["folder"], "folder_name": S["folder_name"],
        "queue": S["queue"] if S["mode"] == "list" else [],
        "csv_info": csv_summary_view(S["csv_staged"]) if S["csv_staged"].get("items") else None,
        "csv_opts": S["csv_opts"],
        "channel_title": S["channel_title"], "last_done": S["last_done"], "last_index_id": S["last_index_id"],
        "done_count": S["done_count"], "total_bytes": S["total_bytes"],
        "scan": S["scan"], "plan": p, "elapsed": elapsed, "avg": avg, "eta": eta,
        "speed": S["speed"], "rem_files": max(0, p["files"] - p["up_files"]),
        "dl": dl, "ul": S["ul"], "next": S["next"], "accs": accs,
        "hist": list(S["hist"]), "log": list(S["log"]),
        "state_ago": (now - S["state_ts"]) if S["state_ts"] else -1,
        "state_file": STATE_FILE_NAME,
        "state_file_id": S["state_file_id"],
        "drive_ok": S["drive_ok"],
        "drive_expired": S["drive_expired"],
        "drive_email": S["drive_email"],
        "drive_err": S["drive_err"],
        "cryptg": HAS_CRYPTG,
        "enc_enabled": S["enc_enabled"],
        "enc_pass": S["enc_pass"],
        "enc_engine": "7z CLI" if find_7z_bin() else "py7zr",
    }


# ------------------------------------------------------------------ web UI
HTML = r"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover,maximum-scale=1">
<meta name=theme-color content="#0a0e16"><meta name=mobile-web-app-capable content=yes>
<meta name=apple-mobile-web-app-capable content=yes><meta name=apple-mobile-web-app-status-bar-style content=black-translucent>
<meta name=apple-mobile-web-app-title content="TG→Drive">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%230a0e16'/%3E%3Cpath d='M8 19l8-8 8 8M16 11v13' stroke='%234f9cff' stroke-width='2.6' fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<title>TG → Drive</title><style>
:root{--bg:#0a0e16;--card:#111826;--card2:#0d1420;--b:#1c2740;--t:#e8eef8;--m:#8396b5;--ac:#4f9cff;--ok:#2ecc71;--wa:#f5a623;--er:#ff5c6c;--sat:env(safe-area-inset-top,0px);--sab:env(safe-area-inset-bottom,0px);--nav-h:60px}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{margin:0;overflow-x:hidden;height:100%}
html{scroll-padding-top:calc(var(--sat) + 54px)}
body{background:var(--bg);color:var(--t);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding-bottom:calc(var(--nav-h) + var(--sab) + 10px)}
body::before{content:"";position:fixed;inset:0 0 auto;height:var(--sat);background:var(--bg);z-index:30}
.w{max-width:760px;margin:auto;padding:0 12px 28px}
header{position:sticky;top:0;z-index:20;background:rgba(10,14,22,.92);backdrop-filter:blur(10px);padding:calc(var(--sat) + 10px) 12px 10px;margin:0 -12px 12px;border-bottom:1px solid var(--b);display:flex;align-items:center;gap:10px;flex-wrap:wrap}
h1{font-size:17px;margin:0;font-weight:700;letter-spacing:.2px}h1 span{color:var(--m);font-weight:500}
.sub{color:var(--m);font-size:11px;margin-left:auto;text-align:right;line-height:1.5;max-width:46%}
#ch{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pill{display:inline-flex;align-items:center;gap:6px;padding:3px 11px;border-radius:99px;background:#1a2438;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;flex:none}
.pill:before{content:"";width:7px;height:7px;border-radius:50%;background:var(--m)}
.pill.running{background:#10331f;color:var(--ok)}.pill.running:before{background:var(--ok);animation:p 1.2s infinite}
.pill.finished{background:#10331f;color:var(--ok)}.pill.finished:before{background:var(--ok)}
.pill.error{background:#3a1519;color:var(--er)}.pill.error:before{background:var(--er)}
.pill.stopped{background:#3a2d10;color:var(--wa)}.pill.stopped:before{background:var(--wa)}
@keyframes p{50%{opacity:.25}}
#ph{color:var(--m);font-size:11px;flex:none}
.offline{display:none;position:sticky;top:0;z-index:25;background:var(--er);color:#fff;text-align:center;font-size:12px;font-weight:600;padding:6px;margin:0 -12px}
.offline.show{display:block}
.drive-warn{display:none;background:#2f1419;border:1px solid #6e2228;color:#ffd0d5;border-radius:12px;padding:11px 14px;margin-bottom:12px;align-items:center;gap:10px;flex-wrap:wrap}
.drive-warn.show{display:flex}
.drive-warn .dw-txt{flex:1;min-width:180px;font-size:12.5px;line-height:1.4}
.drive-warn .dw-txt b{color:#fff;display:block;font-size:13px}
.drive-warn button{flex:none;padding:8px 13px;font-size:12.5px}
.tabpane{display:none}.tabpane.on{display:block}
.card{background:var(--card);border:1px solid var(--b);border-radius:14px;padding:14px 16px;margin-bottom:12px;min-width:0}
.card h3{white-space:nowrap;gap:10px;margin:0 0 10px;font-size:11px;font-weight:600;color:var(--m);text-transform:uppercase;letter-spacing:.8px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap}
.modebar{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;background:var(--card2);border:1px solid var(--b);border-radius:11px;padding:4px;margin-bottom:12px}
.modebar button{background:none;color:var(--m);padding:9px 8px;font-size:12.5px;border-radius:8px;font-weight:600}
.modebar button.on{background:var(--ac);color:#fff}
.ctl{display:grid;grid-template-columns:1fr;gap:10px}
label{font-size:11.5px;color:var(--m);display:block;margin-bottom:5px}
input,textarea{width:100%;background:var(--card2);border:1px solid var(--b);color:var(--t);border-radius:10px;padding:11px 12px;font-size:15px;font-family:inherit;outline:none}
textarea{resize:vertical;min-height:74px;font-size:13.5px;line-height:1.45}
input:focus,textarea:focus{border-color:var(--ac)}
input[type=number]{font-size:14px}
button{border:0;border-radius:10px;padding:12px 16px;font-weight:600;font-size:14px;cursor:pointer;color:#fff;background:var(--ac);-webkit-user-select:none}
button:active{transform:scale(.97)}
button:disabled{opacity:.4;cursor:not-allowed;transform:none}
button.stop{background:#2a3550}button.stop:not(:disabled):active{background:var(--er)}
button.sec{background:var(--card2);border:1px solid var(--b);color:var(--t)}
button.sec:hover{border-color:var(--ac)}
button.sm{padding:8px 12px;font-size:12.5px;border-radius:8px}
button.xs{padding:5px 10px;font-size:11.5px;border-radius:7px}
button.okbtn{background:#187d44}
button.wabtn{background:#3a2d10;border:1px solid #6e5420;color:var(--wa)}
.btns{display:flex;gap:8px;flex-wrap:wrap}.btns button{flex:1}
.fresh{display:flex;align-items:center;gap:7px;margin:2px 0 0;white-space:nowrap;font-size:12.5px;cursor:pointer}.fresh input{width:auto}
.fo-row{display:flex;gap:8px;align-items:stretch}
.fo-row input{flex:1;min-width:0}
.fo-row button{flex:none;white-space:nowrap}
.fo-badge{display:inline-flex;align-items:center;gap:6px;margin-top:6px;background:#10233d;border:1px solid #244572;color:#9cc7ff;padding:4px 10px;border-radius:8px;font-size:12px;max-width:100%}
.fo-badge span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.fo-badge b{cursor:pointer;color:var(--m);padding-left:4px;font-weight:700}
.fo-badge b:hover{color:var(--er)}
.qgrid{display:grid;grid-template-columns:1fr;gap:10px;margin-bottom:10px}
.qbox{background:var(--card2);border:1px solid var(--b);border-radius:11px;padding:11px 12px}
.qbox h4{margin:0 0 8px;font-size:11.5px;color:var(--m);text-transform:uppercase;letter-spacing:.6px}
.rng-row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.qlist-head{display:flex;justify-content:space-between;align-items:center;margin:10px 0 6px;font-size:12px;color:var(--m)}
.qlist{max-height:210px;overflow:auto;display:flex;flex-direction:column;gap:6px;background:var(--card2);border:1px solid var(--b);border-radius:10px;padding:8px}
.qempty{color:var(--m);font-size:12.5px;text-align:center;padding:14px 6px}
.qitem{display:flex;align-items:center;gap:8px;background:var(--card);border:1px solid var(--b);border-radius:8px;padding:7px 10px;font-size:12.5px}
.qitem .tag{font-size:10px;font-weight:700;padding:2px 7px;border-radius:99px;background:#152540;color:var(--ac);text-transform:uppercase;flex:none}
.qitem .tag.rng{background:#2d2210;color:var(--wa)}
.qitem .qt{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--t)}
.qitem .del{background:none;color:var(--m);padding:2px 7px;font-size:14px;border-radius:6px;flex:none}
.qitem .del:hover{background:#3a1519;color:var(--er)}
.csv-drop{border:1.5px dashed #2a3e66;border-radius:11px;background:var(--card2);padding:18px 14px;text-align:center;cursor:pointer;transition:border-color .2s,background .2s}
.csv-drop:hover,.csv-drop.drag{border-color:var(--ac);background:#101d33}
.csv-drop p{margin:6px 0 0;font-size:13px;color:var(--t);font-weight:600}
.csv-drop small{display:block;margin-top:3px;color:var(--m);font-size:11.5px}
.csv-meta{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
.csv-pill{background:var(--card2);border:1px solid var(--b);border-radius:8px;padding:4px 10px;font-size:12px;color:var(--t)}
.csv-pill b{color:var(--ac)}
.csv-cats{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
.csv-cat{padding:5px 10px;font-size:11.5px;border-radius:8px;background:var(--card2);border:1px solid var(--b);color:var(--m);font-weight:600}
.csv-cat.on{background:#152a4a;border-color:var(--ac);color:#fff}
.csv-idx-row{display:flex;gap:8px;align-items:flex-end;margin-top:10px;flex-wrap:wrap}
.csv-idx-row>div{flex:1;min-width:190px}
.csv-idx-row input{padding:9px 11px;font-size:13.5px}
.csv-last-hint{font-size:11.5px;color:var(--wa);cursor:pointer;padding:6px 10px;border-radius:8px;background:#2d2210;border:1px solid #6e5420;white-space:nowrap}
.csv-opts{display:flex;flex-wrap:wrap;gap:14px;margin-top:10px}
.csv-prev{margin-top:10px;max-height:200px;overflow:auto;background:var(--card2);border:1px solid var(--b);border-radius:10px;padding:6px;display:flex;flex-direction:column;gap:5px}
.csv-row{background:var(--card);border:1px solid var(--b);border-radius:8px;padding:6px 9px;font-size:12px}
.csv-row .ct{font-weight:600;color:var(--t);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.csv-row .cp{font-size:11px;color:var(--m);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:2px}
.st-ed{width:100%;min-height:220px;font:12px/1.55 ui-monospace,Consolas,monospace;background:#080b12;color:#b8c8e2;border:1px solid var(--b);border-radius:9px;padding:10px}
.st-link{color:var(--ac);text-decoration:none;font-size:12px;font-weight:600}
.st-link:hover{text-decoration:underline}
.stats{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-bottom:12px}
.st{background:var(--card);border:1px solid var(--b);border-radius:12px;padding:11px 13px}
.st small{display:block;color:var(--m);font-size:10.5px;text-transform:uppercase;letter-spacing:.6px}
.st b{display:block;font-size:18px;margin-top:2px;font-variant-numeric:tabular-nums}.st i{font-style:normal;color:var(--m);font-size:11px}
.bar{height:9px;background:var(--card2);border-radius:6px;overflow:hidden}.bar>i{display:block;height:100%;width:0;background:var(--ac);border-radius:6px;transition:width .5s}
.bar.g>i{background:var(--ok)}.bar.big{height:12px}
.row{display:flex;justify-content:space-between;gap:10px;color:var(--m);font-size:12px;margin-top:6px;font-variant-numeric:tabular-nums}
.nm{font-weight:600;font-size:13px;color:var(--t);margin-bottom:10px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;word-break:break-word;min-height:2.9em}
.two{display:grid;grid-template-columns:1fr;gap:12px;margin-bottom:12px}.two>.card{margin-bottom:0}
canvas{display:block;width:100%}
#map{margin-top:10px;border-radius:6px}
.ac-head-r{display:inline-flex;align-items:center;gap:6px;flex-wrap:wrap}
.accs{display:grid;grid-template-columns:repeat(auto-fit,minmax(175px,1fr));gap:10px}
.ac{background:var(--card2);border:1px solid var(--b);border-radius:11px;padding:11px 12px;transition:opacity .2s,border-color .2s}
.ac.paused{border-color:#6e5420;opacity:.78}
.ac .h{display:flex;align-items:center;gap:8px;margin-bottom:6px}.dot{width:10px;height:10px;border-radius:3px;flex:none}
.ac .n{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.ac .ph{color:var(--m);font-size:11px}
.chip{margin-left:auto;font-size:10px;font-weight:700;padding:2px 8px;border-radius:99px;text-transform:uppercase;letter-spacing:.4px;background:#10331f;color:var(--ok);white-space:nowrap}
.chip.flood{background:#3a2d10;color:var(--wa)}.chip.dead{background:#3a1519;color:var(--er)}.chip.idle{background:#1a2438;color:var(--m)}.chip.paused{background:#3a2d10;color:var(--wa)}
.ac .sp{font-size:18px;font-weight:700;margin:2px 0 6px;font-variant-numeric:tabular-nums}
.ac .er{color:var(--er);font-size:11px;margin-top:5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ac .act{margin-top:9px}
.ac .act button{width:100%;padding:7px 10px;font-size:12px;border-radius:8px}
.logtools{display:flex;gap:8px;margin-bottom:8px}
.logtools input{flex:1;padding:9px 11px;font-size:13px}
.logtools button{padding:9px 12px;font-size:12px;background:var(--card2);border:1px solid var(--b);color:var(--t)}
#log{height:calc(100vh - 230px);min-height:260px;overflow:auto;background:#080b12;border-radius:9px;padding:9px 11px;font:12px/1.65 ui-monospace,Consolas,monospace;color:#9db0cf}
#log b{color:#5a6c8c;font-weight:400;margin-right:8px}#log .ok{color:var(--ok)}#log .warn{color:var(--wa)}#log .err{color:var(--er)}
#log .hit{background:#1a2438}
.nx{display:flex;gap:8px;flex-wrap:wrap}.nx span{background:var(--card2);border:1px solid var(--b);border-radius:8px;padding:3px 9px;font-size:12px;color:var(--m);max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
nav.tabs{position:fixed;left:0;right:0;bottom:0;z-index:20;display:grid;grid-template-columns:repeat(4,1fr);background:rgba(17,24,38,.96);backdrop-filter:blur(10px);border-top:1px solid var(--b);padding-bottom:var(--sab)}
nav.tabs button{background:none;color:var(--m);border-radius:0;padding:9px 2px 8px;font-size:10.5px;font-weight:600;display:flex;flex-direction:column;align-items:center;gap:3px;letter-spacing:.2px}
nav.tabs button .ic{font-size:18px;line-height:1}
nav.tabs button.on{color:var(--ac)}
nav.tabs button:active{transform:none;opacity:.7}
#toasts{position:fixed;top:calc(var(--sat) + 8px);left:0;right:0;z-index:80;display:flex;flex-direction:column;align-items:center;gap:8px;pointer-events:none}
.toast{background:#111826;border:1px solid var(--b);color:var(--t);padding:10px 16px;border-radius:99px;font-size:13px;font-weight:600;box-shadow:0 6px 20px rgba(0,0,0,.4);max-width:88%;text-align:center}
.toast.ok{border-color:#1d5c38;color:var(--ok)}.toast.err{border-color:#6e2228;color:var(--er)}.toast.warn{border-color:#6e5420;color:var(--wa)}
.modal-bg{display:none;position:fixed;inset:0;background:rgba(5,8,14,.72);backdrop-filter:blur(4px);z-index:60;align-items:center;justify-content:center;padding:14px}
.modal-bg.show{display:flex}
.modal{background:var(--card);border:1px solid var(--b);border-radius:16px;padding:18px;max-width:340px;width:100%}
.modal p{margin:0 0 16px;font-size:14px;color:var(--t)}
.modal .btns button{padding:11px}
.modal.drive-modal{max-width:560px;max-height:88dvh;display:flex;flex-direction:column;gap:10px;padding:16px}
.dm-head{display:flex;align-items:center;justify-content:space-between;gap:10px}
.dm-head h2{margin:0;font-size:16px;font-weight:700}
.dm-close{background:var(--card2);border:1px solid var(--b);color:var(--m);padding:5px 11px;border-radius:8px;font-size:14px}
.dm-scopes{display:flex;gap:6px}
.dm-scopes button{flex:1;padding:7px 8px;font-size:12px;background:var(--card2);border:1px solid var(--b);color:var(--m);border-radius:8px}
.dm-scopes button.on{background:#152a4a;border-color:var(--ac);color:#fff}
.dm-crumb{display:flex;align-items:center;gap:4px;flex-wrap:wrap;font-size:12px;color:var(--m);background:var(--card2);border:1px solid var(--b);border-radius:9px;padding:7px 10px}
.dm-crumb span{cursor:pointer;color:var(--ac);font-weight:600}
.dm-crumb span:last-child{color:var(--t);cursor:default}
.dm-bar{display:flex;gap:6px}
.dm-bar input{flex:1;padding:9px 11px;font-size:13.5px}
.dm-bar button{flex:none;padding:9px 13px;font-size:12.5px}
.dm-list{flex:1;min-height:180px;max-height:320px;overflow:auto;background:var(--card2);border:1px solid var(--b);border-radius:10px;padding:6px;display:flex;flex-direction:column;gap:5px}
.dm-row{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;background:var(--card);border:1px solid transparent;cursor:pointer}
.dm-row:hover{border-color:var(--ac)}
.dm-row .dn{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:500;font-size:13.5px}
.dm-row button{flex:none;padding:5px 11px;font-size:12px;border-radius:7px}
.dm-msg{color:var(--m);text-align:center;padding:24px 10px;font-size:13px}
.dm-err{color:var(--er);text-align:center;padding:18px 10px;font-size:13px}
.dm-foot{display:flex;gap:8px;padding-top:4px;border-top:1px solid var(--b)}
.dm-foot button{flex:1;padding:11px 12px;font-size:13.5px}
.dm-foot button.sec{flex:0 0 auto}
.auth-body{overflow:auto;display:flex;flex-direction:column;gap:10px}
.auth-box{background:var(--card2);border:1px solid var(--b);border-radius:11px;padding:11px 12px}
.auth-box h4{margin:0 0 6px;font-size:11.5px;color:var(--m);text-transform:uppercase;letter-spacing:.6px;display:flex;align-items:center;gap:6px}
.auth-box p{margin:0 0 8px;font-size:12.5px;color:var(--m);line-height:1.45}
.auth-uri{background:#080b12;border:1px solid var(--b);border-radius:7px;padding:6px 9px;font:11.5px/1.4 ui-monospace,Consolas,monospace;color:#9cc7ff;word-break:break-all;margin:6px 0 8px}
.auth-status{display:flex;align-items:center;gap:8px;padding:8px 11px;border-radius:9px;font-size:12.5px;background:var(--card2);border:1px solid var(--b)}
.auth-status.ok{border-color:#1d5c38;color:var(--ok)}.auth-status.bad{border-color:#6e2228;color:var(--er)}
.hide{display:none!important}
.ico{width:15px;height:15px;display:inline-block;flex:none;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.ico-sm{width:13px;height:13px}.ico-lg{width:18px;height:18px}
button{display:inline-flex;align-items:center;justify-content:center;gap:6px}
.dm-head h2,.qbox h4,.card h3>span:first-child{display:inline-flex;align-items:center;gap:6px}
.offline{align-items:center;justify-content:center;gap:6px}.offline.show{display:flex}
.fo-badge b{display:inline-flex;align-items:center}
@media(min-width:640px){.w{max-width:1100px;padding:18px 16px 40px}header{position:static;background:none;backdrop-filter:none;border:0;margin:0 0 14px;padding:0}
.offline{margin:0 0 12px;border-radius:10px}
nav.tabs{display:none}body{padding-bottom:40px}.tabpane{display:block!important}
.sub{max-width:none}.ctl-ch{grid-template-columns:2fr 130px;align-items:end}.qgrid{grid-template-columns:1fr 1fr}.fresh{margin:0}
.stats{grid-template-columns:repeat(6,1fr)}.two{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}
#log{height:230px}}
</style></head><body>
<div id=offlinebar class=offline><svg class="ico" viewBox="0 0 24 24"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg> connection lost — retrying…</div>
<div id=toasts></div>

<div id=stopModal class=modal-bg><div class=modal><p>Running transfer rok dein?<br><span style="color:var(--m);font-size:12.5px">Progress Drive me safe rehta hai, resume kar sakte ho.</span></p>
<div class=btns><button class=stop onclick="closeModal();doStop()"><svg class="ico" viewBox="0 0 24 24"><rect x="5" y="5" width="14" height="14" rx="2" fill="currentColor" stroke="none"/></svg> Stop</button><button onclick=closeModal() class=sec>Cancel</button></div></div></div>

<div id=driveModal class=modal-bg onclick="if(event.target===this)closeDriveModal()">
<div class="modal drive-modal">
  <div class=dm-head><h2><svg class="ico" viewBox="0 0 24 24"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg> Google Drive Folder</h2><button class=dm-close onclick=closeDriveModal() aria-label="Close"><svg class="ico ico-sm" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button></div>
  <div class=dm-scopes>
    <button id=dm_s_mydrive class=on onclick="dmSetScope('mydrive')">My Drive</button>
    <button id=dm_s_shared onclick="dmSetScope('shared')">Shared with me</button>
    <button id=dm_s_drives onclick="dmSetScope('drives')">Shared Drives</button>
  </div>
  <div id=dm_crumb class=dm-crumb></div>
  <div class=dm-bar>
    <input id=dm_search placeholder="Folder search karo…" onkeydown="if(event.key==='Enter')dmLoad()">
    <button class=sec onclick=dmLoad() aria-label="Search"><svg class="ico" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg></button>
  </div>
  <div class=dm-bar>
    <input id=dm_newname placeholder="Naya folder naam…" onkeydown="if(event.key==='Enter')dmCreate()">
    <button id=dm_btn_mk class=okbtn onclick=dmCreate()>+ Create</button>
  </div>
  <div id=dm_list class=dm-list><div class=dm-msg>Loading…</div></div>
  <div class=dm-foot>
    <button id=dm_sel_cur class=okbtn onclick=dmSelectCurrent()><svg class="ico" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Select Current Folder</button>
    <button class=sec onclick=closeDriveModal()>Cancel</button>
  </div>
</div></div>

<div id=authModal class=modal-bg onclick="if(event.target===this)closeAuthModal()">
<div class="modal drive-modal">
  <div class=dm-head><h2><svg class="ico" viewBox="0 0 24 24"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg> Google Drive Token / Login</h2><button class=dm-close onclick=closeAuthModal() aria-label="Close"><svg class="ico ico-sm" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button></div>
  <div id=am_status class=auth-status>Checking status…</div>
  <div class=dm-scopes>
    <button id=am_t_auto class=on onclick="amTab('auto')">Browser Login</button>
    <button id=am_t_manual onclick="amTab('manual')">Manual Code</button>
    <button id=am_t_paste onclick="amTab('paste')">Paste Token</button>
  </div>
  <div class=auth-body>
    <div id=am_p_auto class=auth-box>
      <h4><svg class="ico ico-sm" viewBox="0 0 24 24"><path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg> One-Click Google Login</h4>
      <p>Google account se login karke naya <b>token.json</b> + <b>.env</b> automatically update karo. Web client use kar rahe ho to Google Cloud Console me ye Redirect URI added honi chahiye:</p>
      <div id=am_ruri class=auth-uri>Loading…</div>
      <div id=am_csrc style="font-size:11.5px;color:var(--m);margin-bottom:8px"></div>
      <div class=btns>
        <button id=am_btn_login class=okbtn onclick=amStartLogin()><svg class="ico" viewBox="0 0 24 24"><path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg> Login with Google</button>
        <button class=sec onclick="copyTokenJson()"><svg class="ico" viewBox="0 0 24 24"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg> Copy TOKEN_JSON</button>
      </div>
    </div>

    <div id=am_p_manual class="auth-box hide">
      <h4><svg class="ico ico-sm" viewBox="0 0 24 24"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg> Manual Redirect / Code (Render + Desktop Client)</h4>
      <p>Agar app Render par hai aur aapke paas Desktop <b>client_secret.json</b> hai: pehle Google Login kholo, Allow karo, phir browser address bar ka poora <b>http://localhost:...</b> URL (ya sirf <b>code</b>) yahan paste karo.</p>
      <button type=button class="sec sm" style="width:100%;margin-bottom:8px" onclick=amOpenManual()><svg class="ico ico-sm" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg> 1. Open Google Login in New Tab</button>
      <label>2. Paste Redirect URL ya Auth Code</label>
      <input id=am_code placeholder="http://localhost:8080/oauth/callback?state=...&code=4/0A..." autocomplete=off>
      <div style="margin-top:8px"><button id=am_btn_ex class=okbtn style="width:100%" onclick=amSubmitCode()><svg class="ico" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Verify &amp; Save Token</button></div>
    </div>

    <div id=am_p_paste class="auth-box hide">
      <h4><svg class="ico ico-sm" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg> Direct Paste token.json / Upload client_secret.json</h4>
      <p>Agar PC par <b>python tg2gd.py --get-token</b> se token banaya hai to uska JSON seedha yahan paste karke bina restart ke live apply karo.</p>
      <textarea id=am_tok_txt style="min-height:95px;font:12px/1.45 ui-monospace,Consolas,monospace" placeholder='{"token":"...","refresh_token":"...","client_id":"...","client_secret":"..."}'></textarea>
      <div class=btns style="margin-top:8px">
        <button class=okbtn onclick=amPasteToken()><svg class="ico" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Apply Token Live</button>
        <input type=file id=am_cred_file accept=".json,application/json" class=hide onchange="amUploadCreds(this.files[0])">
        <button type=button class=sec onclick="$('am_cred_file').click()"><svg class="ico" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Upload client_secret.json</button>
      </div>
    </div>
  </div>
</div></div>

<div class=w>
<header><h1>TG <span>→</span> Drive</h1><span id=st class=pill>idle</span><span id=ph></span>
<div class=sub><div id=ch></div><div id=sv></div></div></header>

<div id=drive_warn class=drive-warn>
  <svg class="ico ico-lg" style="color:var(--er)" viewBox="0 0 24 24"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
  <div class=dw-txt><b>Google Drive Token Expired / Invalid</b><span id=dw_err>Dobara login karke naya token generate karo.</span></div>
  <button type=button class=okbtn onclick=openAuthModal()><svg class="ico" viewBox="0 0 24 24"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg> Login Google Drive</button>
</div>

<div id=pane-control class="tabpane on">
<div class=card>
  <div class=modebar>
    <button id=m_ch class=on onclick="setMode('channel')"><svg class="ico" viewBox="0 0 24 24"><path d="M4.9 19.1C1 15.2 1 8.8 4.9 4.9"/><path d="M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.5"/><circle cx="12" cy="12" r="2"/><path d="M16.2 7.8c2.3 2.3 2.3 6.1 0 8.5"/><path d="M19.1 4.9C23 8.8 23 15.1 19.1 19"/></svg> Full Channel</button>
    <button id=m_ls onclick="setMode('list')"><svg class="ico" viewBox="0 0 24 24"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg> File List</button>
    <button id=m_csv onclick="setMode('csv')"><svg class="ico" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="8" y1="13" x2="16" y2="13"/><line x1="8" y1="17" x2="16" y2="17"/><line x1="12" y1="9" x2="12" y2="19"/></svg> CSV Import</button>
  </div>

  <!-- Shared Drive Destination -->
  <div style="margin-bottom:12px">
    <label>Drive destination folder (optional — popup se select/create karo ya URL/ID paste karo)</label>
    <div class=fo-row>
      <input id=in_fo placeholder="Blank = My Drive (Root) ya folder select karo…" oninput=onFolderInput() autocomplete=off autocorrect=off autocapitalize=off>
      <button type=button class=sec onclick=openDriveModal()><svg class="ico" viewBox="0 0 24 24"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg> Select / Create</button>
    </div>
    <div id=fo_badge class="fo-badge hide"><svg class="ico" viewBox="0 0 24 24"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg><span id=fo_bname></span><b title="Clear (My Drive Root)" onclick=clearFolder()><svg class="ico ico-sm" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></b></div>
  </div>

  <!-- Mode 1: Full Channel -->
  <div id=box_ch class="ctl ctl-ch" style="margin-bottom:10px">
    <div><label>Channel (@username / invite link / id)</label><input id=in_ch placeholder="https://t.me/+xxxxxxxx" autocomplete=off autocorrect=off autocapitalize=off></div>
    <div><label>Start msg ID</label><input id=in_si type=number inputmode=numeric value=0 min=0></div>
  </div>

  <!-- Mode 2: File List / Queue -->
  <div id=box_ls class=hide style="margin-bottom:10px">
    <div class=qgrid>
      <div class=qbox>
        <h4><svg class="ico" viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg> Per-File Telegram Link(s)</h4>
        <textarea id=in_links placeholder="https://t.me/c/1234567890/45&#10;https://t.me/mychannel/108&#10;(ek ya multiple links paste karo)"></textarea>
        <div style="margin-top:8px"><button type=button class="sm" style="width:100%" onclick=addLinks()>+ Add Link(s) to List</button></div>
      </div>
      <div class=qbox>
        <h4><svg class="ico" viewBox="0 0 24 24"><line x1="4" y1="9" x2="20" y2="9"/><line x1="4" y1="15" x2="20" y2="15"/><line x1="10" y1="3" x2="8" y2="21"/><line x1="16" y1="3" x2="14" y2="21"/></svg> Channel + Msg ID Range</h4>
        <input id=in_rch placeholder="@channel / https://t.me/+... / -100..." autocomplete=off autocorrect=off autocapitalize=off>
        <div class=rng-row>
          <input id=in_rfrom type=number inputmode=numeric placeholder="From ID" min=1>
          <input id=in_rto type=number inputmode=numeric placeholder="To ID" min=1>
        </div>
        <div style="margin-top:8px"><button type=button class="sm" style="width:100%" onclick=addRange()>+ Add Range to List</button></div>
      </div>
    </div>
    <div class=qlist-head>
      <span id=qcount>Upload list (0 items)</span>
      <button type=button class="sec sm" style="padding:4px 10px;font-size:11.5px" onclick=clearQueue()>Clear List</button>
    </div>
    <div id=qlist class=qlist><div class=qempty>Koi file nahi hai — upar se links ya msg ID range add karo</div></div>
  </div>

  <!-- Mode 3: CSV Import -->
  <div id=box_csv class=hide style="margin-bottom:10px">
    <input type=file id=in_csv accept=".csv,text/csv" class=hide onchange="onCsvPick(this.files[0])">
    <div id=csv_drop class=csv-drop onclick="$('in_csv').click()">
      <svg class="ico ico-lg" style="color:var(--ac)" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
      <p id=csv_drop_lbl>CSV file choose karo ya yahan drag &amp; drop karo</p>
      <small>Supports: id_link, NEW TITLE, PATH, TYPE, size_bytes, INDEX ID</small>
    </div>
    <div id=csv_body class=hide>
      <div class=qlist-head style="margin-top:10px">
        <span id=csv_fname style="color:var(--t);font-weight:600"></span>
        <button type=button class="sec xs" onclick=clearCsv()>Clear CSV</button>
      </div>
      <div id=csv_meta class=csv-meta></div>
      <div id=csv_cats class=csv-cats></div>
      <div class=csv-idx-row>
        <div>
          <label>Continue from INDEX ID (optional — blank = shuru se)</label>
          <input id=csv_start_idx placeholder="e.g. 1105 (is INDEX ID se aage upload hoga)" oninput=renderCsv() autocomplete=off>
        </div>
        <span id=csv_last_hint class="csv-last-hint hide" onclick=fillNextIndex()></span>
      </div>
      <div class=csv-opts>
        <label class=fresh><input type=checkbox id=csv_use_path checked> PATH ke hisaab se Drive subfolders banao</label>
        <label class=fresh><input type=checkbox id=csv_use_title checked> NEW TITLE se file rename karo</label>
      </div>
      <div id=csv_match_info style="margin-top:8px;font-size:12px;color:var(--m)"></div>
      <div id=csv_prev class=csv-prev></div>
    </div>
  </div>

  <!-- 7z Store Mode Encryption -->
  <div class=qbox style="margin-bottom:10px">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap">
      <label class=fresh style="margin:0;color:var(--t);font-weight:600">
        <input type=checkbox id=in_enc onchange="toggleEncBox();persist()">
        <svg class="ico ico-sm" style="color:var(--ac)" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
        Auto 7z Encrypt (Store Mode / No Compression)
      </label>
      <span id=enc_eng_lbl style="font-size:11px;color:var(--m)">Per-file .7z · Header Lock</span>
    </div>
    <div id=enc_box class=hide style="margin-top:8px">
      <input id=in_enc_pass type=text placeholder="7z Password daalo (e.g. 1234)" oninput=persist() autocomplete=off autocorrect=off autocapitalize=off>
    </div>
  </div>

  <div class=ctl>
    <div class=btns><label class=fresh><input type=checkbox id=in_fr> Fresh (cache / progress clear)</label></div>
    <div class=btns><button id=b_go onclick=go()><svg class="ico" viewBox="0 0 24 24"><polygon points="6 4 20 12 6 20 6 4" fill="currentColor" stroke="none"/></svg> Start / Resume</button><button id=b_st class=stop onclick=askStop()><svg class="ico" viewBox="0 0 24 24"><rect x="5" y="5" width="14" height="14" rx="2" fill="currentColor" stroke="none"/></svg> Stop</button></div>
  </div>
</div>

<div class=stats>
<div class=st><small>Uploaded</small><b id=s1>0</b><i id=s1b></i></div>
<div class=st><small>Remaining</small><b id=s2>0</b><i id=s2b></i></div>
<div class=st><small>Download speed</small><b id=s3>0</b><i id=s3b></i></div>
<div class=st><small>Upload speed</small><b id=s4>0</b><i id=s4b>idle</i></div>
<div class=st><small>ETA</small><b id=s5>—</b><i id=s5b></i></div>
<div class=st><small>Elapsed</small><b id=s6>—</b><i id=s6b></i></div>
</div>

<div class=card><h3><span>Overall progress</span><span id=opct style="color:var(--t)">0%</span></h3>
<div class="bar big g"><i id=ob></i></div><div class=row><span id=o1></span><span id=o2></span></div></div>

<div class=card><h3><span>Total speed (last 2 min)</span><span id=sp_max></span></h3><canvas id=chart height=90></canvas></div>

<div class=two>
<div class=card><h3><span><svg class="ico" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Downloading</span><span id=dseg></span></h3>
<div class=nm id=dn>—</div><div class=bar><i id=db></i></div>
<div class=row><span id=dt></span><span id=dp></span></div><canvas id=map height=10></canvas></div>
<div class=card><h3><span><svg class="ico" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Uploading to Drive</span><span id=uspd></span></h3>
<div class=nm id=un>—</div><div class="bar g"><i id=ub></i></div>
<div class=row><span id=ut></span><span id=up></span></div></div>
</div>
<div class=card><h3><span>Up next</span></h3><div class=nx id=nx></div></div>
</div>

<div id=pane-accounts class=tabpane>
<div class=card>
  <h3>
    <span>Accounts</span>
    <span class=ac-head-r>
      <span id=acn></span>
      <button type=button class="sec xs" onclick="pauseAllAccs(true)"><svg class="ico ico-sm" viewBox="0 0 24 24"><rect x="6" y="4" width="4" height="16" fill="currentColor" stroke="none"/><rect x="14" y="4" width="4" height="16" fill="currentColor" stroke="none"/></svg> Pause All</button>
      <button type=button class="sec xs" onclick="pauseAllAccs(false)"><svg class="ico ico-sm" viewBox="0 0 24 24"><polygon points="6 4 20 12 6 20 6 4" fill="currentColor" stroke="none"/></svg> Resume All</button>
    </span>
  </h3>
  <div class=accs id=ac></div>
</div>
</div>

<div id=pane-logs class=tabpane>
<div class=card>
<div class=logtools><input id=in_logf placeholder="filter log…" oninput=renderLog()><button onclick=copyLog()>Copy</button></div>
<div id=log></div></div>
</div>

<div id=pane-info class=tabpane>
<div class=card><h3><span>Overall progress</span><span id=opct2 style="color:var(--t)">0%</span></h3>
<div class="bar big g"><i id=ob2></i></div><div class=row><span id=o1b></span><span id=o2b></span></div></div>

<div class=card>
  <h3>
    <span><svg class="ico" viewBox="0 0 24 24"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg> Google Drive Auth</span>
    <span class=ac-head-r>
      <button type=button class="sec xs" onclick=copyTokenJson()><svg class="ico ico-sm" viewBox="0 0 24 24"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg> Copy TOKEN_JSON</button>
      <button type=button class="okbtn xs" onclick=openAuthModal()><svg class="ico ico-sm" viewBox="0 0 24 24"><path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg> Re-login Drive</button>
    </span>
  </h3>
  <div class=row><span>Drive status</span><span id=info_drive>Checking…</span></div>
  <div class=row><span>Account</span><span id=info_drive_em>—</span></div>
</div>

<div class=card><h3><span>Lifetime</span></h3>
<div class=row><span>Total files</span><span id=info_files>0</span></div>
<div class=row><span>Total data</span><span id=info_bytes>0</span></div>
<div class=row><span>Last CSV INDEX ID</span><span id=info_idx>—</span></div>
<div class=row><span>cryptg</span><span id=info_cryptg>—</span></div></div>

<div class=card>
  <h3>
    <span><svg class="ico" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg> Drive State Editor (<span id=st_fname>tg2gd_state.txt</span>)</span>
    <span class=ac-head-r>
      <a id=st_dlink class="st-link hide" target=_blank rel=noopener>Open in Drive</a>
      <button type=button class="sec xs" onclick="loadStateEditor(true)"><svg class="ico ico-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg> Load from Drive</button>
      <button type=button class="okbtn xs" onclick=saveStateEditor()><svg class="ico ico-sm" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg> Save to Drive</button>
    </span>
  </h3>
  <textarea id=st_editor class=st-ed placeholder="Click 'Load from Drive' to view/edit tg2gd_state.txt…"></textarea>
</div>
</div>

<nav class=tabs id=tabnav>
<button data-t=control class=on onclick="showTab('control')"><span class=ic><svg class="ico ico-lg" viewBox="0 0 24 24"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg></span>Control</button>
<button data-t=accounts onclick="showTab('accounts')"><span class=ic><svg class="ico ico-lg" viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></span>Accounts</button>
<button data-t=logs onclick="showTab('logs')"><span class=ic><svg class="ico ico-lg" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span>Logs</button>
<button data-t=info onclick="showTab('info')"><span class=ic><svg class="ico ico-lg" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg></span>Info</button>
</nav>
</div><script>
const $=i=>document.getElementById(i);
const PAL=['#4f9cff','#2ecc71','#f5a623','#c06cff','#ff6b8a','#2ad1d1'];
const fb=n=>{n=+n||0;const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<4){n/=1024;i++}return n.toFixed(i?1:0)+' '+u[i]};
const ft=s=>{s=Math.round(s);if(!isFinite(s)||s<=0)return '—';const h=Math.floor(s/3600),m=Math.floor(s%3600/60),x=s%60;return h?h+'h '+m+'m':m?m+'m '+x+'s':x+'s'};
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const SVG_PLAY='<svg class="ico" viewBox="0 0 24 24"><polygon points="6 4 20 12 6 20 6 4" fill="currentColor" stroke="none"/></svg>',SVG_PAUSE='<svg class="ico" viewBox="0 0 24 24"><rect x="6" y="4" width="4" height="16" fill="currentColor" stroke="none"/><rect x="14" y="4" width="4" height="16" fill="currentColor" stroke="none"/></svg>',SVG_CHK='<svg class="ico" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg>',SVG_FLD='<svg class="ico" viewBox="0 0 24 24"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>',SVG_X='<svg class="ico ico-sm" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';
let filled=false,lastMap='',lastLog=[],failStreak=0;
let curMode='channel',queue=[],folderName='',csvInfo=null,csvCats=new Set(),lastAccs=[],lastIndexId='';

// -- tabs --
function showTab(name){document.querySelectorAll('.tabpane').forEach(p=>p.classList.toggle('on',p.id==='pane-'+name));
document.querySelectorAll('nav.tabs button').forEach(b=>b.classList.toggle('on',b.dataset.t===name));
if(navigator.vibrate)navigator.vibrate(8)}

// -- toasts --
function toast(msg,kind){const d=document.createElement('div');d.className='toast '+(kind||'');d.textContent=msg;$('toasts').appendChild(d);
setTimeout(()=>{d.style.transition='opacity .4s';d.style.opacity=0;setTimeout(()=>d.remove(),400)},2800)}

// -- Google Drive Auth Modal --
function amTab(t){
 ['auto','manual','paste'].forEach(k=>{
  $('am_t_'+k).classList.toggle('on',k===t);
  $('am_p_'+k).classList.toggle('hide',k!==t);
 });
}
async function openAuthModal(){
 $('authModal').classList.add('show');
 amTab('auto');
 $('am_status').className='auth-status';
 $('am_status').textContent='Checking Google Drive auth status…';
 try{
  const r=await fetch('/api/drive/auth/status');
  const j=await r.json();
  if(j.drive_ok){
   $('am_status').className='auth-status ok';
   $('am_status').textContent='Connected: '+(j.email||'Google Drive OK');
  }else{
   $('am_status').className='auth-status bad';
   $('am_status').textContent='Not connected: '+(j.error||'Token expired / missing');
  }
  $('am_ruri').textContent=j.redirect_uri||location.origin+'/oauth/callback';
  $('am_csrc').textContent=j.has_creds?('Credentials: '+j.cred_source+' ('+j.cred_kind+')'):'Warning: client_secret.json nahi mila — Paste Token tab se upload karo';
  $('am_btn_login').disabled=!j.has_creds;
 }catch(e){
  $('am_status').className='auth-status bad';
  $('am_status').textContent='Status load fail ho gaya';
 }
}
function closeAuthModal(){$('authModal').classList.remove('show')}
function amStartLogin(){location.href='/oauth/start'}
async function amOpenManual(){
 try{
  const r=await fetch('/api/drive/auth/url',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({manual:true})});
  const j=await r.json();
  if(!j.ok){toast(j.error||'Auth URL fail','err');return}
  window.open(j.url,'_blank','noopener');
  toast('Google login naye tab me khul gaya — allow karke URL paste karo','ok');
 }catch(e){toast('Request fail ho gaya','err')}
}
async function amSubmitCode(){
 const raw=$('am_code').value.trim();
 if(!raw){toast('Redirect URL ya code paste karo pehle','err');return}
 $('am_btn_ex').disabled=true;
 try{
  const r=await fetch('/api/drive/auth/exchange',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:raw})});
  const j=await r.json();
  if(j.ok){
   $('am_code').value='';
   toast('Google Drive login OK: '+j.email,'ok');
   openAuthModal();tick();
  }else toast(j.error||'Token exchange fail','err');
 }catch(e){toast('Request fail ho gaya','err')}
 $('am_btn_ex').disabled=false;
}
async function amPasteToken(){
 const text=$('am_tok_txt').value.trim();
 if(!text){toast('token.json paste karo pehle','err');return}
 try{
  const r=await fetch('/api/drive/auth/paste',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text})});
  const j=await r.json();
  if(j.ok){
   $('am_tok_txt').value='';
   toast('Token live apply ho gaya: '+j.email,'ok');
   openAuthModal();tick();
  }else toast(j.error||'Invalid token','err');
 }catch(e){toast('Request fail ho gaya','err')}
}
async function amUploadCreds(file){
 if(!file)return;
 try{
  const text=await file.text();
  const r=await fetch('/api/drive/auth/credentials',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({filename:file.name,text})});
  const j=await r.json();
  if(j.ok){toast('Credentials save ho gaye ('+j.kind+')','ok');openAuthModal()}
  else toast(j.error||'Invalid credentials file','err');
 }catch(e){toast('Upload fail ho gaya','err')}
 $('am_cred_file').value='';
}
async function copyTokenJson(){
 try{
  const r=await fetch('/api/drive/auth/token_str');
  const j=await r.json();
  if(!j.ok||!j.token_json){toast(j.error||'Token available nahi hai','err');return}
  await navigator.clipboard.writeText(j.token_json);
  toast('TOKEN_JSON copy ho gaya (Render Env me paste kar sakte ho)','ok');
 }catch(e){toast('Copy nahi ho paya','err')}
}

// -- check OAuth redirect query banner --
(function(){
 const p=new URLSearchParams(location.search);
 if(p.get('auth')==='ok'){
  toast('Google Drive login successful! Naya token save ho gaya','ok');
  history.replaceState(null,'','/');
 }else if(p.get('auth_err')){
  toast('Drive login fail: '+p.get('auth_err'),'err');
  history.replaceState(null,'','/');
 }
})();

// -- mode switcher --
function setMode(m){
 curMode=(m==='list'||m==='csv')?m:'channel';
 $('m_ch').classList.toggle('on',curMode==='channel');
 $('m_ls').classList.toggle('on',curMode==='list');
 $('m_csv').classList.toggle('on',curMode==='csv');
 $('box_ch').classList.toggle('hide',curMode!=='channel');
 $('box_ls').classList.toggle('hide',curMode!=='list');
 $('box_csv').classList.toggle('hide',curMode!=='csv');
 $('b_go').innerHTML=SVG_PLAY+(curMode==='csv'?' Upload CSV':curMode==='list'?' Upload List':' Start / Resume');
 persist();
}

// -- folder badge --
function setFolder(id,name){
 $('in_fo').value=id||'';
 folderName=name||'';
 renderFolderBadge();
 persist();
}
function onFolderInput(){folderName='';renderFolderBadge();persist()}
function clearFolder(){setFolder('','');toast('My Drive (Root) set ho gaya','ok')}
function toggleEncBox(){$('enc_box').classList.toggle('hide',!$('in_enc').checked)}
function renderFolderBadge(){
 const v=$('in_fo').value.trim();
 if(folderName){$('fo_bname').textContent=folderName+(v?' ('+v.slice(0,10)+'…)':'');$('fo_badge').classList.remove('hide')}
 else{$('fo_badge').classList.add('hide')}
}

// -- CSV upload & preview --
const csvDrop=$('csv_drop');
['dragenter','dragover'].forEach(ev=>csvDrop.addEventListener(ev,e=>{e.preventDefault();csvDrop.classList.add('drag')}));
['dragleave','drop'].forEach(ev=>csvDrop.addEventListener(ev,e=>{e.preventDefault();csvDrop.classList.remove('drag')}));
csvDrop.addEventListener('drop',e=>{const f=e.dataTransfer&&e.dataTransfer.files&&e.dataTransfer.files[0];if(f)onCsvPick(f)});

async function onCsvPick(file){
 if(!file)return;
 $('csv_drop_lbl').textContent='Parsing '+file.name+'…';
 try{
  const text=await file.text();
  const r=await fetch('/api/csv/parse',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({filename:file.name,text})});
  const j=await r.json();
  if(!j.ok){toast(j.error||'CSV parse fail','err');$('csv_drop_lbl').textContent='CSV file choose karo ya yahan drag & drop karo';return}
  csvInfo=j.csv;
  csvCats=new Set();
  renderCsv();
  toast('CSV load hui: '+csvInfo.total+' files ('+csvInfo.folders_count+' folders)','ok');
 }catch(e){toast('CSV upload fail ho gaya','err')}
 $('in_csv').value='';
 $('csv_drop_lbl').textContent='CSV file choose karo ya yahan drag & drop karo';
}
async function clearCsv(){
 try{await fetch('/api/csv/clear',{method:'POST'})}catch(e){}
 csvInfo=null;csvCats=new Set();$('csv_start_idx').value='';renderCsv();toast('CSV clear ho gayi','warn');
}
function toggleCsvCat(cat){
 if(!cat){csvCats=new Set()}
 else if(csvCats.has(cat))csvCats.delete(cat);
 else csvCats.add(cat);
 renderCsv();
}
function fillNextIndex(){
 if(!lastIndexId)return;
 const nextVal=/^\d+$/.test(lastIndexId)?String(+lastIndexId+1):lastIndexId;
 $('csv_start_idx').value=nextVal;
 renderCsv();
 toast('Start INDEX ID set: '+nextVal,'ok');
}
function filterCsvPreview(items){
 let list=items||[];
 const sidx=($('csv_start_idx').value||'').trim();
 if(sidx){
  const exact=list.findIndex(it=>String(it.index_id||'').trim()===sidx);
  if(exact>=0)list=list.slice(exact);
  else if(/^\d+$/.test(sidx)){
   const t=+sidx;
   const np=list.findIndex(it=>/^\d+$/.test(String(it.index_id||'').trim())&&+it.index_id>=t);
   list=np>=0?list.slice(np):[];
  }else list=[];
 }
 if(csvCats.size>0)list=list.filter(it=>csvCats.has(it.cat));
 return list;
}
function renderCsv(){
 if(!csvInfo||!csvInfo.total){$('csv_body').classList.add('hide');return}
 $('csv_body').classList.remove('hide');
 $('csv_fname').textContent=csvInfo.filename||'CSV';
 $('csv_meta').innerHTML=
  '<span class=csv-pill><b>'+csvInfo.total+'</b> files</span>'+
  (csvInfo.total_bytes?'<span class=csv-pill><b>'+fb(csvInfo.total_bytes)+'</b></span>':'')+
  '<span class=csv-pill><b>'+csvInfo.folders_count+'</b> folders</span>'+
  '<span class=csv-pill><b>'+(csvInfo.channels?csvInfo.channels.length:1)+'</b> channel</span>'+
  (csvInfo.skipped?'<span class=csv-pill>'+csvInfo.skipped+' skipped</span>':'');
 const cats=csvInfo.categories||{};
 const ckeys=Object.keys(cats);
 if(ckeys.length>1){
  $('csv_cats').innerHTML='<button type=button class="csv-cat'+(csvCats.size===0?' on':'')+'" onclick="toggleCsvCat(\'\')">All ('+csvInfo.total+')</button>'+
   ckeys.map(k=>'<button type=button class="csv-cat'+(csvCats.has(k)?' on':'')+'" onclick="toggleCsvCat(\''+esc(k)+'\')">'+esc(k)+' ('+cats[k]+')</button>').join('');
 }else{$('csv_cats').innerHTML=''}
 if(lastIndexId){
  const nxt=/^\d+$/.test(lastIndexId)?(+lastIndexId+1):lastIndexId;
  $('csv_last_hint').textContent='Last done: #'+lastIndexId+' → Tap for #'+nxt;
  $('csv_last_hint').classList.remove('hide');
 }else{$('csv_last_hint').classList.add('hide')}
 const matched=filterCsvPreview(csvInfo.preview||[]);
 const sidx=($('csv_start_idx').value||'').trim();
 $('csv_match_info').textContent='Upload queue preview: '+matched.length+' file'+(matched.length===1?'':'s')+(sidx?' (from INDEX ID '+sidx+')':'')+(matched.length>60?' · showing first 60':'');
 const show=matched.slice(0,60);
 $('csv_prev').innerHTML=show.length?show.map(it=>'<div class=csv-row><div class=ct title="'+esc(it.name)+'">'+(it.index_id?'#'+esc(it.index_id)+' · ':'')+esc(it.name||('msg #'+it.msg_id))+'</div><div class=cp>'+(it.folder?esc(it.folder)+' · ':'')+(it.size_hint?fb(it.size_hint):'')+(it.cat?' · '+esc(it.cat):'')+'</div></div>').join(''):'<div class=qempty>Is INDEX ID / filter me koi file nahi mili</div>';
}

// -- Drive State Editor --
async function loadStateEditor(fromDrive){
 try{
  const r=await fetch('/api/state/raw'+(fromDrive?'?refresh=1':''));
  const j=await r.json();
  if(!j.ok){toast(j.error||'State load fail','err');return}
  $('st_editor').value=j.text||'';
  if(j.drive_url){$('st_dlink').href=j.drive_url;$('st_dlink').classList.remove('hide')}
  toast(fromDrive?'Drive se latest state load ho gayi':'State load ho gayi','ok');
 }catch(e){toast('State load request fail','err')}
}
async function saveStateEditor(){
 const text=$('st_editor').value;
 if(!text.trim()){toast('State khaali hai','err');return}
 try{JSON.parse(text)}catch(e){toast('Invalid JSON: '+e.message,'err');return}
 try{
  const r=await fetch('/api/state/raw',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text})});
  const j=await r.json();
  if(j.ok){toast('tg2gd_state.txt Drive me save ho gayi','ok');filled=false;tick()}
  else toast(j.error||'Save fail','err');
 }catch(e){toast('Save request fail','err')}
}

// -- Account Pause / Resume --
async function toggleAccIdx(i){
 const a=lastAccs[i];if(!a||a.status==='dead')return;
 try{
  const r=await fetch('/api/account/pause',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:a.id,paused:!a.paused})});
  const j=await r.json();
  if(j.ok){toast(a.name+': '+(j.paused?'Paused':'Resumed'),j.paused?'warn':'ok');tick()}
  else toast(j.error||'Action fail','err');
 }catch(e){toast('Request fail ho gaya','err')}
}
async function pauseAllAccs(paused){
 try{
  const r=await fetch('/api/account/pause',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:'__all__',paused})});
  const j=await r.json();
  if(j.ok){toast(paused?'Sab accounts pause ho gaye':'Sab accounts resume ho gaye',paused?'warn':'ok');tick()}
  else toast(j.error||'Action fail','err');
 }catch(e){toast('Request fail ho gaya','err')}
}

// -- Telegram link parser & File List / Queue --
function parseMsgLink(u){
 u=(u||'').trim().split('?')[0].split('#')[0].replace(/\/+$/,'');
 let m=u.match(/(?:t\.me|telegram\.me)\/c\/(\d+)\/(?:\d+\/)?(\d+)$/i);
 if(m)return {kind:'link',channel:'-100'+m[1],msg_id:+m[2]};
 m=u.match(/(?:t\.me|telegram\.me)\/(?:s\/)?([A-Za-z][A-Za-z0-9_]{3,})\/(?:\d+\/)?(\d+)$/i);
 if(m&&!['c','joinchat','addlist','addstickers','proxy','socks'].includes(m[1].toLowerCase()))
  return {kind:'link',channel:'@'+m[1],msg_id:+m[2]};
 return null;
}
function addLinks(){
 const raw=$('in_links').value.trim();
 if(!raw){toast('Pehle file link(s) paste karo','err');return}
 const tokens=raw.split(/[\s,]+/).filter(Boolean);
 let added=0,bad=0;
 tokens.forEach(tok=>{
  const p=parseMsgLink(tok);
  if(p){queue.push(p);added++}else bad++;
 });
 if(added){$('in_links').value='';renderQueue();persist();toast(added+' link add ho gaye'+(bad?' ('+bad+' invalid)':''),'ok')}
 else toast('Valid Telegram file link nahi mila (e.g. t.me/c/.../123)','err');
}
function addRange(){
 const ch=$('in_rch').value.trim();
 let a=+$('in_rfrom').value||0,b=+$('in_rto').value||0;
 if(!ch){toast('Channel daalo pehle','err');return}
 if(a<=0||b<=0){toast('From ID aur To ID daalo (1 ya usse zyada)','err');return}
 if(a>b){const t=a;a=b;b=t}
 queue.push({kind:'range',channel:ch,start_id:a,end_id:b});
 $('in_rfrom').value='';$('in_rto').value='';
 renderQueue();persist();
 toast('Range #'+a+'–#'+b+' add ho gayi ('+(b-a+1)+' msgs)','ok');
}
function delQueue(i){queue.splice(i,1);renderQueue();persist()}
function clearQueue(){if(!queue.length)return;queue=[];renderQueue();persist();toast('List clear ho gayi','warn')}
function renderQueue(){
 $('qcount').textContent='Upload list ('+queue.length+' item'+(queue.length===1?'':'s')+')';
 if(!queue.length){$('qlist').innerHTML='<div class=qempty>Koi file nahi hai — upar se links ya msg ID range add karo</div>';return}
 $('qlist').innerHTML=queue.map((it,i)=>{
  if(it.kind==='link')
   return '<div class=qitem><span class=tag>LINK</span><span class=qt title="'+esc(it.channel+' #'+it.msg_id)+'">'+esc(it.channel)+' <b>#'+it.msg_id+'</b></span><button class=del onclick="delQueue('+i+')" aria-label="Remove">'+SVG_X+'</button></div>';
  const cnt=it.end_id-it.start_id+1;
  return '<div class=qitem><span class="tag rng">RANGE</span><span class=qt title="'+esc(it.channel)+' #'+it.start_id+'–#'+it.end_id+'">'+esc(it.channel)+' <b>#'+it.start_id+'–#'+it.end_id+'</b> ('+cnt+')</span><button class=del onclick="delQueue('+i+')" aria-label="Remove">'+SVG_X+'</button></div>';
 }).join('');
}

// -- Google Drive Folder Picker & Creator Pop-up --
let dmScope='mydrive',dmStack=[{id:'root',name:'My Drive'}],dmFolders=[];
function openDriveModal(){
 $('driveModal').classList.add('show');
 $('dm_search').value='';$('dm_newname').value='';
 dmSetScope('mydrive');
}
function closeDriveModal(){$('driveModal').classList.remove('show')}
function dmSetScope(sc){
 dmScope=sc;
 ['mydrive','shared','drives'].forEach(k=>$('dm_s_'+k).classList.toggle('on',k===sc));
 if(sc==='mydrive')dmStack=[{id:'root',name:'My Drive'}];
 else if(sc==='shared')dmStack=[{id:'shared_root',name:'Shared with me'}];
 else dmStack=[{id:'drives_root',name:'Shared Drives'}];
 $('dm_search').value='';
 dmLoad();
}
function dmCur(){return dmStack[dmStack.length-1]}
function dmJump(idx){dmStack=dmStack.slice(0,idx+1);$('dm_search').value='';dmLoad()}
function dmOpen(id,name){dmStack.push({id,name});$('dm_search').value='';dmLoad()}
function dmRenderCrumb(){
 $('dm_crumb').innerHTML=dmStack.map((c,i)=>(i?' › ':'')+'<span onclick="dmJump('+i+')">'+esc(c.name)+'</span>').join('');
 const cur=dmCur();
 const canSel=cur.id!=='shared_root'&&cur.id!=='drives_root';
 $('dm_sel_cur').disabled=!canSel;
 $('dm_sel_cur').innerHTML=SVG_CHK+(cur.id==='root'?' Select My Drive (Root)':' Select "'+esc(cur.name)+'"');
 $('dm_btn_mk').disabled=!canSel;
}
async function dmLoad(){
 dmRenderCrumb();
 $('dm_list').innerHTML='<div class=dm-msg>Drive folders load ho rahe hain…</div>';
 const cur=dmCur(),q=$('dm_search').value.trim();
 try{
  const r=await fetch('/api/drive/folders?parent='+encodeURIComponent(cur.id)+'&scope='+encodeURIComponent(dmScope)+'&q='+encodeURIComponent(q));
  const j=await r.json();
  if(!j.ok){$('dm_list').innerHTML='<div class=dm-err>Error: '+esc(j.error||'Drive load fail')+'</div>';return}
  dmFolders=j.folders||[];
  if(!dmFolders.length){$('dm_list').innerHTML='<div class=dm-msg>Yahan koi folder nahi hai — upar se naya folder create kar sakte ho</div>';return}
  $('dm_list').innerHTML=dmFolders.map((f,i)=>'<div class=dm-row onclick="dmOpenIdx('+i+')">'+SVG_FLD+'<span class=dn title="'+esc(f.name)+'">'+esc(f.name)+'</span>'
   +'<button type=button class="sm okbtn" onclick="event.stopPropagation();dmPickIdx('+i+')">Select</button></div>').join('');
 }catch(e){$('dm_list').innerHTML='<div class=dm-err>Network error — dobara try karo</div>'}
}
async function dmCreate(){
 const name=$('dm_newname').value.trim();
 if(!name){toast('Folder ka naam likho pehle','err');return}
 const cur=dmCur();
 if(cur.id==='shared_root'||cur.id==='drives_root'){toast('Pehle koi folder ya My Drive kholo','warn');return}
 $('dm_btn_mk').disabled=true;
 try{
  const r=await fetch('/api/drive/folders',{method:'POST',body:JSON.stringify({name,parent:cur.id})});
  const j=await r.json();
  if(j.ok&&j.folder){
   $('dm_newname').value='';
   toast('Folder "'+j.folder.name+'" ban gaya','ok');
   dmOpen(j.folder.id,j.folder.name);
  }else toast('Create fail: '+(j.error||'unknown'),'err');
 }catch(e){toast('Create request fail ho gaya','err')}
 $('dm_btn_mk').disabled=false;
}
function dmOpenIdx(i){const f=dmFolders[i];if(f)dmOpen(f.id,f.name)}
function dmPickIdx(i){const f=dmFolders[i];if(f)dmPick(f.id,f.name)}
function dmPick(id,name){
 setFolder(id==='root'?'':id,name);
 closeDriveModal();
 toast('Folder select hua: '+name,'ok');
}
function dmSelectCurrent(){
 const cur=dmCur();
 if(cur.id==='shared_root'||cur.id==='drives_root')return;
 dmPick(cur.id,cur.id==='root'?'My Drive (Root)':cur.name);
}

// -- stop confirm modal --
function askStop(){$('stopModal').classList.add('show')}
function closeModal(){$('stopModal').classList.remove('show')}
async function doStop(){try{await fetch('/api/stop',{method:'POST'});toast('Stop signal bhej diya','warn')}catch(e){toast('Stop request fail ho gaya','err')}}

// -- persist last-used settings locally --
try{
 const saved=JSON.parse(localStorage.getItem('tg2gd_last')||'{}');
 if(saved.channel)$('in_ch').value=saved.channel;
 if(saved.folder)$('in_fo').value=saved.folder;
 if(saved.folder_name)folderName=saved.folder_name;
 if(Array.isArray(saved.queue))queue=saved.queue;
 if(saved.enc_enabled){$('in_enc').checked=true;toggleEncBox()}
 if(saved.enc_pass)$('in_enc_pass').value=saved.enc_pass;
 if(saved.mode)setMode(saved.mode);
 renderFolderBadge();renderQueue();
}catch(e){}
function persist(){
 try{localStorage.setItem('tg2gd_last',JSON.stringify({
  mode:curMode,channel:$('in_ch').value,folder:$('in_fo').value,folder_name:folderName,queue,
  enc_enabled:$('in_enc').checked,enc_pass:$('in_enc_pass').value
 }))}catch(e){}
}

async function go(){
 const folder=$('in_fo').value.trim();
 const enc_enabled=$('in_enc').checked,enc_pass=$('in_enc_pass').value.trim();
 if(enc_enabled&&!enc_pass){toast('7z Encryption ke liye password daalo','err');return}
 const payload={mode:curMode,folder,folder_name:folderName,fresh:$('in_fr').checked,enc_enabled,enc_pass};
 if(curMode==='csv'){
  if(!csvInfo||!csvInfo.total){toast('Pehle CSV file upload karo','err');return}
  payload.csv_opts={
   use_path:$('csv_use_path').checked,
   use_title:$('csv_use_title').checked,
   cats:Array.from(csvCats),
   start_index_id:($('csv_start_idx').value||'').trim()
  };
 }else if(curMode==='list'){
  if(!queue.length){toast('File list khaali hai — links ya range add karo','err');return}
  payload.queue=queue;
 }else{
  const channel=$('in_ch').value.trim();
  if(!channel){toast('Channel daalo pehle','err');return}
  payload.channel=channel;
  payload.start_id=+$('in_si').value||0;
 }
 persist();
 $('b_go').disabled=true;
 try{
  const r=await fetch('/api/start',{method:'POST',body:JSON.stringify(payload)});
  const j=await r.json();
  if(j.ok){toast('Shuru ho gaya','ok');if(navigator.vibrate)navigator.vibrate(15)}
  else toast(j.error||'Pehle se chal raha hai ya busy hai','warn');
 }catch(e){toast('Start request fail ho gaya','err')}
 $('in_fr').checked=false;filled=true;$('b_go').disabled=false;
}
function copyLog(){const txt=lastLog.map(r=>'['+r[0]+'] '+r[2]).join('\n');
(navigator.clipboard?navigator.clipboard.writeText(txt):Promise.reject()).then(()=>toast('Log copy ho gaya','ok')).catch(()=>toast('Copy nahi ho paya','err'))}
function renderLog(){const l=$('log'),q=($('in_logf').value||'').toLowerCase();
const rows=q?lastLog.filter(r=>r[2].toLowerCase().includes(q)):lastLog;
const bt=l.scrollTop+l.clientHeight>=l.scrollHeight-30;
l.innerHTML=rows.map(r=>'<div class="'+r[1]+(q?' hit':'')+'"><b>'+r[0]+'</b>'+esc(r[2])+'</div>').join('');
if(bt)l.scrollTop=l.scrollHeight}
function drawChart(h){const c=$('chart'),W=c.clientWidth*devicePixelRatio,H=90*devicePixelRatio;c.width=W;c.height=H;const x=c.getContext('2d');
const mx=Math.max(1048576,...h)*1.15;x.strokeStyle='#1c2740';x.lineWidth=1;for(let i=1;i<4;i++){const y=H*i/4;x.beginPath();x.moveTo(0,y);x.lineTo(W,y);x.stroke()}
if(h.length<2)return;const n=120,sx=W/(n-1),off=n-h.length;x.beginPath();h.forEach((v,i)=>{const px=(i+off)*sx,py=H-v/mx*H;i?x.lineTo(px,py):x.moveTo(px,py)});
x.strokeStyle='#4f9cff';x.lineWidth=2*devicePixelRatio;x.stroke();x.lineTo((h.length-1+off)*sx,H);x.lineTo(off*sx,H);x.closePath();x.fillStyle='rgba(79,156,255,.15)';x.fill();$('sp_max').textContent='max '+fb(Math.max(...h))+'/s'}
function drawMap(map,n){if(!n){$('map').height=0;lastMap='';return}if(map===lastMap)return;lastMap=map;const c=$('map'),W=900;
const cell=Math.max(3,Math.min(16,Math.floor(Math.sqrt(W*70/n)))),cols=Math.floor(W/(cell+1)),rows=Math.ceil(n/cols);c.width=W;c.height=rows*(cell+1);const x=c.getContext('2d');
for(let i=0;i<n;i++){const o=parseInt(map.substr(i*2,2),16)||0;x.fillStyle=o==0?'#1a2438':o==255?'#5b6b86':PAL[(o-1)%PAL.length];x.fillRect((i%cols)*(cell+1),Math.floor(i/cols)*(cell+1),cell,cell)}}
async function tick(){let s;try{const r=await fetch('/api/status');if(r.status==401){location='/login';return}s=await r.json();failStreak=0;$('offlinebar').classList.remove('show')}catch(e){failStreak++;if(failStreak>=2)$('offlinebar').classList.add('show');return}
const prevLastIdx=lastIndexId;
lastIndexId=s.last_index_id||'';
if(!filled&&(s.channel||s.folder||(s.queue&&s.queue.length)||s.csv_info)&&!['INPUT','TEXTAREA'].includes(document.activeElement.tagName)){
 if(s.channel)$('in_ch').value=s.channel;
 if(s.folder)$('in_fo').value=s.folder;
 if(s.folder_name){folderName=s.folder_name;renderFolderBadge()}
 if(s.queue&&s.queue.length&&!queue.length){queue=s.queue;renderQueue()}
 if(s.csv_info&&!csvInfo){
  csvInfo=s.csv_info;
  if(s.csv_opts){
   $('csv_use_path').checked=s.csv_opts.use_path!==false;
   $('csv_use_title').checked=s.csv_opts.use_title!==false;
   csvCats=new Set(s.csv_opts.cats||[]);
   if(s.csv_opts.start_index_id)$('csv_start_idx').value=s.csv_opts.start_index_id;
  }
  renderCsv();
 }
 if(s.enc_enabled){$('in_enc').checked=true;toggleEncBox()}
 if(s.enc_pass)$('in_enc_pass').value=s.enc_pass;
 if(s.mode==='list'||s.mode==='csv')setMode(s.mode);
 filled=true;
}else if(prevLastIdx!==lastIndexId&&csvInfo){renderCsv()}
// Drive auth banner & Info tab status
if(s.drive_expired||!s.drive_ok){
 $('drive_warn').classList.add('show');
 $('dw_err').textContent=s.drive_err||'Token expire/revoke ho gaya — wapis login karo.';
 $('info_drive').textContent='Expired / Disconnected';
 $('info_drive').style.color='var(--er)';
}else{
 $('drive_warn').classList.remove('show');
 $('info_drive').textContent='Connected (OK)';
 $('info_drive').style.color='var(--ok)';
}
$('info_drive_em').textContent=s.drive_email||'—';

const run=s.status=='running';$('st').textContent=s.stopping?'stopping…':s.all_paused&&run?'paused':s.status;$('st').className='pill '+(s.stopping||(s.all_paused&&run)?'stopped':s.status);$('b_go').disabled=run;$('b_st').disabled=!run||s.stopping;
const scanning=s.phase=='scanning';$('ph').textContent=scanning?('scanning… '+s.scan.files+' files'):(s.phase||'');
$('ch').textContent=s.channel_title||'';
const sf=s.state_file||'tg2gd_state.txt';
$('st_fname').textContent=sf;
if(s.state_file_id){$('st_dlink').href='https://drive.google.com/file/d/'+s.state_file_id+'/view';$('st_dlink').classList.remove('hide')}
$('sv').textContent=s.state_ago<0?'state: not saved yet':'state saved '+Math.round(s.state_ago)+'s ago (Drive: '+sf+')';
const p=s.plan,dl=s.dl,ul=s.ul;
$('s1').textContent=p.up_files+' / '+p.files;$('s1b').textContent=fb(p.up_bytes)+' uploaded';
$('s2').textContent=s.rem_files;$('s2b').textContent=fb(Math.max(0,p.bytes-p.up_bytes))+' left';
$('s3').textContent=fb(s.speed)+'/s';$('s3b').textContent='avg '+fb(s.avg)+'/s · '+s.accs.filter(a=>a.status=='ok'&&!a.paused&&a.speed>0).length+' acc active';
$('s4').textContent=fb(ul?ul.speed:0)+'/s';$('s4b').textContent=ul?'uploading now':'idle';$('s5').textContent=ft(s.eta);$('s5b').textContent='download';
$('s6').textContent=s.elapsed?ft(s.elapsed):'—';$('s6b').textContent='lifetime: '+s.done_count+' files · '+fb(s.total_bytes);
const pc=p.bytes?Math.min(100,(p.up_bytes+(ul?ul.done:0))/p.bytes*100):0;$('ob').style.width=pc+'%';$('opct').textContent=pc.toFixed(1)+'%';
$('o1').textContent=fb(p.up_bytes)+' / '+fb(p.bytes);
$('o2').textContent='downloaded '+p.dl_files+' files'+(s.mode==='csv'?(' · csv mode'+(lastIndexId?' · last INDEX #'+lastIndexId:'')):s.mode==='list'?' · list mode':' · last done msg #'+s.last_done);
if(dl){const q=dl.size?dl.done/dl.size*100:0;$('dn').textContent=(dl.index_id?'[INDEX #'+dl.index_id+'] ':'')+'#'+dl.id+'  '+dl.name;$('dn').title=(dl.folder?dl.folder+'/':'')+dl.name;$('db').style.width=q+'%';$('dt').textContent=fb(dl.done)+' / '+fb(dl.size)+'  @ '+fb(dl.speed)+'/s';
$('dp').textContent=q.toFixed(1)+'%  ·  '+ft(dl.speed>0?(dl.size-dl.done)/dl.speed:0);$('dseg').textContent=dl.nseg?dl.segs_done+' / '+dl.nseg+' segments':'';drawMap(dl.map||'',dl.nseg)}
else{$('dn').textContent=scanning?'scanning…':'—';$('db').style.width=0;$('dt').textContent=$('dp').textContent=$('dseg').textContent='';drawMap('',0)}
if(ul){const q=ul.size?ul.done/ul.size*100:0;$('un').textContent=(ul.index_id?'[INDEX #'+ul.index_id+'] ':'')+'#'+ul.id+'  '+ul.name;$('un').title=(ul.folder?ul.folder+'/':'')+ul.name;$('uspd').textContent=fb(ul.speed)+'/s';$('ub').style.width=q+'%';$('ut').textContent=fb(ul.done)+' / '+fb(ul.size)+'  @ '+fb(ul.speed)+'/s';$('up').textContent=q.toFixed(1)+'%  ·  '+ft(ul.speed>0?(ul.size-ul.done)/ul.speed:0)}
else{$('un').textContent='—';$('uspd').textContent='';$('ub').style.width=0;$('ut').textContent=$('up').textContent=''}
$('nx').innerHTML=s.next.length?s.next.map(n=>'<span>'+(n.index_id?'IDX #'+esc(n.index_id)+' · ':'#'+n.id+' · ')+(n.name?esc(n.name.slice(0,32))+' · ':'')+fb(n.size)+'</span>').join(''):'<span>—</span>';
lastAccs=s.accs||[];
const pCnt=lastAccs.filter(a=>a.paused&&a.status!=='dead').length;
const mxs=Math.max(2097152,...lastAccs.map(a=>a.speed));
$('acn').textContent=lastAccs.length+' accounts'+(pCnt?' · '+pCnt+' paused':'')+(s.cryptg?' · cryptg: OK':' · cryptg: off');
$('ac').innerHTML=lastAccs.map((a,i)=>{const col=PAL[a.idx%PAL.length];
const dead=a.status=='dead';
let ch=dead?'<span class="chip dead">dead</span>':a.paused?'<span class="chip paused">paused</span>':a.status=='flood'?'<span class="chip flood">flood '+a.flood_left+'s</span>':a.speed>0?'<span class=chip>active</span>':'<span class="chip idle">idle</span>';
const btn=dead?'<button type=button class="sec" disabled>Dead</button>':a.paused?'<button type=button class="okbtn" onclick="toggleAccIdx('+i+')">'+SVG_PLAY+' Resume</button>':'<button type=button class="wabtn" onclick="toggleAccIdx('+i+')">'+SVG_PAUSE+' Pause</button>';
return '<div class="ac'+(a.paused&&!dead?' paused':'')+'"><div class=h><span class=dot style="background:'+col+'"></span><div style="min-width:0"><div class=n>'+esc(a.name)+'</div><div class=ph>'+esc(a.phone)+'</div></div>'+ch+'</div>'
+'<div class=sp>'+fb(a.speed)+'/s</div><div class=bar><i style="width:'+Math.min(100,a.speed/mxs*100)+'%;background:'+col+'"></i></div>'
+'<div class=row><span>'+fb(a.bytes)+' · '+a.segs+' seg</span><span>'+a.conn+' conn</span></div>'
+'<div class=row><span>flood: '+a.floods+'</span><span>errors: '+a.errors+'</span></div>'+(a.last_err?'<div class=er title="'+esc(a.last_err)+'">'+esc(a.last_err)+'</div>':'')
+'<div class=act>'+btn+'</div></div>'}).join('');
drawChart(s.hist);
lastLog=s.log;renderLog();
$('opct2').textContent=$('opct').textContent;$('ob2').style.width=$('ob').style.width;$('o1b').textContent=$('o1').textContent;$('o2b').textContent=$('o2').textContent;
$('info_files').textContent=s.done_count;$('info_bytes').textContent=fb(s.total_bytes);$('info_idx').textContent=lastIndexId?('#'+lastIndexId):'—';$('info_cryptg').textContent=s.cryptg?'OK (fast decrypt)':'off (slower)';
if(s.enc_engine)$('enc_eng_lbl').textContent='Per-file .7z · Header Lock · '+s.enc_engine;
}
setInterval(tick,1000);tick();
window.addEventListener('online',()=>{failStreak=0;$('offlinebar').classList.remove('show');tick()});
window.addEventListener('offline',()=>$('offlinebar').classList.add('show'));
</script></body></html>"""

LOGIN = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover,maximum-scale=1">
<meta name=theme-color content="#0a0e16">
<body style="background:#0a0e16;color:#e8eef8;font-family:system-ui;display:grid;place-items:center;min-height:100dvh;margin:0;padding:env(safe-area-inset-top,0) 16px env(safe-area-inset-bottom,0);box-sizing:border-box">
<form method=post style="background:#111826;padding:28px;border-radius:18px;border:1px solid #1c2740;width:100%;max-width:320px">
<h3 style="margin:0 0 16px;font-size:18px;display:flex;align-items:center"><svg style="width:18px;height:18px;vertical-align:-3px;margin-right:6px;stroke:#4f9cff;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>TG → Drive</h3>
<input name=p type=password inputmode=numeric autofocus placeholder=Password autocomplete=current-password style="width:100%;padding:14px 13px;border-radius:10px;border:1px solid #1c2740;background:#0d1420;color:#fff;outline:none;font-size:16px;box-sizing:border-box">
<button style="margin-top:14px;width:100%;padding:14px;border:0;border-radius:10px;background:#4f9cff;color:#fff;font-weight:600;font-size:15px;cursor:pointer">Enter</button></form>"""

SESS = set()


def authed(req):
    return req.cookies.get("sid") in SESS


async def _reload_drive_state_after_login():
    """Login ke baad agar startup par Drive state load nahi ho paayi thi to load kar lo."""
    global STATE_FILE_ID
    if STATE_FILE_ID:
        return
    try:
        fid, st, _ = await asyncio.wait_for(asyncio.to_thread(drive_load_state_sync), 30)
        if fid:
            STATE_FILE_ID = fid
            S["state_file_id"] = fid
        if st and S["status"] == "idle":
            apply_state_dict(st)
            log(f"Drive state load ho gayi ({STATE_FILE_NAME}): mode={S['mode']}, done={S['done_count']}", "ok")
    except Exception as e:
        log(f"post-login state load warn: {e}", "warn")


async def h_login(req):
    if req.method == "POST":
        d = await req.post()
        if d.get("p") == PASSWORD:
            sid = secrets.token_hex(16)
            SESS.add(sid)
            r = web.HTTPFound("/")
            r.set_cookie("sid", sid, httponly=True, max_age=86400 * 7)
            raise r
    return web.Response(text=LOGIN, content_type="text/html")


async def h_index(req):
    # Support root callback (http://localhost:8080/?state=...&code=...) jaise purane get_token.py me hota tha
    if req.query.get("code") and req.query.get("state") in OAUTH_FLOWS:
        return await h_oauth_callback(req)
    if not authed(req):
        raise web.HTTPFound("/login")
    return web.Response(text=HTML, content_type="text/html")


async def h_oauth_start(req):
    if not authed(req):
        raise web.HTTPFound("/login")
    try:
        auth_url, _, redirect_uri, src, kind = build_oauth_url(req, manual=False)
        log(f"Google Drive OAuth shuru ({src}, {kind}, redirect={redirect_uri})", "ok")
        raise web.HTTPFound(auth_url)
    except web.HTTPException:
        raise
    except Exception as e:
        from urllib.parse import quote
        log(f"OAuth start fail: {e}", "err")
        raise web.HTTPFound("/?auth_err=" + quote(str(e)[:140]))


async def h_oauth_callback(req):
    from urllib.parse import quote
    err = req.query.get("error")
    if err:
        log(f"Google OAuth cancel/error: {err}", "err")
        raise web.HTTPFound("/?auth_err=" + quote(err))
    code = req.query.get("code", "")
    state = req.query.get("state", "")
    if not code or state not in OAUTH_FLOWS:
        if not authed(req):
            raise web.HTTPFound("/login")
        raise web.HTTPFound("/?auth_err=" + quote("Invalid ya expired OAuth state — dobara login karo"))
    try:
        _, _, email, saved_paths, env_updated = await asyncio.to_thread(exchange_oauth_code_sync, code, state)
        extra = f", {os.path.basename(env_updated)} updated" if env_updated else ""
        log(f"Google Drive login OK: {email} (token.json saved{extra})", "ok")
        await _reload_drive_state_after_login()
        raise web.HTTPFound("/?auth=ok")
    except web.HTTPException:
        raise
    except Exception as e:
        log(f"Google OAuth callback fail: {e}", "err")
        raise web.HTTPFound("/?auth_err=" + quote(str(e)[:160]))


async def h_drive_auth_status(req):
    if not authed(req):
        return web.Response(status=401)
    has_creds, cred_src, cred_kind, ruri = False, "", "", ""
    try:
        cfg, cred_src, cred_kind = get_client_config()
        has_creds = True
        ruri = pick_redirect_uri(req, cfg, cred_kind, manual=False)
    except Exception:
        proto = req.headers.get("X-Forwarded-Proto", req.scheme).split(",")[0].strip()
        host = req.headers.get("X-Forwarded-Host", req.host).split(",")[0].strip()
        ruri = f"{proto}://{host}/oauth/callback"
    return web.json_response({
        "ok": True,
        "drive_ok": S["drive_ok"] and not S["drive_expired"],
        "email": S["drive_email"],
        "error": S["drive_err"],
        "has_creds": has_creds,
        "cred_source": cred_src,
        "cred_kind": cred_kind,
        "redirect_uri": ruri,
    })


async def h_drive_auth_url(req):
    if not authed(req):
        return web.Response(status=401)
    try:
        d = await req.json()
    except Exception:
        d = {}
    manual = bool(d.get("manual"))
    custom_ruri = (d.get("redirect_uri") or "").strip()
    try:
        url, state, ruri, src, kind = build_oauth_url(req, manual=manual, custom_redirect=custom_ruri)
        return web.json_response({"ok": True, "url": url, "state": state, "redirect_uri": ruri, "source": src, "kind": kind})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_drive_auth_exchange(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    raw = (d.get("input") or "").strip()
    if not raw:
        return web.json_response({"ok": False, "error": "Redirect URL ya code daalo"})
    code, state = raw, ""
    if "code=" in raw:
        parsed = urlparse(raw if "://" in raw else "http://localhost/?" + raw.lstrip("?"))
        qs = parse_qs(parsed.query)
        code = (qs.get("code") or [""])[0]
        state = (qs.get("state") or [""])[0]
    try:
        _, _, email, _, env_updated = await asyncio.to_thread(exchange_oauth_code_sync, code, state)
        extra = f", {os.path.basename(env_updated)} updated" if env_updated else ""
        log(f"Google Drive manual login OK: {email} (token.json saved{extra})", "ok")
        await _reload_drive_state_after_login()
        return web.json_response({"ok": True, "email": email})
    except Exception as e:
        log(f"Google OAuth manual exchange fail: {e}", "err")
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_drive_auth_paste(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    text = (d.get("text") or "").strip()
    try:
        parsed = json.loads(text)
        if not isinstance(parsed, dict) or not parsed.get("refresh_token"):
            return web.json_response({"ok": False, "error": "Valid token.json daalo jisme refresh_token ho"})
        await asyncio.to_thread(save_token_dict, parsed)
        ok, email, err = await asyncio.to_thread(verify_drive_connection_sync)
        if not ok:
            return web.json_response({"ok": False, "error": f"Token verify fail: {err}"})
        log(f"Google Drive token manually paste & verify OK: {email}", "ok")
        await _reload_drive_state_after_login()
        return web.json_response({"ok": True, "email": email})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_drive_auth_credentials(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    text = (d.get("text") or "").strip()
    try:
        cfg = json.loads(text)
        if not isinstance(cfg, dict) or ("web" not in cfg and "installed" not in cfg):
            return web.json_response({"ok": False, "error": "Valid Google OAuth client_secret.json chahiye ('installed' ya 'web' key ke saath)"})
        kind = "web" if "web" in cfg else "installed"
        os.environ["GOOGLE_CREDENTIALS_JSON"] = json.dumps(cfg, separators=(",", ":"))
        for p in (os.path.join(DATA_DIR, "client_secret.json"), os.path.join(HERE, "client_secret.json")):
            try:
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, indent=2)
            except OSError:
                pass
        log(f"OAuth client_secret.json upload ho gayi ({kind})", "ok")
        return web.json_response({"ok": True, "kind": kind})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_drive_auth_token_str(req):
    if not authed(req):
        return web.Response(status=401)
    try:
        t = load_token_dict()
        compact = json.dumps(t, separators=(",", ":"))
        return web.json_response({"ok": True, "token_json": compact})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_status(req):
    if not authed(req):
        return web.Response(status=401)
    return web.json_response(status_json())


async def h_state_raw_get(req):
    global STATE_FILE_ID
    if not authed(req):
        return web.Response(status=401)
    if req.query.get("refresh") == "1":
        try:
            fid, _, raw_txt = await asyncio.to_thread(drive_load_state_sync)
            if fid:
                STATE_FILE_ID = fid
                S["state_file_id"] = fid
                return web.json_response({
                    "ok": True, "file_id": fid, "filename": STATE_FILE_NAME,
                    "drive_url": f"https://drive.google.com/file/d/{fid}/view",
                    "text": raw_txt,
                })
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)[:200]})
    txt = json.dumps({**state_dict(), "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}, indent=2, ensure_ascii=False)
    fid = STATE_FILE_ID or ""
    return web.json_response({
        "ok": True, "file_id": fid, "filename": STATE_FILE_NAME,
        "drive_url": f"https://drive.google.com/file/d/{fid}/view" if fid else "",
        "text": txt,
    })


async def h_state_raw_post(req):
    if not authed(req):
        return web.Response(status=401)
    if JOB and not JOB.done():
        return web.json_response({"ok": False, "error": "Pehle chal raha transfer Stop karo, phir state edit karo"})
    d = await req.json()
    text = d.get("text") or ""
    try:
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            return web.json_response({"ok": False, "error": "State valid JSON object ({...}) honi chahiye"})
    except Exception as e:
        return web.json_response({"ok": False, "error": f"Invalid JSON: {e}"})
    apply_state_dict(parsed)
    ok = await save_state(True)
    if not ok:
        return web.json_response({"ok": False, "error": "Drive me save fail ho gaya (logs dekho)"})
    log(f"Drive state ({STATE_FILE_NAME}) manually update ho gayi", "ok")
    return web.json_response({"ok": True})


async def h_csv_parse(req):
    if not authed(req):
        return web.Response(status=401)
    try:
        ct = (req.headers.get("Content-Type") or "").lower()
        if "multipart/form-data" in ct:
            reader = await req.multipart()
            field = await reader.next()
            if field is None:
                return web.json_response({"ok": False, "error": "CSV file nahi mili"})
            fname = field.filename or "upload.csv"
            raw = await field.read(decode=False)
            text = raw.decode("utf-8-sig", errors="replace")
        else:
            d = await req.json()
            fname = (d.get("filename") or "upload.csv").strip()
            text = d.get("text") or ""
        parsed = parse_csv_text(text, filename=fname)
        S["csv_staged"] = parsed
        await save_state(True)
        log(f"CSV parsed & saved to Drive: {parsed['filename']} ({len(parsed['items'])} files, {parsed['folders_count']} folders)", "ok")
        return web.json_response({"ok": True, "csv": csv_summary_view(parsed)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_csv_clear(req):
    if not authed(req):
        return web.Response(status=401)
    S["csv_staged"] = empty_csv_staged()
    S["csv_opts"]["start_index_id"] = ""
    await save_state(True)
    return web.json_response({"ok": True})


async def h_account_pause(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    aid = d.get("id")
    paused = d.get("paused")
    if aid == "__all__":
        target = bool(paused)
        for k, a in S["accs"].items():
            if a["status"] != "dead":
                set_account_paused(k, target)
        await save_state(True)
        return web.json_response({"ok": True, "id": "__all__", "paused": target})
    if aid not in S["accs"]:
        return web.json_response({"ok": False, "error": "Account nahi mila"})
    if paused is None:
        paused = not S["accs"][aid]["paused"]
    ok, err = set_account_paused(aid, bool(paused))
    if ok:
        await save_state(True)
    return web.json_response({"ok": ok, "id": aid, "paused": S["accs"][aid]["paused"], "error": err})


async def h_drive_list(req):
    if not authed(req):
        return web.Response(status=401)
    parent = req.query.get("parent", "root")
    scope = req.query.get("scope", "mydrive")
    search = req.query.get("q", "").strip()
    try:
        res = await asyncio.to_thread(drive_list_folders_sync, parent, scope, search)
        return web.json_response({"ok": True, **res})
    except Exception as e:
        log(f"drive folder list err: {e}", "err")
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_drive_create(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    name = (d.get("name") or "").strip()
    parent = (d.get("parent") or "root").strip()
    if not name:
        return web.json_response({"ok": False, "error": "Folder naam khaali hai"})
    try:
        f = await asyncio.to_thread(drive_create_folder_sync, name, parent)
        log(f"Drive folder banaya: {f['name']} ({f['id']})", "ok")
        return web.json_response({"ok": True, "folder": f})
    except Exception as e:
        log(f"drive folder create err: {e}", "err")
        return web.json_response({"ok": False, "error": str(e)[:200]})


async def h_drive_info(req):
    if not authed(req):
        return web.Response(status=401)
    fid = parse_folder(req.query.get("id", ""))
    try:
        info = await asyncio.to_thread(drive_folder_info_sync, fid)
        return web.json_response({"ok": True, "folder": info})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)[:200]})


def reset_progress():
    """Purana progress (last_done, last_index_id, done_keys, cur, .part files) saaf - naye channel/fresh upload ke liye."""
    S.update(last_done=0, last_index_id="", done_count=0, total_bytes=0, _ent_id=None, channel_title="", done_keys=[])
    S["scan"] = {"files": 0, "bytes": 0}
    cur_state["cur"] = None
    save_state.last = None
    with FOLDER_LOCK:
        FOLDER_CACHE.clear()
    try:
        for fn in os.listdir(DATA_DIR):
            full_p = os.path.join(DATA_DIR, fn)
            if fn.endswith(".part") or fn.endswith(".part.7z"):
                os.remove(full_p)
            elif fn.endswith("_enc_tmp") and os.path.isdir(full_p):
                shutil.rmtree(full_p, ignore_errors=True)
    except OSError:
        pass


async def start_job(mode="channel", channel="", folder="", folder_name="", start_id=0, queue=None, csv_opts=None, fresh=False, enc_enabled=None, enc_pass=None):
    global JOB
    if JOB and not JOB.done():
        return False, "Pehle se chal raha hai"
    mode = mode if mode in ("channel", "list", "csv") else "channel"
    if enc_enabled is not None:
        if enc_enabled and not (enc_pass or "").strip():
            return False, "7z Encryption ON hai — pehle password daalo"
        S["enc_enabled"] = bool(enc_enabled)
        S["enc_pass"] = (enc_pass or "").strip() if enc_enabled else ""
    if mode == "csv":
        opts = csv_opts if isinstance(csv_opts, dict) else S["csv_opts"]
        use_path = bool(opts.get("use_path", True))
        use_title = bool(opts.get("use_title", True))
        cats = [str(c) for c in (opts.get("cats") or []) if str(c).strip()]
        start_index_id = str(opts.get("start_index_id") or "").strip()
        src_items = queue if queue else (S["csv_staged"].get("items") or [])
        filtered_items = filter_csv_items(src_items, cats=cats, start_index_id=start_index_id)
        q = normalize_queue(filtered_items)
        if not q:
            if start_index_id:
                return False, f"INDEX ID '{start_index_id}' ke baad koi matching file nahi mili"
            return False, "CSV me koi matching file nahi mili — pehle CSV upload karo"
        if fresh:
            log("fresh start: purana CSV progress clear kiya", "warn")
            S["done_keys"] = []
            S["last_index_id"] = ""
            cur_state["cur"] = None
            with FOLDER_LOCK:
                FOLDER_CACHE.clear()
        elif S["mode"] != "csv":
            cur_state["cur"] = None
        S["csv_opts"] = {"use_path": use_path, "use_title": use_title, "cats": cats, "start_index_id": start_index_id}
        S.update(mode="csv", queue=q, folder=folder, folder_name=folder_name)
    elif mode == "list":
        q = normalize_queue(queue)
        if not q:
            return False, "File list khaali hai ya valid link/range nahi mila"
        if fresh or S["mode"] != "list" or S["queue"] != q:
            if fresh:
                log("fresh start: purana list progress clear kiya", "warn")
            S["done_keys"] = []
            cur_state["cur"] = None
        S.update(mode="list", queue=q, folder=folder, folder_name=folder_name)
    else:
        if not channel:
            return False, "Channel daalo pehle"
        if fresh or S["mode"] != "channel" or (S["channel"] and S["channel"].strip() != channel.strip()):
            log("fresh start: purana progress (last_done / cache) clear kiya", "warn")
            reset_progress()
        S.update(mode="channel", channel=channel, folder=folder, folder_name=folder_name, start_id=start_id)
    await save_state(True)
    JOB = asyncio.create_task(run_job())
    return True, ""


async def h_start(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    mode = d.get("mode", "channel")
    folder = parse_folder(d.get("folder", ""))
    folder_name = (d.get("folder_name") or "").strip()
    ok, err = await start_job(
        mode=mode,
        channel=(d.get("channel") or "").strip(),
        folder=folder,
        folder_name=folder_name,
        start_id=int(d.get("start_id") or 0),
        queue=d.get("queue"),
        csv_opts=d.get("csv_opts"),
        fresh=bool(d.get("fresh")),
        enc_enabled=bool(d.get("enc_enabled", False)),
        enc_pass=(d.get("enc_pass") or "").strip(),
    )
    return web.json_response({"ok": ok, "error": err})


async def h_stop(req):
    if not authed(req):
        return web.Response(status=401)
    S["stop"] = True
    if S["status"] == "running":
        S["phase"] = "stopping…"
    for t in list(CUR_TASKS):
        t.cancel()
    return web.json_response({"ok": True})


async def startup_checks():
    log("---- startup checks ----")
    try:
        tp = os.path.join(DATA_DIR, ".write_test")
        with open(tp, "wb") as f:
            f.write(b"ok")
            f.flush()
            os.fsync(f.fileno())
        os.remove(tp)
        log(f"disk OK: {DATA_DIR}  (free {fmt(shutil.disk_usage(DATA_DIR).free)})", "ok")
    except Exception as e:
        log(f"disk FAIL: {e}", "err")
    log("cryptg: " + ("OK (fast decrypt)" if HAS_CRYPTG else "nahi hai (optional, speed kam)"), "ok" if HAS_CRYPTG else "warn")
    _sz = find_7z_bin()
    log("7z encrypt engine: " + (f"native CLI ({_sz})" if _sz else "py7zr (pure Python fallback)"), "ok")
    for aid, cl in CLIENTS.items():
        a = S["accs"][aid]
        try:
            me = await asyncio.wait_for(cl.get_me(), 20)
            p_tag = " [PAUSED]" if a.get("paused") else ""
            log(f"telegram OK: {a['name']} (@{me.username or me.id}){p_tag}", "ok")
        except Exception as e:
            if is_dead(e):
                acc_dead(a, e)
            else:
                log(f"telegram FAIL: {a['name']}: {e}", "err")
    try:
        ok, em, err = await asyncio.wait_for(asyncio.to_thread(verify_drive_connection_sync), 30)
        if ok:
            log(f"drive OK: {em}", "ok")
        else:
            log(f"drive FAIL: {err}  (UI se 'Login Google Drive' karo ya `python tg2gd.py --get-token` chalao)", "err")
    except Exception as e:
        S["drive_ok"] = False
        S["drive_expired"] = True
        S["drive_err"] = str(e)[:200]
        log(f"drive FAIL: {e}  (UI se 'Login Google Drive' karo)", "err")
    log("------------------------")


async def on_start(app):
    global PRIMARY, PRIMARY_AID, STATE_FILE_ID, STATE_LOCK
    STATE_LOCK = asyncio.Lock()
    cfg = load_json("ACCOUNTS_JSON", "accounts.json")
    ticked = set(cfg.get("par", {}).get("ticked", [a["id"] for a in cfg["list"]]))
    for a in cfg["list"]:
        if a["id"] not in ticked:
            continue
        cl = TelegramClient(StringSession(a["session"]), int(a["api_id"]), a["api_hash"],
                            connection_retries=10, retry_delay=2, flood_sleep_threshold=0)
        try:
            await cl.connect()
            if not await cl.is_user_authorized():
                log(f"{a['name']}: session invalid, skip", "err")
                continue
        except Exception as e:
            log(f"{a['name']}: connect fail {e}", "err")
            continue
        CLIENTS[a["id"]] = cl
        S["accs"][a["id"]] = {"idx": len(S["accs"]), "name": a["name"], "phone": a["phone"], "bytes": 0,
                              "speed": 0, "segs": 0, "conn": CONN,
                              "status": "ok", "paused": False,
                              "flood_until": 0, "floods": 0, "errors": 0, "last_err": ""}
        if a["id"] == cfg.get("active") or PRIMARY is None:
            PRIMARY, PRIMARY_AID = cl, a["id"]
    if not CLIENTS:
        raise SystemExit("koi bhi account connect nahi hua")
    log(f"{len(CLIENTS)} accounts ready", "ok")
    log(f"UI: http://localhost:{PORT}  (password: {PASSWORD})")
    app["tasks"] = [asyncio.create_task(speed_loop())]
    await startup_checks()
    if S["drive_ok"]:
        log(f"saved state dhoond raha hoon (Drive root: {STATE_FILE_NAME})...")
        st = None
        try:
            fid, st, _ = await asyncio.wait_for(asyncio.to_thread(drive_load_state_sync), 60)
            if fid:
                STATE_FILE_ID = fid
                S["state_file_id"] = fid
        except Exception as e:
            log(f"drive state load fail/timeout: {e}", "warn")
            st = None
        if st:
            apply_state_dict(st)
            log(f"Drive state mila ({STATE_FILE_NAME}): mode={S['mode']}, done={S['done_count']}, last_idx={S['last_index_id'] or '-'}", "ok")
            if st.get("running"):
                if S["mode"] == "csv" and (S["queue"] or S["csv_staged"].get("items")):
                    log("restart se pehle CSV mode running tha -> auto resume", "warn")
                    await start_job(mode="csv", folder=S["folder"], folder_name=S["folder_name"],
                                    queue=S["queue"] or None, csv_opts=S["csv_opts"])
                elif S["mode"] == "list" and S["queue"]:
                    log("restart se pehle list mode running tha -> auto resume", "warn")
                    await start_job(mode="list", folder=S["folder"], folder_name=S["folder_name"], queue=S["queue"])
                elif S["mode"] == "channel" and S["channel"]:
                    log("restart se pehle channel mode running tha -> auto resume", "warn")
                    await start_job(mode="channel", channel=S["channel"], folder=S["folder"], folder_name=S["folder_name"], start_id=S["start_id"])
        else:
            log(f"Drive root me {STATE_FILE_NAME} nahi mili -> fresh start")
    else:
        log("Drive token missing/expired — UI khol kar 'Login Google Drive' karo", "warn")
    app["tasks"].append(asyncio.create_task(state_loop()))


def main():
    app = web.Application(client_max_size=50 * 1024 * 1024)
    app.add_routes([
        web.get("/", h_index),
        web.route("*", "/login", h_login),
        web.get("/oauth/start", h_oauth_start),
        web.get("/oauth/callback", h_oauth_callback),
        web.get("/api/drive/auth/status", h_drive_auth_status),
        web.post("/api/drive/auth/url", h_drive_auth_url),
        web.post("/api/drive/auth/exchange", h_drive_auth_exchange),
        web.post("/api/drive/auth/paste", h_drive_auth_paste),
        web.post("/api/drive/auth/credentials", h_drive_auth_credentials),
        web.get("/api/drive/auth/token_str", h_drive_auth_token_str),
        web.get("/api/status", h_status),
        web.get("/api/state/raw", h_state_raw_get),
        web.post("/api/state/raw", h_state_raw_post),
        web.post("/api/start", h_start),
        web.post("/api/stop", h_stop),
        web.post("/api/csv/parse", h_csv_parse),
        web.post("/api/csv/clear", h_csv_clear),
        web.post("/api/account/pause", h_account_pause),
        web.get("/api/drive/folders", h_drive_list),
        web.post("/api/drive/folders", h_drive_create),
        web.get("/api/drive/folder_info", h_drive_info),
    ])
    app.on_startup.append(on_start)
    web.run_app(app, port=PORT, host="0.0.0.0", print=None)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ("--get-token", "get-token", "--login", "login"):
        cli_port = int(sys.argv[2]) if len(sys.argv) > 2 else None
        run_cli_get_token(cli_port)
    else:
        main()