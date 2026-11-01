"""
Telegram -> (Render/PC disk) -> Google Drive   |  multi-account, resumable, live UI

Chalane ke liye (Windows / Linux / Render):
    python tg2gd.py
Missing packages khud install ho jaate hain. Same folder me accounts.json + token.json rakho
(ya env: ACCOUNTS_JSON / TOKEN_JSON, Render Secret Files: /etc/secrets/).

Env (sab optional):
    PORT=8080  DATA_DIR=./data  APP_PASSWORD=808177  STATE_INTERVAL=3
"""
import os, sys, re, json, time, asyncio, secrets, logging, collections, shutil, subprocess, importlib


def ensure_deps():
    need = {"aiohttp": "aiohttp", "telethon": "telethon",
            "googleapiclient": "google-api-python-client", "google.auth": "google-auth"}
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


def load_dotenv():
    """Script ke folder ki .env padh kar os.environ me daalta hai (Render env vars ko override nahi karta)."""
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(p):
        return
    for line in open(p, encoding="utf-8"):
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
from telethon.sessions import StringSession
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

logging.basicConfig(level=logging.ERROR)
PORT = int(os.environ.get("PORT", 8080))
PASSWORD = os.environ.get("APP_PASSWORD", "808177")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.getcwd(), "data"))
STATE_INTERVAL = float(os.environ.get("STATE_INTERVAL", 3))
SEG = 8 * 1024 * 1024            # 1 segment = 8 MB (accounts me baante jaate hain)
REQ = 1024 * 1024                # telegram request size
STATE_TAG = "TG2GD_STATE"
DEAD = {"AuthKeyUnregisteredError", "SessionRevokedError", "UserDeactivatedError",
        "UserDeactivatedBanError", "AuthKeyDuplicatedError", "SessionExpiredError"}
os.makedirs(DATA_DIR, exist_ok=True)


def load_json(env, fname):
    v = os.environ.get(env)
    if v:
        return json.loads(v)
    here = os.path.dirname(os.path.abspath(__file__))
    for p in (fname, os.path.join(here, fname), os.path.join("/etc/secrets", fname)):
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    raise SystemExit(f"{env} ya {fname} nahi mila")


# ------------------------------------------------------------------ global state
S = {
    "status": "idle", "phase": "", "channel": "", "channel_title": "", "folder": "", "start_id": 0,
    "last_done": 0, "done_count": 0, "total_bytes": 0,          # lifetime (saved messages me persist)
    "scan": {"files": 0, "bytes": 0},
    "plan": {"files": 0, "bytes": 0, "dl_files": 0, "dl_bytes": 0, "up_files": 0, "up_bytes": 0},
    "started_at": 0, "ended_at": 0, "speed": 0, "state_ts": 0,
    "dl": None, "ul": None, "next": [],
    "accs": {}, "log": collections.deque(maxlen=300), "hist": collections.deque(maxlen=120),
    "stop": False, "failed": False,
}
CLIENTS = {}
PRIMARY = None
PRIMARY_AID = None
STATE_MSG_ID = None
STATE_BLOCK = [0.0]
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


async def flood_safe(fn, who="", tries=10):
    """Control-plane calls (messages fetch etc.) FloodWait par ruk kar dobara try karte hain."""
    for _ in range(tries):
        try:
            return await fn()
        except FloodWaitError as e:
            log(f"[{who}] FloodWait {e.seconds}s - ruk raha hoon", "warn")
            for _ in range(e.seconds + 1):
                if S["stop"]:
                    raise
                await asyncio.sleep(1)
    return await fn()


# ------------------------------------------------------------------ saved-messages state
def state_dict():
    cur = cur_state["cur"]
    d = {"channel": S["channel"], "folder": S["folder"], "last_done": S["last_done"],
         "done_count": S["done_count"], "total_bytes": S["total_bytes"],
         "running": S["status"] == "running", "cur": cur}
    if len(json.dumps(d)) > 3800:           # telegram msg limit
        d["cur"] = None
    return d


async def save_state(force=False):
    global STATE_MSG_ID
    if not PRIMARY or time.time() < STATE_BLOCK[0]:
        return
    d = state_dict()
    body = json.dumps(d, separators=(",", ":"))
    if body == getattr(save_state, "last", None) and not force:
        S["state_ts"] = time.time()
        return
    txt = STATE_TAG + "\n" + json.dumps({**d, "ts": int(time.time())}, separators=(",", ":"))
    try:
        if STATE_MSG_ID:
            await PRIMARY.edit_message("me", STATE_MSG_ID, txt)
        else:
            m = await PRIMARY.send_message("me", txt)
            STATE_MSG_ID = m.id
        save_state.last = body
        S["state_ts"] = time.time()
    except FloodWaitError as e:
        STATE_BLOCK[0] = time.time() + e.seconds + 1
        log(f"state save: FloodWait {e.seconds}s (baad me save hoga)", "warn")
    except Exception as e:
        s = str(e).lower()
        if "not modified" in s:
            save_state.last = body
            S["state_ts"] = time.time()
        elif "message_id_invalid" in s or "not found" in s:
            STATE_MSG_ID = None
        else:
            log(f"state save err: {e}", "err")


async def load_state():
    global STATE_MSG_ID
    async for m in PRIMARY.iter_messages("me", search=STATE_TAG, limit=5):
        if m.text and m.text.startswith(STATE_TAG):
            try:
                d = json.loads(m.text.split("\n", 1)[1])
            except Exception:
                continue
            STATE_MSG_ID = m.id
            return d
    return None


async def state_loop():
    while True:
        await asyncio.sleep(STATE_INTERVAL)
        if S["status"] == "running":
            await save_state()


# ------------------------------------------------------------------ helpers
def safe_name(s):
    s = re.sub(r'[\\/:*?"<>|\r\n\t]', " ", s).strip(" .")
    return re.sub(r"\s+", " ", s)[:150]


def drive_name(msg):
    f = msg.file
    orig = (f.name or "") if f else ""
    ext = (f.ext or "") if f else ""
    if not ext and "." in orig:
        ext = "." + orig.rsplit(".", 1)[1]
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
    m = re.search(r"(?:t\.me|telegram\.me)/([A-Za-z0-9_]+)", s)
    if m:
        return "name", "@" + m.group(1)
    if re.fullmatch(r"-?\d+", s):
        return "id", int(s)
    return "name", s if s.startswith("@") else "@" + s


async def resolve_channel(client, spec, who=""):
    from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
    from telethon.tl.types import ChatInviteAlready
    kind, val = parse_channel(spec)
    if kind == "invite":
        r = await client(CheckChatInviteRequest(val))
        if isinstance(r, ChatInviteAlready):
            return r.chat
        log(f"[{who}] channel ka member nahi tha -> invite link se join kar raha hoon", "warn")
        try:
            u = await client(ImportChatInviteRequest(val))
            return u.chats[0]
        except FloodWaitError:
            raise
        except Exception as e:
            if "already" in str(e).lower():
                r = await client(CheckChatInviteRequest(val))
                return r.chat
            raise
    try:
        return await client.get_entity(val)
    except FloodWaitError:
        raise
    except Exception:
        await client.get_dialogs()
        return await client.get_entity(val)


async def get_peer(client):
    from telethon.tl.types import PeerChannel
    try:
        return await client.get_input_entity(PeerChannel(int(S["_ent_id"])))
    except Exception:
        return await resolve_channel(client, S["channel"])


# ------------------------------------------------------------------ account control
async def acc_flood(a, secs):
    a["floods"] += 1
    a["status"] = "flood"
    a["flood_until"] = time.time() + secs + 1
    log(f"[{a['name']}] FloodWait {secs}s - ye account ruka, baaki chalte rahenge", "warn")
    for _ in range(secs + 1):
        if S["stop"]:
            break
        await asyncio.sleep(1)
    if a["status"] == "flood":
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


# ------------------------------------------------------------------ download (segments across accounts)
async def download_msg(msg, size, path):
    """Success par real size return, stop par None."""
    dl = S["dl"]
    # photo: chhoti file, seedha primary se
    if msg.photo and not msg.document:
        await flood_safe(lambda: PRIMARY.download_media(msg, file=path), "primary")
        real = os.path.getsize(path)
        dl.update(done=real, size=real, nseg=1, segs_done=1)
        CUR_OWNER[0] = bytearray([S["accs"][PRIMARY_AID]["idx"] + 1])
        S["accs"][PRIMARY_AID]["bytes"] += real
        return real

    if size <= SEG:                      # chhoti file: sirf primary se, overhead kam
        pa = S["accs"][PRIMARY_AID]
        for attempt in range(3):
            if pa["status"] != "ok":
                break
            cur_state["cur"] = None
            CUR_OWNER[0] = bytearray([pa["idx"] + 1])
            dl.update(done=0, size=size, nseg=1, segs_done=0)
            base = [0]

            def cb(rec, tot):
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
                return real
            except asyncio.CancelledError:
                CUR_TASKS.clear()
                if S["stop"]:
                    return None
                raise
            except Exception as e:
                CUR_TASKS.clear()
                pa["bytes"] -= base[0]
                dl["done"] -= base[0]
                if isinstance(e, FloodWaitError):
                    flood_mark(pa, e.seconds)       # baaki accounts wali raah se aage badho
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
    if c and c["id"] == msg.id and c["size"] == size and os.path.exists(path) \
            and os.path.getsize(path) == size and len(c["segs"]) == len(bm) * 2:
        bm = bytearray.fromhex(c["segs"])
        for i in range(nseg):
            if bm_get(bm, i):
                owner[i] = 255
        log(f"resume: {sum(1 for i in range(nseg) if bm_get(bm, i))}/{nseg} segments pehle se disk par hain", "ok")
    else:
        with open(path, "wb") as f:
            f.truncate(size)
    cur_state["cur"] = {"id": msg.id, "size": size, "segs": bm.hex()}
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
        while a["status"] == "flood" and remaining[0] > 0 and not S["stop"]:
            await asyncio.sleep(1)

        async def fetch():
            peer = await get_peer(client)
            r = await client.get_messages(peer, ids=msg.id)
            if not r or not r.media:
                raise RuntimeError("msg nahi mila (account channel me hai?)")
            return r

        for attempt in range(6):
            if S["stop"] or fatal:
                return
            try:
                m = await fetch()
                break
            except FloodWaitError as e:
                await acc_flood(a, e.seconds)
            except Exception as e:
                if is_dead(e):
                    acc_dead(a, e)
                    return
                a["errors"] += 1
                a["last_err"] = str(e)[:90]
                log(f"[{a['name']}] msg fetch fail: {e}", "err")
                await asyncio.sleep(2)
        if m is None:
            log(f"[{a['name']}] is file me madad nahi kar paya - skip", "warn")
            return

        while remaining[0] > 0 and not S["stop"] and not fatal and a["status"] != "dead":
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
                    chunk = bytes(chunk)[: end - pos]
                    async with wlock:
                        fh.seek(pos)
                        fh.write(chunk)
                    pos += len(chunk)
                    a["bytes"] += len(chunk)
                    dl["done"] += len(chunk)
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
            except Exception as e:
                got = pos - start
                a["bytes"] -= got
                dl["done"] -= got
                if isinstance(e, FloodWaitError):
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


# ------------------------------------------------------------------ drive upload
def drive_service():
    t = load_json("TOKEN_JSON", "token.json")
    creds = Credentials(
        token=t.get("token"), refresh_token=t["refresh_token"], token_uri=t["token_uri"],
        client_id=t["client_id"], client_secret=t["client_secret"], scopes=t.get("scopes"))
    if not creds.valid:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds, cache_discovery=False)


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
    t = load_json("TOKEN_JSON", "token.json")
    creds = Credentials(
        token=t.get("token"), refresh_token=t["refresh_token"], token_uri=t["token_uri"],
        client_id=t["client_id"], client_secret=t["client_secret"], scopes=t.get("scopes"))
    if not creds.valid:
        creds.refresh(Request())
    sess = AuthorizedSession(creds)
    total = os.path.getsize(path)
    meta = {"name": name}
    if folder:
        meta["parents"] = [folder]
    r = sess.post("https://www.googleapis.com/upload/drive/v3/files",
                  params={"uploadType": "resumable", "supportsAllDrives": "true", "fields": "id,name"},
                  json=meta, headers={"X-Upload-Content-Length": str(total)}, timeout=60)
    if r.status_code >= 400:
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
            raise RuntimeError(f"drive upload {code}: {r.text[:200]}")


async def upload_file(msg_id, path, name, size):
    ul = {"id": msg_id, "name": name, "size": size, "done": 0, "speed": 0}
    S["ul"] = ul
    try:
        r = await asyncio.to_thread(upload_blocking, path, name, S["folder"], ul)
    finally:
        S["ul"] = None
    if r:
        log(f"Drive OK: {name}", "ok")
    return bool(r)


# ------------------------------------------------------------------ job
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


async def run_once():
    S.update(stop=False, failed=False, status="running", phase="starting")
    upq = asyncio.Queue(maxsize=1)
    up_task = None
    try:
        ent = await flood_safe(lambda: resolve_channel(PRIMARY, S["channel"], "primary"), "primary")
        S["_ent_id"] = ent.id
        S["channel_title"] = getattr(ent, "title", "") or str(ent.id)
        keep = f"{cur_state['cur']['id']}.part" if cur_state["cur"] else None
        for fn in os.listdir(DATA_DIR):
            if fn.endswith(".part") and fn != keep:
                try:
                    os.remove(os.path.join(DATA_DIR, fn))
                except OSError:
                    pass
        for aid, cl in CLIENTS.items():
            a = S["accs"][aid]
            if cl is PRIMARY or a["status"] == "dead":
                continue
            try:
                await flood_safe(lambda: resolve_channel(cl, S["channel"], a["name"]), a["name"])
            except Exception as e:
                if is_dead(e):
                    acc_dead(a, e)
                else:
                    a["errors"] += 1
                    a["last_err"] = str(e)[:90]
                    log(f"[{a['name']}] channel resolve fail: {e}", "err")
        min_id = max(S["last_done"], S["start_id"] - 1 if S["start_id"] else 0)
        log(f"channel: {S['channel_title']}  | {min_id} ke baad se scan", "ok")
        items = await scan_channel(ent, min_id)
        S["plan"] = {"files": len(items), "bytes": sum(s for _, s in items),
                     "dl_files": 0, "dl_bytes": 0, "up_files": 0, "up_bytes": 0}
        S["started_at"] = time.time()
        S["ended_at"] = 0
        log(f"scan done: {len(items)} files, {fmt(S['plan']['bytes'])}", "ok")

        async def uploader():
            while True:
                it = await upq.get()
                if it is None:
                    return
                msg_id, path, name, size = it
                try:
                    ok = await upload_file(msg_id, path, name, size)
                    if ok:
                        os.remove(path)
                        S["last_done"] = max(S["last_done"], msg_id)
                        S["done_count"] += 1
                        S["total_bytes"] += size
                        S["plan"]["up_files"] += 1
                        S["plan"]["up_bytes"] += size
                        c = cur_state["cur"]
                        if c and c["id"] == msg_id:
                            cur_state["cur"] = None
                        await save_state(True)
                except Exception as e:
                    log(f"UPLOAD FAIL {name}: {e}", "err")
                    S["failed"] = True
                    S["stop"] = True

        up_task = asyncio.create_task(uploader())
        S["phase"] = "transferring"
        ids = [i for i, _ in items]
        for g in range(0, len(ids), 50):
            if S["stop"]:
                break
            grp = ids[g:g + 50]
            msgs = await flood_safe(lambda: PRIMARY.get_messages(ent, ids=grp), "primary")
            for k, msg in enumerate(msgs):
                if S["stop"]:
                    break
                if not msg or not msg.media or not msg.file or not msg.file.size:
                    S["plan"]["files"] -= 1
                    continue
                size = msg.file.size
                name = drive_name(msg)
                path = os.path.join(DATA_DIR, f"{msg.id}.part")
                pos = g + k
                S["next"] = [{"id": i, "size": s} for i, s in items[pos + 1:pos + 6]]
                S["dl"] = {"id": msg.id, "name": name, "size": size, "done": 0, "speed": 0,
                           "nseg": 0, "segs_done": 0, "start": time.time()}
                log(f"#{msg.id}  {name}  ({fmt(size)})")
                real = await download_msg(msg, size, path)
                if real is None:
                    break
                S["plan"]["dl_files"] += 1
                S["plan"]["dl_bytes"] += real
                S["dl"] = None
                await upq.put((msg.id, path, name, real))
        S["next"] = []
        S["status"] = "stopped" if S["stop"] else "finished"
    except BaseException as e:
        if S["stop"] and not S["failed"]:
            S["status"] = "stopped"
            if not isinstance(e, Exception):
                pass
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
    for a in S["accs"].values():
        accs.append({**a, "flood_left": max(0, int(a["flood_until"] - now)) if a["status"] == "flood" else 0})
    return {
        "status": S["status"], "phase": S["phase"], "stopping": bool(S["stop"] and S["status"] == "running"), "channel": S["channel"], "folder": S["folder"],
        "channel_title": S["channel_title"], "last_done": S["last_done"],
        "done_count": S["done_count"], "total_bytes": S["total_bytes"],
        "scan": S["scan"], "plan": p, "elapsed": elapsed, "avg": avg, "eta": eta,
        "speed": S["speed"], "rem_files": max(0, p["files"] - p["up_files"]),
        "dl": dl, "ul": S["ul"], "next": S["next"], "accs": accs,
        "hist": list(S["hist"]), "log": list(S["log"]),
        "state_ago": (now - S["state_ts"]) if S["state_ts"] else -1,
        "cryptg": HAS_CRYPTG,
    }


# ------------------------------------------------------------------ web UI
HTML = r"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><meta name=theme-color content="#0b0f17">
<title>TG → Drive</title><style>
:root{--bg:#0a0e16;--card:#111826;--card2:#0d1420;--b:#1c2740;--t:#e8eef8;--m:#8396b5;--ac:#4f9cff;--ok:#2ecc71;--wa:#f5a623;--er:#ff5c6c}
*{box-sizing:border-box}html,body{margin:0;overflow-x:hidden}body{background:var(--bg);color:var(--t);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
.w{max-width:1100px;margin:auto;padding:18px 16px 40px}
header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px}
h1{font-size:19px;margin:0;font-weight:700;letter-spacing:.2px}h1 span{color:var(--m);font-weight:500}
.sub{color:var(--m);font-size:12px;margin-left:auto;text-align:right}
.pill{display:inline-flex;align-items:center;gap:6px;padding:3px 11px;border-radius:99px;background:#1a2438;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.pill:before{content:"";width:7px;height:7px;border-radius:50%;background:var(--m)}
.pill.running{background:#10331f;color:var(--ok)}.pill.running:before{background:var(--ok);animation:p 1.2s infinite}
.pill.finished{background:#10331f;color:var(--ok)}.pill.finished:before{background:var(--ok)}
.pill.error{background:#3a1519;color:var(--er)}.pill.error:before{background:var(--er)}
.pill.stopped{background:#3a2d10;color:var(--wa)}.pill.stopped:before{background:var(--wa)}
@keyframes p{50%{opacity:.25}}
.card{background:var(--card);border:1px solid var(--b);border-radius:14px;padding:14px 16px;margin-bottom:12px}
.card{min-width:0}.card h3{white-space:nowrap;gap:10px;margin:0 0 10px;font-size:11px;font-weight:600;color:var(--m);text-transform:uppercase;letter-spacing:.8px;display:flex;justify-content:space-between;align-items:center}
.ctl{display:grid;grid-template-columns:2fr 2fr 110px auto;gap:10px;align-items:end}
label{font-size:11px;color:var(--m);display:block;margin-bottom:4px}
input{width:100%;background:var(--card2);border:1px solid var(--b);color:var(--t);border-radius:9px;padding:9px 11px;font-size:13px;outline:none}
input:focus{border-color:var(--ac)}
button{border:0;border-radius:9px;padding:10px 18px;font-weight:600;font-size:13px;cursor:pointer;color:#fff;background:var(--ac)}
button:hover{filter:brightness(1.1)}button:disabled{opacity:.4;cursor:not-allowed}button.stop{background:#2a3550}button.stop:hover{background:var(--er)}
.btns{display:flex;gap:8px}
.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:12px}
.st{background:var(--card);border:1px solid var(--b);border-radius:12px;padding:11px 13px}
.st small{display:block;color:var(--m);font-size:11px;text-transform:uppercase;letter-spacing:.6px}
.st b{display:block;font-size:19px;margin-top:2px;font-variant-numeric:tabular-nums}.st i{font-style:normal;color:var(--m);font-size:11px}
.bar{height:9px;background:var(--card2);border-radius:6px;overflow:hidden}.bar>i{display:block;height:100%;width:0;background:var(--ac);border-radius:6px;transition:width .5s}
.bar.g>i{background:var(--ok)}.bar.big{height:12px}
.row{display:flex;justify-content:space-between;gap:10px;color:var(--m);font-size:12px;margin-top:6px;font-variant-numeric:tabular-nums}
.nm{font-weight:600;font-size:13px;color:var(--t);margin-bottom:10px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;word-break:break-word;min-height:2.9em}
.two{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px;margin-bottom:12px}.two>.card{margin-bottom:0}
canvas{display:block;width:100%}
#map{margin-top:10px;border-radius:6px}
.accs{display:grid;grid-template-columns:repeat(auto-fit,minmax(235px,1fr));gap:10px}
.ac{background:var(--card2);border:1px solid var(--b);border-radius:11px;padding:11px 12px}
.ac .h{display:flex;align-items:center;gap:8px;margin-bottom:6px}.dot{width:10px;height:10px;border-radius:3px;flex:none}
.ac .n{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.ac .ph{color:var(--m);font-size:11px}
.chip{margin-left:auto;font-size:10px;font-weight:700;padding:2px 8px;border-radius:99px;text-transform:uppercase;letter-spacing:.4px;background:#10331f;color:var(--ok);white-space:nowrap}
.chip.flood{background:#3a2d10;color:var(--wa)}.chip.dead{background:#3a1519;color:var(--er)}.chip.idle{background:#1a2438;color:var(--m)}
.ac .sp{font-size:18px;font-weight:700;margin:2px 0 6px;font-variant-numeric:tabular-nums}
.ac .er{color:var(--er);font-size:11px;margin-top:5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#log{height:230px;overflow:auto;background:#080b12;border-radius:9px;padding:9px 11px;font:12px/1.65 ui-monospace,Consolas,monospace;color:#9db0cf}
#log b{color:#5a6c8c;font-weight:400;margin-right:8px}#log .ok{color:var(--ok)}#log .warn{color:var(--wa)}#log .err{color:var(--er)}
.nx{display:flex;gap:8px;flex-wrap:wrap}.nx span{background:var(--card2);border:1px solid var(--b);border-radius:8px;padding:3px 9px;font-size:12px;color:var(--m)}
@media(max-width:900px){.stats{grid-template-columns:repeat(3,1fr)}.two{grid-template-columns:1fr}.ctl{grid-template-columns:1fr}}
@media(max-width:520px){.stats{grid-template-columns:repeat(2,1fr)}}
</style></head><body><div class=w>
<header><h1>Telegram <span>→</span> Drive</h1><span id=st class=pill>idle</span><span id=ph style="color:var(--m);font-size:12px"></span>
<div class=sub><div id=ch></div><div id=sv></div></div></header>

<div class=card><div class=ctl>
<div><label>Channel (@username / invite link / id)</label><input id=in_ch placeholder="https://t.me/+xxxxxxxx"></div>
<div><label>Drive folder (URL ya ID)</label><input id=in_fo placeholder="https://drive.google.com/drive/folders/..."></div>
<div><label>Start msg ID</label><input id=in_si type=number value=0 min=0></div>
<div class=btns><button id=b_go onclick=go()>▶ Start / Resume</button><button id=b_st class=stop onclick=stp()>■ Stop</button></div>
</div></div>

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
<div class=card><h3><span>⬇ Downloading</span><span id=dseg></span></h3>
<div class=nm id=dn>—</div><div class=bar><i id=db></i></div>
<div class=row><span id=dt></span><span id=dp></span></div><canvas id=map height=10></canvas></div>
<div class=card><h3><span>⬆ Uploading to Drive</span><span id=uspd></span></h3>
<div class=nm id=un>—</div><div class="bar g"><i id=ub></i></div>
<div class=row><span id=ut></span><span id=up></span></div></div>
</div>
<div class=card><h3><span>Up next</span></h3><div class=nx id=nx></div></div>

<div class=card><h3><span>Accounts</span><span id=acn></span></h3><div class=accs id=ac></div></div>
<div class=card><h3><span>Live trace</span></h3><div id=log></div></div>
</div><script>
const $=i=>document.getElementById(i);
const PAL=['#4f9cff','#2ecc71','#f5a623','#c06cff','#ff6b8a','#2ad1d1'];
const fb=n=>{n=+n||0;const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<4){n/=1024;i++}return n.toFixed(i?1:0)+' '+u[i]};
const ft=s=>{s=Math.round(s);if(!isFinite(s)||s<=0)return '—';const h=Math.floor(s/3600),m=Math.floor(s%3600/60),x=s%60;return h?h+'h '+m+'m':m?m+'m '+x+'s':x+'s'};
const esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
let filled=false,lastMap='';
async function go(){const r=await fetch('/api/start',{method:'POST',body:JSON.stringify({channel:$('in_ch').value,folder:$('in_fo').value,start_id:+$('in_si').value||0})});filled=true}
async function stp(){await fetch('/api/stop',{method:'POST'})}
function drawChart(h){const c=$('chart'),W=c.clientWidth*devicePixelRatio,H=90*devicePixelRatio;c.width=W;c.height=H;const x=c.getContext('2d');
const mx=Math.max(1048576,...h)*1.15;x.strokeStyle='#1c2740';x.lineWidth=1;for(let i=1;i<4;i++){const y=H*i/4;x.beginPath();x.moveTo(0,y);x.lineTo(W,y);x.stroke()}
if(h.length<2)return;const n=120,sx=W/(n-1),off=n-h.length;x.beginPath();h.forEach((v,i)=>{const px=(i+off)*sx,py=H-v/mx*H;i?x.lineTo(px,py):x.moveTo(px,py)});
x.strokeStyle='#4f9cff';x.lineWidth=2*devicePixelRatio;x.stroke();x.lineTo((h.length-1+off)*sx,H);x.lineTo(off*sx,H);x.closePath();x.fillStyle='rgba(79,156,255,.15)';x.fill();$('sp_max').textContent='max '+fb(Math.max(...h))+'/s'}
function drawMap(map,n){if(!n){$('map').height=0;lastMap='';return}if(map===lastMap)return;lastMap=map;const c=$('map'),W=900;
const cell=Math.max(3,Math.min(16,Math.floor(Math.sqrt(W*70/n)))),cols=Math.floor(W/(cell+1)),rows=Math.ceil(n/cols);c.width=W;c.height=rows*(cell+1);const x=c.getContext('2d');
for(let i=0;i<n;i++){const o=parseInt(map.substr(i*2,2),16)||0;x.fillStyle=o==0?'#1a2438':o==255?'#5b6b86':PAL[(o-1)%PAL.length];x.fillRect((i%cols)*(cell+1),Math.floor(i/cols)*(cell+1),cell,cell)}}
async function tick(){let s;try{const r=await fetch('/api/status');if(r.status==401){location='/login';return}s=await r.json()}catch(e){return}
if(!filled&&(s.channel||s.folder)&&document.activeElement.tagName!='INPUT'){$('in_ch').value=s.channel;$('in_fo').value=s.folder;filled=true}
const run=s.status=='running';$('st').textContent=s.stopping?'stopping…':s.status;$('st').className='pill '+(s.stopping?'stopped':s.status);$('b_go').disabled=run;$('b_st').disabled=!run||s.stopping;
const scanning=s.phase=='scanning';$('ph').textContent=scanning?('scanning… '+s.scan.files+' files'):(s.phase||'');
$('ch').textContent=s.channel_title||'';$('sv').textContent=s.state_ago<0?'state: not saved yet':'state saved '+Math.round(s.state_ago)+'s ago (Saved Messages)';
const p=s.plan,dl=s.dl,ul=s.ul;
$('s1').textContent=p.up_files+' / '+p.files;$('s1b').textContent=fb(p.up_bytes)+' uploaded';
$('s2').textContent=s.rem_files;$('s2b').textContent=fb(Math.max(0,p.bytes-p.up_bytes))+' left';
$('s3').textContent=fb(s.speed)+'/s';$('s3b').textContent='avg '+fb(s.avg)+'/s · '+s.accs.filter(a=>a.status=='ok'&&a.speed>0).length+' acc active';
$('s4').textContent=fb(ul?ul.speed:0)+'/s';$('s4b').textContent=ul?'uploading now':'idle';$('s5').textContent=ft(s.eta);$('s5b').textContent='download';
$('s6').textContent=s.elapsed?ft(s.elapsed):'—';$('s6b').textContent='lifetime: '+s.done_count+' files · '+fb(s.total_bytes);
const pc=p.bytes?Math.min(100,(p.up_bytes+(ul?ul.done:0))/p.bytes*100):0;$('ob').style.width=pc+'%';$('opct').textContent=pc.toFixed(1)+'%';
$('o1').textContent=fb(p.up_bytes)+' / '+fb(p.bytes);$('o2').textContent='downloaded '+p.dl_files+' files · last done msg #'+s.last_done;
if(dl){const q=dl.size?dl.done/dl.size*100:0;$('dn').textContent='#'+dl.id+'  '+dl.name;$('dn').title=dl.name;$('db').style.width=q+'%';$('dt').textContent=fb(dl.done)+' / '+fb(dl.size)+'  @ '+fb(dl.speed)+'/s';
$('dp').textContent=q.toFixed(1)+'%  ·  '+ft(dl.speed>0?(dl.size-dl.done)/dl.speed:0);$('dseg').textContent=dl.nseg?dl.segs_done+' / '+dl.nseg+' segments':'';drawMap(dl.map||'',dl.nseg)}
else{$('dn').textContent=scanning?'scanning channel…':'—';$('db').style.width=0;$('dt').textContent=$('dp').textContent=$('dseg').textContent='';drawMap('',0)}
if(ul){const q=ul.size?ul.done/ul.size*100:0;$('un').textContent='#'+ul.id+'  '+ul.name;$('un').title=ul.name;$('uspd').textContent=fb(ul.speed)+'/s';$('ub').style.width=q+'%';$('ut').textContent=fb(ul.done)+' / '+fb(ul.size)+'  @ '+fb(ul.speed)+'/s';$('up').textContent=q.toFixed(1)+'%  ·  '+ft(ul.speed>0?(ul.size-ul.done)/ul.speed:0)}
else{$('un').textContent='—';$('uspd').textContent='';$('ub').style.width=0;$('ut').textContent=$('up').textContent=''}
$('nx').innerHTML=s.next.length?s.next.map(n=>'<span>#'+n.id+' · '+fb(n.size)+'</span>').join(''):'<span>—</span>';
const mxs=Math.max(2097152,...s.accs.map(a=>a.speed));$('acn').textContent=s.accs.length+' accounts'+(s.cryptg?' · cryptg ✓':' · cryptg ✗');
$('ac').innerHTML=s.accs.map(a=>{const col=PAL[a.idx%PAL.length];let ch=a.status=='flood'?'<span class="chip flood">flood '+a.flood_left+'s</span>':a.status=='dead'?'<span class="chip dead">dead</span>':a.speed>0?'<span class=chip>active</span>':'<span class="chip idle">idle</span>';
return '<div class=ac><div class=h><span class=dot style="background:'+col+'"></span><div style="min-width:0"><div class=n>'+esc(a.name)+'</div><div class=ph>'+esc(a.phone)+'</div></div>'+ch+'</div>'
+'<div class=sp>'+fb(a.speed)+'/s</div><div class=bar><i style="width:'+Math.min(100,a.speed/mxs*100)+'%;background:'+col+'"></i></div>'
+'<div class=row><span>'+fb(a.bytes)+' · '+a.segs+' seg</span><span>'+a.conn+' conn</span></div>'
+'<div class=row><span>flood: '+a.floods+'</span><span>errors: '+a.errors+'</span></div>'+(a.last_err?'<div class=er title="'+esc(a.last_err)+'">'+esc(a.last_err)+'</div>':'')+'</div>'}).join('');
drawChart(s.hist);
const l=$('log'),bt=l.scrollTop+l.clientHeight>=l.scrollHeight-30;l.innerHTML=s.log.map(r=>'<div class="'+r[1]+'"><b>'+r[0]+'</b>'+esc(r[2])+'</div>').join('');if(bt)l.scrollTop=l.scrollHeight}
setInterval(tick,1000);tick();
</script></body></html>"""

LOGIN = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><body style="background:#0a0e16;color:#e8eef8;font-family:system-ui;display:grid;place-items:center;height:100vh;margin:0">
<form method=post style="background:#111826;padding:28px;border-radius:16px;border:1px solid #1c2740;width:290px">
<h3 style="margin:0 0 14px">🔒 TG → Drive</h3><input name=p type=password autofocus placeholder=Password style="width:100%;padding:11px;border-radius:9px;border:1px solid #1c2740;background:#0d1420;color:#fff;outline:none">
<button style="margin-top:12px;width:100%;padding:11px;border:0;border-radius:9px;background:#4f9cff;color:#fff;font-weight:600;cursor:pointer">Enter</button></form>"""

SESS = set()


def authed(req):
    return req.cookies.get("sid") in SESS


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
    if not authed(req):
        raise web.HTTPFound("/login")
    return web.Response(text=HTML, content_type="text/html")


async def h_status(req):
    if not authed(req):
        return web.Response(status=401)
    return web.json_response(status_json())


async def start_job(channel, folder, start_id):
    global JOB
    if JOB and not JOB.done():
        return False
    S.update(channel=channel, folder=folder, start_id=start_id)
    JOB = asyncio.create_task(run_job())
    return True


async def h_start(req):
    if not authed(req):
        return web.Response(status=401)
    d = await req.json()
    ok = await start_job(d["channel"].strip(), parse_folder(d["folder"]), int(d.get("start_id") or 0))
    return web.json_response({"ok": ok})


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
    for aid, cl in CLIENTS.items():
        a = S["accs"][aid]
        try:
            me = await asyncio.wait_for(cl.get_me(), 20)
            log(f"telegram OK: {a['name']} (@{me.username or me.id})", "ok")
        except Exception as e:
            if is_dead(e):
                acc_dead(a, e)
            else:
                log(f"telegram FAIL: {a['name']}: {e}", "err")
    try:
        def chk():
            return drive_service().about().get(fields="user(emailAddress)").execute()["user"]["emailAddress"]
        em = await asyncio.wait_for(asyncio.to_thread(chk), 30)
        log(f"drive OK: {em}", "ok")
    except Exception as e:
        log(f"drive FAIL: {e}  (token.json check karo)", "err")
    log("------------------------")


async def on_start(app):
    global PRIMARY, PRIMARY_AID
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
                              "speed": 0, "segs": 0, "conn": int(a.get("cfg", {}).get("conn", 2)),
                              "status": "ok", "flood_until": 0, "floods": 0, "errors": 0, "last_err": ""}
        if a["id"] == cfg.get("active") or PRIMARY is None:
            PRIMARY, PRIMARY_AID = cl, a["id"]
    if not CLIENTS:
        raise SystemExit("koi bhi account connect nahi hua")
    log(f"{len(CLIENTS)} accounts ready", "ok")
    log(f"UI: http://localhost:{PORT}  (password: {PASSWORD})")
    app["tasks"] = [asyncio.create_task(speed_loop())]
    await startup_checks()
    log("saved state dhoond raha hoon (Saved Messages)...")
    try:
        st = await asyncio.wait_for(load_state(), 60)
    except Exception as e:
        log(f"state load fail/timeout: {e}", "warn")
        st = None
    if st:
        S.update(channel=st.get("channel", ""), folder=st.get("folder", ""),
                 last_done=st.get("last_done", 0), done_count=st.get("done_count", 0),
                 total_bytes=st.get("total_bytes", 0))
        cur_state["cur"] = st.get("cur")
        log(f"saved state mila: last_done={S['last_done']}, done={S['done_count']}", "ok")
        if st.get("running") and S["channel"]:
            log("crash se pehle running tha -> auto resume", "warn")
            await start_job(S["channel"], S["folder"], 0)
    else:
        log("koi purana state nahi mila -> fresh start")
    app["tasks"].append(asyncio.create_task(state_loop()))


def main():
    app = web.Application()
    app.add_routes([web.get("/", h_index), web.route("*", "/login", h_login),
                    web.get("/api/status", h_status), web.post("/api/start", h_start),
                    web.post("/api/stop", h_stop)])
    app.on_startup.append(on_start)
    web.run_app(app, port=PORT, host="0.0.0.0", print=None)


if __name__ == "__main__":
    main()
