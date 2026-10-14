"""
TIANTA IPC SDK — unified cross-language process-interop layer (Python reference).

Reference implementation of the spec in sdk/DESIGN.md. Exposes a function-style
API identical (by name/semantics) across Python/Go/Rust/Java/C++ (the TIANTA
naming convention, prefix "TI_"):

  Basic:        TI_connect, TI_queue_enqueue/dequeue/pending, TI_lock_acquire/release/with
  Task:         TI_task_post/wait/subscribe/unsubscribe (+ 4 TI_KEY_* task key constants)
  Logging:      TI_log_setup, TI_log_info/warn/error, TI_log_report_error

Storage defaults:
  dir : /opt/tianta/sys   (auto-created)
  db  : /opt/tianta/sys/platform.dat  (auto-created)
  log : /opt/tianta/sys/<app>.log  (size-based rolling)

Use only with the stdlib. Run tests: python3 tests/test_tianta.py
"""

import contextlib
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Default storage
# ---------------------------------------------------------------------------
DEFAULT_DIR = "/opt/tianta/sys"
DEFAULT_DB = os.path.join(DEFAULT_DIR, "platform.dat")
ENV_DB = "TIANTA_PLATFORM_DB"          # override db path (production)
ENV_DIR = "TIANTA_PLATFORM_DIR"        # override dir (for tests)


def _default_db():
    return os.environ.get(ENV_DB) or DEFAULT_DB


# ---------------------------------------------------------------------------
# Task message keys (identical names/values in every language port)
# ---------------------------------------------------------------------------
TI_KEY_TM_DONE = "tm_record_done"          # (1) 卫星遥测数据文件记录完成
TI_KEY_CMD_DONE = "cmd_file_done"          # (2) 新的指令文件存储完成
TI_KEY_DL_REQ = "download_request"         # (3) 有下传申请
TI_KEY_SYS_NTF = "system_notification"     # (4) 有系统通知
TI_ALL_TASK_KEYS = (TI_KEY_TM_DONE, TI_KEY_CMD_DONE, TI_KEY_DL_REQ, TI_KEY_SYS_NTF)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    key        TEXT    NOT NULL DEFAULT 'default',
    payload    TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_key_status ON queue(key, status, id);
CREATE TABLE IF NOT EXISTS locks (
    name     TEXT PRIMARY KEY,
    holder   TEXT,
    acquired INTEGER
);
"""


def _ensure_dir(path):
    """Create the directory (and parents) if it does not exist."""
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)
    try:
        os.chmod(d, 0o750)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Logging: rolling log file + error report
# ---------------------------------------------------------------------------
class _Logger:
    def __init__(self):
        self._path = None
        self._max_bytes = 5 * 1024 * 1024
        self._backups = 3
        self._lock = threading.Lock()

    def setup(self, app="platform", dir=None, max_bytes=None, backups=None):
        if dir is None:
            env_dir = os.environ.get(ENV_DIR)
            dir = env_dir or DEFAULT_DIR
        _ensure_dir(os.path.join(dir, "x"))
        if max_bytes is not None:
            self._max_bytes = int(max_bytes)
        if backups is not None:
            self._backups = int(backups)
        self._path = os.path.join(dir, f"{app}.log")
        return self._path

    def _rotate(self):
        if self._max_bytes <= 0 or not os.path.exists(self._path):
            return
        if os.path.getsize(self._path) < self._max_bytes:
            return
        # shift .N -> .N+1
        for i in range(self._backups, 0, -1):
            src = f"{self._path}.{i}"
            dst = f"{self._path}.{i+1}"
            if os.path.exists(src):
                os.replace(src, dst)
        os.replace(self._path, f"{self._path}.1")

    def _write(self, level, msg):
        if not self._path:
            return
        with self._lock:
            self._rotate()
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            line = f"[{ts}] [{level}] {msg}\n"
            with open(self._path, "a") as f:
                f.write(line)

    def info(self, msg): self._write("INFO", msg)
    def warn(self, msg): self._write("WARN", msg)
    def error(self, msg): self._write("ERROR", msg)


_LOGGER = _Logger()

# API -----------------------------------------------------------------------
def TI_log_setup(app="platform", dir=None, max_bytes=None, backups=None):
    return _LOGGER.setup(app, dir, max_bytes, backups)


def TI_log_info(msg): _LOGGER.info(msg)
def TI_log_warn(msg): _LOGGER.warn(msg)
def TI_log_error(msg): _LOGGER.error(msg)
def TI_log_report_error(msg): _LOGGER.error(msg)


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------
class TIHandle:
    """Opaque per-process connection handle. Base API functions take it."""

    def __init__(self, path, conn):
        self.path = path
        self._conn = conn

    @property
    def conn(self):
        return self._conn


def TI_connect(db=None):
    """Open/create the platform database (default /opt/tianta/sys/platform.dat)."""
    path = db or _default_db()
    _ensure_dir(path)
    con = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
    con.isolation_level = None
    con.execute("PRAGMA busy_timeout=30000")

    def _retry(fn, tries=10):
        last = None
        for i in range(tries):
            try:
                return fn()
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or i == tries - 1:
                    raise
                last = e
                time.sleep(0.1)
        raise last

    _retry(lambda: con.execute("PRAGMA journal_mode=WAL"))
    con.execute("PRAGMA synchronous=NORMAL")
    _retry(lambda: con.executescript(_SCHEMA))
    return TIHandle(path, con)


def _immediate(con):
    deadline = time.monotonic() + 30
    while True:
        try:
            con.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


# ---------------------------------------------------------------------------
# Basic API — queue
# ---------------------------------------------------------------------------
def TI_queue_enqueue(ch, key="default", payload=None):
    if payload is None:
        payload = key
        key = "default"
    with _tx(ch):
        cur = ch.conn.execute(
            "INSERT INTO queue (key, payload, created_at) VALUES (?, ?, ?)",
            (key, payload, int(time.time() * 1000)),
        )
        return cur.lastrowid


def TI_queue_dequeue(ch, key=None, timeout_ms=0):
    """Take one pending message. If key is None, any key matches.
    Blocks up to timeout_ms (0 = try once). Returns (id, key, payload) or None."""
    deadline = (time.monotonic() * 1000) + timeout_ms if timeout_ms > 0 else 0
    while True:
        with _tx(ch):
            if key is None:
                row = ch.conn.execute(
                    "SELECT id, key, payload FROM queue "
                    "WHERE status='pending' ORDER BY id LIMIT 1"
                ).fetchone()
            else:
                row = ch.conn.execute(
                    "SELECT id, key, payload FROM queue "
                    "WHERE status='pending' AND key=? ORDER BY id LIMIT 1",
                    (key,),
                ).fetchone()
            if row is not None:
                ch.conn.execute(
                    "UPDATE queue SET status='done' WHERE id=?", (row[0],)
                )
                return row
        if timeout_ms and (time.monotonic() * 1000) < deadline:
            time.sleep(0.05)
        else:
            return None


def TI_queue_pending(ch, key=None):
    with _tx(ch):
        if key is None:
            row = ch.conn.execute(
                "SELECT COUNT(*) FROM queue WHERE status='pending'"
            ).fetchone()
        else:
            row = ch.conn.execute(
                "SELECT COUNT(*) FROM queue WHERE status='pending' AND key=?",
                (key,),
            ).fetchone()
        return row[0]


@contextlib.contextmanager
def _tx(ch):
    _immediate(ch.conn)
    try:
        yield
    finally:
        ch.conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Basic API — lock
# ---------------------------------------------------------------------------
def _ensure_lock_row(ch, name):
    ch.conn.execute(
        "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?, NULL, NULL)",
        (name,),
    )


def TI_lock_acquire(ch, name, timeout_ms=0):
    """Named mutex. Returns True if acquired. timeout_ms=0 -> try once."""
    _ensure_lock_row(ch, name)
    holder = f"pid-{os.getpid()}"
    current = int(time.time() * 1000)
    cutoff = current - 60_000  # stale-after 60s
    deadline = (time.monotonic() * 1000) + timeout_ms if timeout_ms > 0 else 0
    while True:
        with _tx(ch):
            row = ch.conn.execute(
                "SELECT acquired FROM locks WHERE name=? AND acquired IS NOT NULL",
                (name,),
            ).fetchone()
            if row is None:
                ch.conn.execute(
                    "UPDATE locks SET holder=?, acquired=? WHERE name=?",
                    (holder, current, name),
                )
                return True
            if row[0] and row[0] <= cutoff:
                n = ch.conn.execute(
                    "UPDATE locks SET holder=?, acquired=? WHERE name=? AND acquired=?",
                    (holder, current, name, row[0]),
                ).rowcount
                if n > 0:
                    return True
        if timeout_ms and (time.monotonic() * 1000) < deadline:
            time.sleep(0.01)
        else:
            return False


def TI_lock_release(ch, name):
    ch.conn.execute(
        "UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?", (name,)
    )


def TI_lock_with(ch, name, timeout_ms, fn):
    if not TI_lock_acquire(ch, name, timeout_ms):
        raise TimeoutError(f"could not acquire lock '{name}'")
    try:
        return fn()
    finally:
        TI_lock_release(ch, name)


# ---------------------------------------------------------------------------
# Task API — post / wait / subscribe / unsubscribe
# ---------------------------------------------------------------------------
def TI_task_post(ch, key, payload):
    """Post one task message on the given key. Returns its id."""
    return TI_queue_enqueue(ch, key, payload)


def TI_task_wait(ch, key, timeout_ms=0):
    """Block until one message for `key` arrives (or timeout). Returns payload or None."""
    item = TI_queue_dequeue(ch, key=key, timeout_ms=timeout_ms)
    return item[2] if item else None


_SUBS = {}          # key -> {tag: (ch, callback, stop_event, thread)}
_SUBS_LOCK = threading.Lock()


def TI_task_subscribe(ch, key, callback, tag=None):
    """Subscribe to `key`; a background thread calls callback(payload) per message.
    Returns a unique tag usable with TI_task_unsubscribe."""
    tag = tag or str(uuid.uuid4())
    stop = threading.Event()
    thread = threading.Thread(
        target=_sub_loop, args=(ch.path, key, callback, stop), daemon=True
    )
    with _SUBS_LOCK:
        subs = _SUBS.setdefault(key, {})
        subs[tag] = (ch, callback, stop, thread)
    thread.start()
    return tag


def _sub_loop(path, key, callback, stop):
    # Each subscriber owns its OWN connection (avoid cross-thread use of Handle's)
    ch = TI_connect(path)
    while not stop.is_set():
        item = TI_queue_dequeue(ch, key=key, timeout_ms=200)
        if item is None:
            continue
        try:
            callback(item[2])
        except Exception as e:  # noqa: BLE001
            _LOGGER.error(f"subscriber callback error key={key}: {e!r}")


def TI_task_unsubscribe(ch, key, tag):
    with _SUBS_LOCK:
        subs = _SUBS.get(key, {})
        entry = subs.pop(tag, None)
    if entry:
        _, _, stop, thread = entry
        stop.set()
        thread.join(timeout=1.0)  # ensure the loop fully exits before returning


# ---------------------------------------------------------------------------
# Convenience: one-shot functional demo helpers (optional)
# ---------------------------------------------------------------------------
def TI_task_post_file_done_tm(ch, file_path, meta=None):
    import json
    payload = json.dumps({"file": file_path, "meta": meta or {}})
    return TI_task_post(ch, TI_KEY_TM_DONE, payload)
