"""
A cross-process message queue and mutex lock backed by SQLite.

Designed for: low-concurrency, multi-process, single-machine IPC on a shared
SQLite file. Uses WAL mode + busy_timeout for write concurrency, and
`BEGIN IMMEDIATE` transactions for atomic dequeue (no lost / double consumption).

The schema is shared across all language ports (python/, go/, rust/, java/, cpp/)
so that processes written in different languages can enqueue/dequeue/lock the
*same* database file.

Requires Python's stdlib only (sqlite3). Usage:

    q = Queue("my.db")
    q.enqueue("hello")
    while True:
        item = q.dequeue()          # (id, payload) or None
        if item:
            q.ack(item[0])          # optional confirm; dequeue already marks done
            break

    with Lock("my.db", "my_lock"):
        # exclusive critical section
        ...
"""

import contextlib
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    payload    TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, id);
CREATE TABLE IF NOT EXISTS locks (
    name      TEXT PRIMARY KEY,
    holder    TEXT,
    acquired  INTEGER
);
"""


def _connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
    con.isolation_level = None  # autocommit; transactions managed manually via BEGIN IMMEDIATE
    con.execute("PRAGMA busy_timeout=30000")
    # journal_mode=WAL needs a moment of exclusive access on first setup; retry it
    _retry(lambda: con.execute("PRAGMA journal_mode=WAL"))
    con.execute("PRAGMA synchronous=NORMAL")
    _retry(lambda: con.executescript(SCHEMA))
    return con


def _retry(fn, tries: int = 10):
    for i in range(tries):
        try:
            fn()
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or i == tries - 1:
                raise
            time.sleep(0.1)


class Queue:
    """A minimal FIFO-backed message queue.

    Multiple processes each create their own connection; dequeue uses an
    IMMEDIATE transaction so exactly one process claims each item.
    """

    def __init__(self, path: str):
        self.path = path
        self._con = _connect(path)

    def enqueue(self, payload: str) -> int:
        with self._immediate():
            cur = self._con.execute(
                "INSERT INTO queue (payload, created_at) VALUES (?, ?)",
                (payload, int(time.time() * 1000)),
            )
            return cur.lastrowid

    def dequeue(self, timeout_ms: int = 0):
        """Claim and return the oldest pending item as (id, payload).

        The item is atomically marked 'done' under an IMMEDIATE transaction,
        so it is never handed to two consumers. Returns None if empty.
        Blocks up to timeout_ms (0 = return immediately) waiting for an item.
        """
        deadline = (time.monotonic() * 1000) + timeout_ms if timeout_ms > 0 else 0
        while True:
            with self._immediate():
                row = self._con.execute(
                    "SELECT id, payload FROM queue "
                    "WHERE status='pending' ORDER BY id LIMIT 1"
                ).fetchone()
                if row is not None:
                    self._con.execute(
                        "UPDATE queue SET status='done' WHERE id=?", (row[0],)
                    )
                    return row
            if timeout_ms and (time.monotonic() * 1000) < deadline:
                time.sleep(0.05)
            else:
                return None

    def ack(self, item_id: int) -> None:
        # dequeue already marks items done; kept for API symmetry
        with self._immediate():
            self._con.execute(
                "UPDATE queue SET status='done' WHERE id=?", (item_id,)
            )

    def pending_count(self) -> int:
        with self._immediate():
            row = self._con.execute(
                "SELECT COUNT(*) FROM queue WHERE status='pending'"
            ).fetchone()
            return row[0]

    @contextlib.contextmanager
    def _immediate(self):
        # Retry loop to absorb SQLITE_BUSY that sneaks past busy_timeout
        deadline = time.monotonic() + 10
        while True:
            try:
                self._con.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        try:
            yield
        finally:
            self._con.execute("COMMIT")

    def close(self):
        self._con.close()


class Lock:
    """A named mutex for a critical section, shared across processes.

    Lock ownership is granted atomically by an IMMEDIATE transaction on the
    `locks` table (each name is a single row). A stale lock released by a
    crashed process ages out after `stale_after_ms`.
    """

    def __init__(self, path: str, name: str, stale_after_ms: int = 60_000):
        self.path = path
        self.name = name
        self.stale_after_ms = stale_after_ms
        con = _connect(path)
        # ensure the lock row exists
        with self._immediate(con):
            con.execute(
                "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?, NULL, NULL)",
                (name,),
            )
        self._con = con

    def acquire(self, timeout_ms: int = 0) -> bool:
        deadline = (time.monotonic() * 1000) + timeout_ms if timeout_ms > 0 else 0
        holder = f"pid-{__import__('os').getpid()}"
        while True:
            acquired = self._try_acquire(holder)
            if acquired:
                return True
            if timeout_ms and (time.monotonic() * 1000) < deadline:
                time.sleep(0.05)
            else:
                return False

    def _try_acquire(self, holder: str) -> bool:
        now = int(time.time() * 1000)
        cutoff = now - self.stale_after_ms
        with self._immediate(self._con):
            row = self._con.execute(
                "SELECT holder, acquired FROM locks WHERE name=? AND acquired IS NOT NULL",
                (self.name,),
            ).fetchone()
            if row is None:
                # not currently held -> take it
                self._con.execute(
                    "UPDATE locks SET holder=?, acquired=? WHERE name=?",
                    (holder, now, self.name),
                )
                return True
            held_by, acquired = row
            if acquired and acquired <= cutoff:
                # stale lock left by a crashed process -> steal it
                self._con.execute(
                    "UPDATE locks SET holder=?, acquired=? WHERE name=? AND acquired=?",
                    (holder, now, self.name, acquired),
                )
                return self._con.total_changes > 0
            return False

    def release(self) -> None:
        with self._immediate(self._con):
            self._con.execute(
                "UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?",
                (self.name,),
            )

    @contextlib.contextmanager
    def __call__(self, timeout_ms: int = 0):
        ok = self.acquire(timeout_ms)
        if not ok:
            raise TimeoutError(f"could not acquire lock '{self.name}'")
        try:
            yield
        finally:
            self.release()

    @contextlib.contextmanager
    def _immediate(self, con):
        deadline = time.monotonic() + 10
        while True:
            try:
                con.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        try:
            yield
        finally:
            con.execute("COMMIT")

    def close(self):
        self._con.close()
