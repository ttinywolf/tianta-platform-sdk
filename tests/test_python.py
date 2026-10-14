"""Cross-process tests for the Python SQLite queue + lock.

Run:  python3 tests/test_python.py
"""
import multiprocessing as mp
import os
import tempfile
import time

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
from ipc_queue import Queue, Lock


def worker_enqueue(path, n):
    q = Queue(path)
    for i in range(n):
        q.enqueue(f"msg-{os.getpid()}-{i}")
    q.close()


def worker_dequeue(path, out_file, stop_event):
    q = Queue(path)
    with open(out_file, "a") as f:
        while not stop_event.is_set():
            item = q.dequeue(timeout_ms=200)
            if item is None:
                continue
            f.write(item[1] + "\n")
            f.flush()
    q.close()


def worker_lock_counter(path, name, increments):
    # Increment a shared counter, protected by the cross-process lock.
    # If the lock fails, concurrent read-modify-write loses updates.
    lk = Lock(path, name)
    for _ in range(increments):
        with lk(timeout_ms=5000):
            row = lk._con.execute(
                "SELECT value FROM counter WHERE id=1"
            ).fetchone()
            val = row[0] if row else 0
            if row is None:
                lk._con.execute(
                    "INSERT INTO counter (id, value) VALUES (1, 1)"
                )
            else:
                lk._con.execute(
                    "UPDATE counter SET value=? WHERE id=1", (val + 1,)
                )
    lk.close()


def _init_counter(path):
    import sqlite3
    con = sqlite3.connect(path, timeout=10)
    con.execute("CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER)")
    con.execute("INSERT OR REPLACE INTO counter (id, value) VALUES (1, 0)")
    con.commit()
    con.close()


def test_queue_multiprocess():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)

    n_producers, n_per = 4, 50
    ctx = mp.get_context("spawn")
    procs = []
    for _ in range(n_producers):
        p = ctx.Process(target=worker_enqueue, args=(path, n_per))
        procs.append(p)
        p.start()
    for p in procs:
        p.join()
    assert all(p.exitcode == 0 for p in procs)

    q = Queue(path)
    assert q.pending_count() == n_producers * n_per, q.pending_count()
    q.close()

    stop_event = ctx.Event()
    out_file = path + ".out"
    total = n_producers * n_per
    n_consumers = 3
    procs = []
    for _ in range(n_consumers):
        p = ctx.Process(target=worker_dequeue, args=(path, out_file, stop_event))
        procs.append(p)
        p.start()

    # wait until everything is consumed, then signal stop
    deadline = 20
    start = time.time()
    while True:
        if os.path.exists(out_file) and sum(1 for _ in open(out_file)) >= total:
            break
        if (time.time() - start) > deadline:
            break
        time.sleep(0.1)
    stop_event.set()
    for p in procs:
        p.join(timeout=5)

    items = []
    if os.path.exists(out_file):
        with open(out_file) as f:
            items = [ln.strip() for ln in f if ln.strip()]
    assert len(items) == total, f"consumed {len(items)} != {total}"
    assert len(set(items)) == total, "duplicate consumption detected!"
    print(f"queue ok: {total} items consumed, no duplicates")

    for f in (path, path + "-wal", path + "-shm", out_file, path + ".count"):
        if os.path.exists(f):
            os.unlink(f)


def test_lock_multiprocess():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    _init_counter(path)

    n_procs, increments = 4, 200
    procs = []
    for _ in range(n_procs):
        p = mp.Process(
            target=worker_lock_counter,
            args=(path, "test_lock", increments),
        )
        procs.append(p)
        p.start()
    for p in procs:
        p.join()
    assert all(p.exitcode == 0 for p in procs)

    lk = Lock(path, "test_lock")
    val = lk._con.execute("SELECT value FROM counter WHERE id=1").fetchone()[0]
    lk.close()
    expected = n_procs * increments
    assert val == expected, f"counter lost updates: {val} != {expected}"
    print(f"lock ok: counter={val} == expected {expected} (no lost updates)")

    if os.path.exists(path):
        os.unlink(path)


if __name__ == "__main__":
    test_queue_multiprocess()
    test_lock_multiprocess()
    print("\nALL PYTHON TESTS PASSED")
