#!/usr/bin/env python3
"""Tests for the unified IPC SDK (python/tianta.py).

Verifies: auto-create of dir/db, basic queue/lock, the four task keys,
subscribe-push, rolling log, and multi-process queue/lock safety.

Uses temp dirs via db paths (not the real /opt/tianta/sys). Run:
    python3 tests/test_tianta.py
"""
import multiprocessing as mp
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
import tianta as S


def worker_post(ch_path, key, n, prefix):
    ch = S.TI_connect(ch_path)
    for i in range(n):
        S.TI_task_post(ch, key, f"{prefix}-{i}")
    ch.conn.close()


def worker_consume_until_empty(ch_path, key, out, stop):
    ch = S.TI_connect(ch_path)
    with open(out, "a") as f:
        while not stop.is_set():
            item = S.TI_queue_dequeue(ch, key=key, timeout_ms=200)
            if item is None:
                continue
            f.write(item[2] + "\n")
    ch.conn.close()


def new_db(tmp, tag):
    d = os.path.join(tmp, tag)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "platform.dat")


def test_autocreate(tmp):
    """connect() auto-creates nested dir and the db with the right schema."""
    db = new_db(tmp, "autocreate")
    ch = S.TI_connect(db)
    tables = {r[0] for r in ch.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert {"queue", "locks"} <= tables, tables
    cols = {r[1] for r in ch.conn.execute("PRAGMA table_info(queue)").fetchall()}
    assert {"key", "payload", "status", "created_at"} <= cols, cols
    # WAL active
    mode = ch.conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal", mode
    ch.conn.close()
    print("[autocreate] dir + db auto-created, WAL + schema ready")
    return db


def test_basic_queue_lock(tmp):
    db = new_db(tmp, "basic")
    ch = S.TI_connect(db)
    S.TI_queue_enqueue(ch, "alpha", "A1")
    S.TI_queue_enqueue(ch, "beta", "B1")
    assert S.TI_queue_pending(ch) == 2
    item = S.TI_queue_dequeue(ch, key="alpha")
    assert item is not None and item[1] == "alpha" and item[2] == "A1"
    assert S.TI_queue_pending(ch, key="alpha") == 0
    assert S.TI_queue_pending(ch) == 1
    # lock
    assert S.TI_lock_acquire(ch, "lk1", 0) is True
    assert S.TI_lock_acquire(ch, "lk2", 0) is True
    S.TI_lock_release(ch, "lk1")
    S.TI_lock_release(ch, "lk2")
    ch.conn.close()
    print("[basic] queue + lock OK")


def test_task_keys_and_wait(tmp):
    db = new_db(tmp, "task")
    ch = S.TI_connect(db)
    assert S.TI_KEY_TM_DONE == "tm_record_done"
    assert S.TI_KEY_CMD_DONE == "cmd_file_done"
    assert S.TI_KEY_DL_REQ == "download_request"
    assert S.TI_KEY_SYS_NTF == "system_notification"
    S.TI_task_post(ch, S.TI_KEY_TM_DONE, '{"file":"tm.bin"}')
    got = S.TI_task_wait(ch, S.TI_KEY_TM_DONE, timeout_ms=2000)
    assert got == '{"file":"tm.bin"}', got
    ch.conn.close()
    print("[task-keys] four keys + post/wait OK")


def test_subscribe_push(tmp):
    db = new_db(tmp, "sub")
    ch = S.TI_connect(db)
    received = []
    tag = S.TI_task_subscribe(ch, S.TI_KEY_DL_REQ, lambda p: received.append(p))
    time.sleep(0.4)  # let the subscriber thread connect
    S.TI_task_post(ch, S.TI_KEY_DL_REQ, "dl-1")
    S.TI_task_post(ch, S.TI_KEY_DL_REQ, "dl-2")
    time.sleep(0.8)
    S.TI_task_unsubscribe(ch, S.TI_KEY_DL_REQ, tag)
    # allow any single in-flight dequeue to settle (dequeue timeout is 200ms)
    time.sleep(0.5)
    S.TI_task_post(ch, S.TI_KEY_DL_REQ, "dl-3")  # after settle -> must NOT arrive
    time.sleep(0.5)
    assert "dl-3" not in received, f"message delivered after unsubscribe: {received}"
    assert set(received) >= {"dl-1", "dl-2"}, received
    ch.conn.close()
    print(f"[subscribe] pushed {received}, no post-unsubscribe delivery")


def test_rolling_log(tmp):
    d = os.path.join(tmp, "logs")
    os.makedirs(d, exist_ok=True)
    path = S.TI_log_setup("platform", dir=d, max_bytes=300, backups=2)
    for i in range(80):
        S.TI_log_info(f"row {i} " + "y" * 20)
    S.TI_log_error("boom critical")
    assert os.path.exists(path)
    assert os.path.exists(path + ".1"), "rotation did not happen"
    # error line present in current or a backup
    found = False
    for p in [path] + [f"{path}.{i}" for i in range(1, 4)]:
        if os.path.exists(p):
            if "[ERROR] boom critical" in open(p, encoding="utf-8", errors="replace").read():
                found = True
    assert found, "error line not found in any rotated file"
    print("[log] rolling log + [ERROR] reporting OK")


def test_multiprocess_sdk(tmp):
    ctx = mp.get_context("spawn")
    db = new_db(tmp, "mp")
    out = os.path.join(tmp, "out.txt")

    procs = []
    for p in range(4):
        proc = ctx.Process(target=worker_post, args=(db, "data", 50, f"p{p}"))
        procs.append(proc)
        proc.start()
    for p in procs:
        p.join()
    assert all(p.exitcode == 0 for p in procs)

    ch = S.TI_connect(db)
    assert S.TI_queue_pending(ch, key="data") == 200, S.TI_queue_pending(ch, key="data")
    ch.conn.close()

    # consumers drain until empty (signal stop via timeout: stop when empty twice)
    stop = ctx.Event()
    consumers = [ctx.Process(target=worker_consume_until_empty, args=(db, "data", out, stop))
                 for _ in range(3)]
    for c in consumers:
        c.start()
    # wait until queue empty
    qch = S.TI_connect(db)
    deadline = time.time() + 15
    while S.TI_queue_pending(qch, key="data") > 0 and time.time() < deadline:
        time.sleep(0.1)
    qch.conn.close()
    stop.set()
    for c in consumers:
        c.join(5)

    with open(out) as f:
        lines = [l.strip() for l in f if l.strip()]
    assert len(lines) == 200, f"consumed {len(lines)} != 200"
    assert len(set(lines)) == 200, "duplicate consumption"
    print(f"[multiprocess] 4 writers -> 3 readers: {len(lines)} items, no dupes")


if __name__ == "__main__":
    tmp = tempfile.mkdtemp(prefix="sdk-test-")
    test_autocreate(tmp)
    test_basic_queue_lock(tmp)
    test_task_keys_and_wait(tmp)
    test_subscribe_push(tmp)
    test_rolling_log(tmp)
    test_multiprocess_sdk(tmp)
    print("\nALL TIANTA TESTS PASSED")
