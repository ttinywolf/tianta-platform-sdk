#!/usr/bin/env python3
"""Multi-process concurrent-READ verification.

Proves that many processes can read the SAME SQLite file simultaneously and
concurrently with writers, without SQLITE_BUSY errors and with consistent data.

Scenarios:
  1. N readers hammer pending_count() + raw SELECT on the queue table while
     writers are also inserting -> all reads succeed, no "database is locked".
  2. N readers read the same snapshot of payloads -> identical results.

Run:  python3 tests/test_multi_reader.py
"""
import multiprocessing as mp
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
from ipc_queue import _connect, Queue

READERS = 5
WRITERS = 2


def writer(path, n):
    q = Queue(path)
    for i in range(n):
        q.enqueue(f"w-{os.getpid()}-{i}")
    q.close()


def reader(path, results):
    """Hammer reads while other processes write. Collect any errors."""
    con = _connect(path)
    errors = []
    reads = 0
    for _ in range(300):
        try:
            # raw read: pending count
            c = con.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
            # raw read: a few rows
            rows = con.execute(
                "SELECT id, payload FROM queue ORDER BY id LIMIT 20"
            ).fetchall()
            if (c or 1) >= 0 and rows is not None:
                reads += 1
        except Exception as e:  # noqa: BLE001
            errors.append(str(e))
        time.sleep(0.005)
    con.close()
    results.put((reads, errors))


def raw_snapshot_readers(path, results):
    """All readers read one identical snapshot of payloads -> must match."""
    con = _connect(path)
    snapshot = [r[0] for r in con.execute(
        "SELECT payload FROM queue ORDER BY id").fetchall()]
    con.close()
    results.put(snapshot)


def test_concurrent_reads_with_writes():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)

    ctx = mp.get_context("spawn")

    # start writers that keep inserting while readers hammer
    writers = []
    for _ in range(WRITERS):
        p = ctx.Process(target=writer, args=(path, 200))
        writers.append(p)
        p.start()

    results = ctx.Queue()
    readers = []
    for _ in range(READERS):
        p = ctx.Process(target=reader, args=(path, results))
        readers.append(p)
        p.start()

    for p in writers:
        p.join()
    for p in readers:
        p.join()

    assert all(p.exitcode == 0 for p in writers), "writer failed"
    assert all(p.exitcode == 0 for p in readers), "reader crashed"

    entries = []
    while not results.empty():
        entries.append(results.get())
    all_ok = True
    total_reads = 0
    for reads, errors in entries:
        total_reads += reads
        if errors:
            all_ok = False
            print("reader saw errors:", errors[:3])
    assert all_ok, "some reader hit SQLITE_BUSY/errors"
    assert total_reads > 0, "no reads happened"
    print(f"[concurrent-read] {READERS} readers + {WRITERS} writers: "
          f"{total_reads} reads all succeeded, zero errors")

    os.unlink(path) if os.path.exists(path) else None


def test_identical_snapshots():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)

    q = Queue(path)
    for i in range(50):
        q.enqueue(f"payload-{i}")
    q.close()

    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    readers = []
    for _ in range(READERS):
        p = ctx.Process(target=raw_snapshot_readers, args=(path, results))
        readers.append(p)
        p.start()
    for p in readers:
        p.join()

    assert all(p.exitcode == 0 for p in readers), "snapshot reader crashed"
    snapshots = []
    while not results.empty():
        snapshots.append(results.get())
    assert len(snapshots) == READERS, f"got {len(snapshots)} readers"
    for s in snapshots[1:]:
        assert s == snapshots[0], "readers saw inconsistent snapshots"
    assert len(snapshots[0]) == 50
    print(f"[snapshot] {READERS} readers all saw identical 50-row snapshot")

    os.unlink(path) if os.path.exists(path) else None


if __name__ == "__main__":
    test_concurrent_reads_with_writes()
    test_identical_snapshots()
    print("\nALL MULTI-READER TESTS PASSED")
