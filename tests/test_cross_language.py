#!/usr/bin/env python3
"""Cross-language interoperability test.

Proves that processes in different languages can share ONE SQLite file:
  - A C++ producer writes N messages.
  - A Python consumer reads them all (no duplicates/loss).
  - Python and C++ take turns incrementing a counter under a shared lock.

Requires the C++ binary to be built first:
  clang++ -std=c++17 -pthread sqliteipc.cpp -lsqlite3 -o /tmp/sqliteipc_test
Then run:  python3 tests/test_cross_language.py
"""
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
from ipc_queue import Queue, Lock

CPP_BIN = os.environ.get("SQLITEIPC_BIN", "/tmp/sqliteipc_test")


def cpp(*args):
    env = dict(os.environ, SQLITEIPC_BIN=CPP_BIN)
    subprocess.run([CPP_BIN, *args], check=True, env=env, stdout=subprocess.DEVNULL)


def test_cpp_writes_python_reads():
    d = tempfile.mkdtemp()
    db = os.path.join(d, "shared.db")
    out = os.path.join(d, "out.txt")

    # C++ produces
    cpp("produce", db, "100")
    # verify pending count from Python
    q = Queue(db)
    assert q.pending_count() == 100, q.pending_count()
    q.close()

    # Python consumes all into out.txt
    q = Queue(db)
    stop = time.time() + 10
    with open(out, "w") as f:
        while time.time() < stop:
            item = q.dequeue(timeout_ms=200)
            if item is None:
                if q.pending_count() == 0:
                    break
                continue
            f.write(item[1] + "\n")
    q.close()

    with open(out) as f:
        items = [ln.strip() for ln in f if ln.strip()]
    assert len(items) == 100, f"python consumed {len(items)} != 100"
    assert all(i.startswith("produce-") for i in items), items[:3]
    assert len(set(items)) == 100, "duplicates across languages"
    print(f"[cross] C++ producer -> Python consumer: 100 items OK")
    cleanup(d)


def test_shared_lock_two_languages():
    d = tempfile.mkdtemp()
    db = os.path.join(d, "lock.db")
    countfile = os.path.join(d, "count.txt")
    with open(countfile, "w") as f:
        f.write("0")

    n_cpp, n_py = 300, 300

    def py_lock_worker():
        lk = Lock(db, "cross_lock")
        for _ in range(n_py):
            with lk(timeout_ms=10000):
                with open(countfile) as f:
                    v = int(f.read().strip() or "0")
                with open(countfile, "w") as f:
                    f.write(str(v + 1))
        lk.close()

    import threading
    t = threading.Thread(target=py_lock_worker)
    t.start()
    # C++ increments in a subprocess under the same named lock
    p = subprocess.Popen(
        [CPP_BIN, "locknamed", db, "cross_lock", countfile, str(n_cpp)],
        env=dict(os.environ, SQLITEIPC_BIN=CPP_BIN),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    t.join()
    rc = p.wait()
    assert rc == 0, f"cpp lock2 exit {rc}"

    with open(countfile) as f:
        final = int(f.read().strip() or "0")
    expected = n_cpp + n_py
    assert final == expected, f"counter={final} want={expected} (cross-language lost updates)"
    print(f"[cross] shared lock Python+C++: counter={final} == {expected} OK")
    cleanup(d)


def cleanup(d):
    for f in os.listdir(d):
        try:
            os.unlink(os.path.join(d, f))
        except OSError:
            pass
    os.rmdir(d)


if __name__ == "__main__":
    test_cpp_writes_python_reads()
    test_shared_lock_two_languages()
    print("\nALL CROSS-LANGUAGE TESTS PASSED")
