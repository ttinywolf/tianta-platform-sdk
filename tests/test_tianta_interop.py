#!/usr/bin/env python3
"""Cross-language TIANTA interop test.

Proves that the unified TIANTA layer lets different languages interoperate on
ONE SQLite file (unified schema with the `key` column):
  - C++ TIANTA producer writes keyed messages; Python TIANTA consumer reads them.
  - Python lock + C++ lock share the same named lock over the same file.

Requires the C++ TIANTA test binary built first:
  clang++ -std=c++17 -pthread cpp/tianta_test.cpp -lsqlite3 -o /tmp/tianta_cpp
Run:  TIANTA_CPP=/tmp/tianta_cpp python3 tests/test_tianta_interop.py
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
import tianta as T

CPP = os.environ.get("TIANTA_CPP", "/tmp/tianta_cpp")


def cpp(*args):
    subprocess.run([CPP, *args], check=True,
                   env=dict(os.environ, TIANTA_CPP=CPP),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def test_cpp_produce_python_consume():
    d = tempfile.mkdtemp(prefix="interop-")
    db = os.path.join(d, "platform.dat")
    out = os.path.join(d, "out.txt")

    # C++ producer writes 100 keyed messages
    cpp("produce", db, "100")
    ch = T.TI_connect(db)
    assert T.TI_queue_pending(ch, key="data") == 100, T.TI_queue_pending(ch, key="data")

    # Python consumer drains them
    stop = time.time() + 10
    with open(out, "w") as f:
        while time.time() < stop:
            item = T.TI_queue_dequeue(ch, key="data", timeout_ms=200)
            if item is None:
                if T.TI_queue_pending(ch, key="data") == 0:
                    break
                continue
            f.write(item[2] + "\n")
    ch.conn.close()

    with open(out) as f:
        items = [ln.strip() for ln in f if ln.strip()]
    assert len(items) == 100, f"consumed {len(items)} != 100"
    assert all(i.startswith("produce-") for i in items)
    assert len(set(items)) == 100, "duplicates across languages"
    print(f"[interop] C++ TIANTA producer -> Python TIANTA consumer: 100 items OK")


def test_shared_lock_two_languages():
    d = tempfile.mkdtemp(prefix="interop-lock-")
    db = os.path.join(d, "platform.dat")
    cf = os.path.join(d, "count.txt")
    with open(cf, "w") as f:
        f.write("0")

    n_cpp, n_py = 300, 300

    def py_lock():
        ch = T.TI_connect(db)
        for _ in range(n_py):
            def critical():
                with open(cf) as f:
                    v = int(f.read().strip() or "0")
                with open(cf, "w") as f:
                    f.write(str(v + 1))
            T.TI_lock_with(ch, "cross_lock", 10000, critical)
        ch.conn.close()

    t = threading.Thread(target=py_lock)
    t.start()
    cpp("lock", db, "cross_lock", cf, str(n_cpp))
    t.join()

    with open(cf) as f:
        final = int(f.read().strip() or "0")
    expected = n_cpp + n_py
    assert final == expected, f"counter={final} want {expected} (lost updates)"
    print(f"[interop] Python+C++ share named lock: counter={final} == {expected} OK")


if __name__ == "__main__":
    test_cpp_produce_python_consume()
    test_shared_lock_two_languages()
    print("\nALL CROSS-LANGUAGE TIANTA INTEROP TESTS PASSED")
