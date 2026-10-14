# TIANTA Platform SDK

> **TIANTA 公司 · 星载平台 SDK** · TIANTA On-Board Platform SDK
>
> 面向卫星在轨(星载)平台的统一进程间通信(IPC)SDK。基于 SQLite 提供
> **跨语言一致**的「消息队列 + 互斥锁 + 滚动日志」能力,面向低并发、单机、
> 多进程、多语言共享同一数据文件的星载软件场景。

TIANTA Platform SDK 是一个轻量级的跨进程「消息队列 + 互斥锁」库,后端为单一
SQLite 文件。适用约束(与星载平台需求对齐):

- **低并发**(少量进程)
- **单机**(进程间 IPC,非分布式)
- **零外部服务**(无 Redis / 无 broker——仅 SQLite)
- **多语言**(不同语言进程共享*同一个*数据库文件,可跨语言互操作)

它刻意**不**选择共享内存或消息中间件:对"低并发、既要队列又要锁、跨语言"
的场景,SQLite 是最简单且绑定最全的方案。

## 两种使用层次

| 层次 | 说明 | 语言 |
|------|------|------|
| **低层 Queue / Lock** | 极简队列 + 命名互斥锁 | Python / Go / Rust / Java / C++ |
| **统一 TIANTA 层 (`TI_*`)** | 函数式 API:队列、锁、任务、四类星载业务对象、开放扩展字段、滚动日志 | 全部五种语言,API 名称/语义一致 |

### 四类星载业务任务(内置)

| 业务 | key | 消息对象(带结构化字段 + 扩展字段) |
|------|-----|----------------------------------|
| (1) 卫星遥测数据文件记录完成 | `tm_record_done` | `TmRecordDone` |
| (2) 新的指令文件存储完成 | `cmd_file_done` | `CmdFileDone` |
| (3) 有下传申请 | `download_request` | `DownloadReq` |
| (4) 有系统通知 | `system_notification` | `SystemNotice` |

## 存储约定(所有语言一致)

| 项 | 值 |
|----|----|
| 数据目录 | `/opt/tianta/sys`(不存在则自动创建) |
| 主库文件 | `/opt/tianta/sys/platform.dat`(不存在则自动创建,含建表 + WAL) |
| 日志文件 | `/opt/tianta/sys/<app>.log`(大小滚动,保留 N 份) |

## 快速开始(Python)

```python
import tianta as T
from tianta_tasks import TmRecordDone, DownloadReq

ch = T.TI_connect()                      # 默认 /opt/tianta/sys/platform.dat

# 基础队列 + 锁
T.TI_queue_enqueue(ch, "key", "payload")
item = T.TI_queue_dequeue(ch, "key")
T.TI_lock_with(ch, "my_lock", 5000, lambda: ...)

# 四类星载业务任务 + 开放扩展字段
TmRecordDone.post(ch, file_path="/data/tm.bin", size=4096, mission_id="MISS-2026")
ev = TmRecordDone.wait(ch, timeout_ms=5000)

def on_dl(e): print("下传申请", e.req_id, e.data_type)
DownloadReq.on(ch, on_dl)
```

> 其余语言(Go / Rust / Java / C++)提供**同名同语义**的 `TI_*` API 与任务对象,
> 见下文「语言端口」。

## License

[Apache License 2.0](./LICENSE) · © **TIANTA 公司 (TIANTA Corp.)** · 星载平台 SDK
See [NOTICE](./NOTICE) for attribution.

---


One schema, identical across every language port, so any combination of
languages can interoperate:

```sql
CREATE TABLE IF NOT EXISTS queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    key        TEXT    NOT NULL DEFAULT 'default',  -- message/task category
    payload    TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',  -- pending | done
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_key_status ON queue(key, status, id);
CREATE TABLE IF NOT EXISTS locks (
    name     TEXT PRIMARY KEY,
    holder   TEXT,
    acquired INTEGER
);
```

Core techniques (all standard SQLite, no extensions):

| Concern | Mechanism |
|---------|-----------|
| Write concurrency across processes | `PRAGMA journal_mode=WAL` + `PRAGMA busy_timeout` |
| Atomic dequeue (no lost / double consumption) | `BEGIN IMMEDIATE` transaction: SELECT oldest `pending` row, mark `done`, COMMIT — SQLite's single-writer lock serializes the claim |
| Mutex between processes | Named-only `locks` row claimed under `BEGIN IMMEDIATE`; the holder column records who + a timestamp |
| Crash safety / stale locks | `acquired` timestamp; locks older than 60 s are treated as stale and stolen |
| Cross-language sharing | Identical schema + column names in all 5 ports |

### Why not shared memory / Zenoh?

Zenoh does have shared memory, but it optimizes **zero-copy high-throughput
publish-subscribe** for C/Rust. It has no built-in queue/lock semantics, poorer
polyglot bindings, no persistence, and pulls in a full runtime — the opposite
of *lightweight + simple + low concurrency*. Turso/Limbo (async SQLite
rewrites) target **high concurrency / distributed** use cases, not this one. If
you later need real async, concurrent writers, or networking, revisit those —
not now.

## Language ports

| Language | File(s) | Note |
|----------|---------|------|
| Python | `python/ipc_queue.py` | stdlib `sqlite3`, zero deps |
| Go | `go/sqliteipc.go` | `database/sql` + `modernc.org/sqlite` (no CGO) |
| Rust | `rust/src/lib.rs` | `rusqlite` (bundled SQLite) |
| Java | `java/sqliteipc/SqliteIpc.java` | JDBC + Xerial `sqlite-jdbc` |
| C++ | `cpp/sqliteipc.hpp` | SQLite3 C API |

> Python, Rust and C++ are **verified** by multi-process tests and a
> **cross-language** test (see below). Go and Java are complete ports whose
> toolchains aren't installed in this environment — build/run their tests when
> you have the SDK.

## Quick usage (Python)

```python
from ipc_queue import Queue, Lock

q = Queue("shared.db")
q.enqueue("hello")

item = q.dequeue()          # -> (id, payload) or None
if item:
    print(item[1])

with Lock("shared.db", "my_lock"):
    # exclusive critical section across all processes
    ...
```

The other languages expose the same operations: `enqueue` / `dequeue` /
`pending_count` on a Queue, and `acquire` / `release` / `with_lock` on a Lock.

---

## Unified TIANTA IPC layer (`sdk/`)

A **function-style, protocol-level** API layer designed to be identical across
all five languages, on top of the low-level Queue/Lock, using the **TIANTA
(`TI_` prefix)** naming convention. Full spec:
[`sdk/DESIGN.md`](sdk/DESIGN.md). Python reference impl: `python/tianta.py`.

- **Storage defaults**: `/opt/tianta/sys/platform.dat` (auto-created), log at
  `/opt/tianta/sys/<app>.log` (size-based rolling). Overridable via env.
- **Basic API**: `TI_connect`, `TI_queue_enqueue/dequeue/pending`,
  `TI_lock_acquire/release/with`
- **Task API** (carried by keyed queue messages + subscribe-push):
  `TI_task_post/wait/subscribe/unsubscribe` with four task key constants
  (`TI_KEY_TM_DONE`, `TI_KEY_CMD_DONE`, `TI_KEY_DL_REQ`, `TI_KEY_SYS_NTF`)
- **Logging**: `TI_log_setup`, `TI_log_info/warn/error`, `TI_log_report_error`

Verified by multi-process tests in **all five languages** (Python, Go, Rust,
Java, C++) plus a cross-language interop test (see `tests/test_tianta_interop.py`
for C++ ↔ Python sharing one file).

### Language ports (all verified)

| Lang | TIANTA layer | tasks layer | notes |
|------|--------------|-------------|-------|
| Python | `python/tianta.py` | `python/tianta_tasks.py` | stdlib `sqlite3`, zero deps |
| Go | `go/tianta.go` | `go/tianta_tasks.go` | `modernc.org/sqlite` (no CGO) |
| Rust | `rust/src/tianta.rs` | `rust/src/tianta_tasks.rs` | `rusqlite` (bundled) |
| Java | `java/sqliteipc/Tianta.java` | `java/sqliteipc/TiantaTasks.java` | Xerial `sqlite-jdbc` |
| C++ | `cpp/tianta.hpp` | (in test) | `sqlite3` C API |

Each port uses the identical `TI_*` names/semantics, the same unified schema
(with the `key` column) and supports the open-ended extension fields contract.

### High-level task objects (`python/tianta_tasks.py`)

Four **task-specific objects** built on `tianta.py`, each with structured
payload + subscribe/on + blocking wait, for the four satellite business tasks:

| Object | key | 业务 |
|--------|-----|------|
| `TmRecordDone` | `TI_KEY_TM_DONE` | 卫星遥测数据文件记录完成 |
| `CmdFileDone` | `TI_KEY_CMD_DONE` | 新的指令文件存储完成 |
| `DownloadReq` | `TI_KEY_DL_REQ` | 有下传申请 |
| `SystemNotice` | `TI_KEY_SYS_NTF` | 有系统通知 |

```python
from tianta_tasks import TmRecordDone, SystemNotice, DownloadReq

# 投递(结构化字段自动组装为 JSON)
TmRecordDone.post(ch, file_path="/data/tm.bin", size=4096, station="A")

# 订阅回调(后台线程推送)
DownloadReq.on(ch, lambda ev: print("下传申请", ev.req_id, ev.data_type))

# 阻塞等待
ev = SystemNotice.wait(ch, timeout_ms=5000)
if ev: print(ev.level, ev.title, ev.code)
```

Events are `dict`-like and expose attribute access: `ev.file_path` ==
`ev["file_path"]`. Verified: `python3 tests/test_tianta_tasks.py`.

**Extension / custom fields**: every task object accepts **any** extra keyword
in `post()` beyond its predefined fields — they are preserved as-is at the
top-level of the payload (no allow-list filtering). Consumed events carry them
through automatically:

```python
# custom fields (mission_id, orbit_height_km, freq) are NOT in FIELDS
TmRecordDone.post(ch, file_path="tm.bin", size=99,
                  mission_id="MISS-2026", orbit_height_km=600, freq=145.8)

ev = TmRecordDone.wait(ch)          # -> ev.mission_id == "MISS-2026"
ev.metadata()                       # -> {"mission_id": ..., "orbit_height_km": ..., "freq": ...}
ev.add_metadata("antenna", "S-BAND")# append another extension field; ev.antenna == "S-BAND"
```

Helpers: `TaskEvent.metadata()` returns only the custom (non-predefined) fields;
`TaskEvent.add_metadata(k, v)` appends one (returns `self` for chaining).


## Testing

Each verified port includes a real multi-process test that spawns OS processes
and checks for:
- **queue**: every produced message consumed exactly once (no loss, no dupes)
- **lock**: a shared counter incremented `N` times by `M` processes equals `N*M`
  exactly (no lost updates ⇒ real mutual exclusion)

```bash
# Python (stdlib only) — low-level + unified TIANTA layer (base/tasks/subscribe/log)
python3 tests/test_python.py
python3 tests/test_tianta.py
python3 tests/test_tianta_tasks.py

# Rust (real processes via the worker binary)
cd rust && cargo test --test integration --test tianta -- --test-threads=1

# Go
cd go && go mod tidy && go test ./...

# C++ (real processes via fork/exec)
clang++ -std=c++17 -pthread cpp/sqliteipc.cpp -lsqlite3 -o /tmp/sqliteipc_test
SQLITEIPC_BIN=/tmp/sqliteipc_test /tmp/sqliteipc_test
clang++ -std=c++17 -pthread cpp/tianta_test.cpp -lsqlite3 -o /tmp/tianta_cpp
TIANTA_CPP=/tmp/tianta_cpp /tmp/tianta_cpp

# Java (needs JDK + sqlite-jdbc + slf4j on classpath)
javac -cp sqlite-jdbc.jar java/sqliteipc/*.java
java --enable-native-access=ALL-UNNAMED -cp .:sqlite-jdbc.jar:slf4j-api.jar:slf4j-nop.jar sqliteipc.TiantaTest

# Cross-language interop: unified TIANTA layer, C++ <-> Python share one file
TIANTA_CPP=/tmp/tianta_cpp python3 tests/test_tianta_interop.py
```

## Notes & caveats

- **Use somewhere other than a network filesystem** (NFS/SMB) — SQLite's file
  locks are unreliable over NFS.
- **One process == one or more connections.** Each process opens its own
  connection; don't share a single `Connection` across threads in Java/Rust C
  API unless it's designed for it (Go's `database/sql` pool is fine, Python's
  `check_same_thread=False` connection is kept single-threaded here).
- Dequeue marks items `done` immediately (at-most-once). If you want
  "in-flight + retry", add a `lease_until` column and a takeover reaper.
- The lock is **advisory** + **default 60 s stale timeout** — every process
  must go through the lock for it to mean anything.
```
