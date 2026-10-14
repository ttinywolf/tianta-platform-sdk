# TIANTA IPC SDK — 统一 API 设计

> 目标:一套**跨语言统一的进程间通信 SDK**,底层基于 SQLite,
> 提供 **基础 API(队列 + 锁)+ 任务 API(带 key 的消息 + 订阅推送 + 滚动日志)**。
>
> 五种语言(Python / Go / Rust / Java / C++)暴露**同名、同语义**的接口,
> 共享**同一个 SQLite 文件**,可跨语言互操作。
>
> 本文件是权威规范;`python/` 为该规范的**参考实现,**并通过多进程测试验证。

---

## 0. 存储约定(所有语言一致)

| 项 | 值 | 说明 |
|----|----|------|
| 数据目录 | `/opt/tianta/sys` | 不存在则**自动创建**(含父目录,尽量设权限 0o750) |
| 主库文件 | `/opt/tianta/sys/platform.dat` | SQLite 数据库;文件不存在则**自动创建** |
| 日志文件 | `/opt/tianta/sys/<app>.log` | 滚动日志,见 §4 |
| 可覆盖 | 均可通过 env 或显式参数覆盖 | 便于测试,但默认值固定如上 |

**自动创建语义**:`TI_connect()` 负责 `mkdir -p /opt/tianta/sys` 并创建 `platform.dat`
(若缺失)与建表、开 WAL。任何语言调用 `TI_connect()` 都保证数据库就绪。

---

## 1. Schema(所有语言完全一致)

```sql
-- 基础队列(通用消息)
CREATE TABLE IF NOT EXISTS queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    key        TEXT    NOT NULL DEFAULT 'default',   -- 消息分类键
    payload    TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',   -- pending | done
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_key_status ON queue(key, status, id);

-- 命名互斥锁
CREATE TABLE IF NOT EXISTS locks (
    name     TEXT PRIMARY KEY,
    holder   TEXT,
    acquired INTEGER
);
```

> 相比上一版 queue 表新增 `key` 列,用于承载四类任务与任意消息分类。
> 所有语言新实现都必须采用此 Schema。

### 四类任务的消息键(常量,所有语言同名)

| 常量 | 值 | 含义 |
|------|----|------|
| `TI_KEY_TM_DONE` | `tm_record_done` | (1) 卫星遥测数据文件记录完成 |
| `TI_KEY_CMD_DONE` | `cmd_file_done` | (2) 新的指令文件存储完成 |
| `TI_KEY_DL_REQ` | `download_request` | (3) 有下传申请 |
| `TI_KEY_SYS_NTF` | `system_notification` | (4) 有系统通知 |

---

## 2. 基础 API(所有语言同名、同参)

统一句柄概念:`TI_connect()` 返回一个 `TIHandle/Channel`,后续调用都传它。

| 函数 | 签名 | 说明 |
|------|------|------|
| `TI_connect` | `(db=None) -> TIHandle` | 打开/创建库(默认 platform.dat),建表、WAL、自动建目录 |
| `TI_queue_enqueue` | `(ch, key, payload) -> id` | 入队一条带 key 的消息 |
| `TI_queue_dequeue` | `(ch, key=None, timeout_ms=0) -> item \| None` | 取走一条(key 为空则任意);原子标记 done |
| `TI_queue_pending` | `(ch, key=None) -> int` | 队列中 pending 条数(可按 key 过滤) |
| `TI_lock_acquire` | `(ch, name, timeout_ms) -> bool` | 抢命名锁(0=试一次) |
| `TI_lock_release` | `(ch, name)` | 释放命名锁 |
| `TI_lock_with` | `(ch, name, timeout_ms, fn) -> R` | 持锁执行 fn(互斥临界区) |

`item = (id, key, payload)`。

> 兼容性:旧的 `Queue`/`Lock` 类仍保留(上一版交付),新 SDK 是**在其之上**的
> 函数式、跨语言统一的封装层。

---

## 3. 任务 API(贴近任务 + 订阅回调)

统一用"带 key 的队列消息"承载四类任务。所有语言提供同一组函数:

| 函数 | 签名 | 说明 |
|------|------|------|
| `TI_task_post` | `(ch, key, payload) -> id` | 投递一条任务消息(等价 queue_enqueue) |
| `TI_task_wait` | `(ch, key, timeout_ms) -> payload \| None` | **阻塞**等待该 key 的一条任务,超时返回 None |
| `TI_task_subscribe` | `(ch, key, callback, tag=None) -> tag` | **订阅**该 key,后台线程收到即回调 `callback(payload)`;返回 tag 用于退订 |
| `TI_task_unsubscribe` | `(ch, key, tag)` | 退订 |

- `callback(payload)`:阻塞式应用回调,回调抛出异常不导致订阅线程崩溃(记录日志并继续)。
- `tag`:同一 key 可多个订阅者,各自独立回调线程;`TI_task_subscribe` 返回唯一 tag。
- `TI_task_wait` 与 `TI_task_subscribe` 可共存(不同消费者各取所需)。

**四类任务的语义映射**:

```
tm_record_done      <- 遥测数据文件的"记录完成"事件
cmd_file_done       <- 新指令文件的"存储完成"事件
download_request    <- 收到下传申请
system_notification <- 系统通知
```

**推荐用法(贴近任务)**:

```python
ch = TI_connect()
TI_task_post(ch, TI_KEY_TM_DONE, '{"file": "/data/tm-2026.bin", "size": 1024}')

def on_dl(req): process_download(req)
TI_task_subscribe(ch, TI_KEY_DL_REQ, on_dl)

msg = TI_task_wait(ch, TI_KEY_SYS_NTF, timeout_ms=5000)
```

### 3.1 高层任务对象(任务专属封装)

在 §3 的通用任务 API 之上,各语言提供**四个任务专属对象**,带结构化字段 +
"订阅回调 + 阻塞等待"双接口,方法名带业务语义:

| 对象 | key | 业务 | 预定义字段 |
|------|-----|------|-----------|
| `TmRecordDone` | `TI_KEY_TM_DONE` | 遥测数据文件记录完成 | `file_path, size, start_utc, end_utc, station` |
| `CmdFileDone` | `TI_KEY_CMD_DONE` | 指令文件存储完成 | `cmd_id, file_path, cmd_type, priority` |
| `DownloadReq` | `TI_KEY_DL_REQ` | 有下传申请 | `req_id, data_type, target, priority` |
| `SystemNotice` | `TI_KEY_SYS_NTF` | 系统通知 | `level, title, body, code` |

每个对象统一四方法(所有语言同名):

| 方法 | 签名 | 说明 |
|------|------|------|
| `post` | `(ch, **fields) -> id` | 投递一条,结构化字段自动补默认值并序列化为 JSON |
| `on` | `(ch, callback, tag=None) -> tag` | 订阅回调 `callback(TaskEvent)`;错误不崩线程 |
| `off` | `(ch, tag)` | 退订 |
| `wait` | `(ch, timeout_ms=0) -> TaskEvent \| None` | 阻塞等待一条 |

`TaskEvent` 兼作 dict 与属性访问:`ev.file_path == ev["file_path"]`,并携带 `_ts` 触发时间。

> 注:预定义字段仅作**便捷默认值**,`post()` 传参优先;未提供的用默认值。
> 详见 §3.2 扩展字段。

### 3.2 开放扩展字段(用户自定义,所有语言统一约定)

每类任务消息支持**任意用户自定义/扩展字段**,SDK **不做白名单过滤**:

1. **payload 顶层直接加任意 key**。`post()` 可传入任何未在预定义 FIELDS 中
   声明的字段,一律原样保留到 payload 顶层;`on()`/`wait()` 返回的
   `TaskEvent` 完整携带。
2. **metadata 辅助接口**(所有语言统一提供):
   - `ev.metadata() -> dict`:仅返回**自定义(非预定义)**字段。
   - `ev.add_metadata(key, value) -> ev`:追加一个扩展字段,返回自身便于链式。
   - 保留名(不可被覆盖):`_ts`(触发时间)、`_predefined`(内部)。
3. **逆向兼容**:已声明的预定义字段与新增自定义字段共存于同一 payload;
   未来扩展新业务字段时,无需改 SDK,直接用 `post()` 新增 key 或消费端
   按需读取。

```python
# 自定义字段(mission_id / orbit_height_km / freq)不在预定义 FIELDS 中
TmRecordDone.post(ch, file_path="tm.bin", size=99,
                  mission_id="MISS-2026", orbit_height_km=600, freq=145.8)

ev = TmRecordDone.wait(ch)
ev.mission_id        # -> "MISS-2026"
ev.metadata()        # -> {"mission_id": ..., "orbit_height_km": ..., "freq": ...}
ev.add_metadata("antenna", "S-BAND")   # 追加扩展字段
```

---

## 4. 日志 & 错误报告(滚动日志文件)

| 函数 | 签名 | 说明 |
|------|------|------|
| `TI_log_setup` | `(app, dir=None, max_bytes=5MB, backups=3)` | 初始化滚动日志 `platform.log`(或 `<app>.log`) |
| `TI_log_info` / `TI_log_warn` / `TI_log_error` | `(app, msg)` | 分级写日志 |
| `TI_log_report_error` | `(msg)` | 便捷:标记为错误级并带时间戳 |

- 路径:`/opt/tianta/sys/<app>.log`
- **滚动规则**:文件超过 `max_bytes` 时重命名为 `<app>.log.1`…`<app>.log.N`,
  保留 `backups` 份(经典的 size-based rotation)。
- 每行格式:`[ISO8601] [LEVEL] message`
- **所有内部失败**(连库失败、入队失败、异常回调)自动写 error 日志,
  应用无需手动捕获即可留下错误报告。

---

## 5. 线程/进程模型(所有语言遵守)

- **进程级**:不同进程各持独立连接,读写同库 —— 天然多进程安全。
- **线程级**:每个 `TIHandle`/`Connection` 默认只被**一个线程**使用;
  `TI_task_subscribe` 内部为每个订阅者起独立线程 **并单独持连接**(避免跨线程共享连接)。
- 因此:**基础 API** 直接并发安全;订阅推送的**回调在线程中执行**,回调解包注意线程安全。

---

## 6. 跨语言矩阵

| API | Python | Go | Rust | Java | C++ |
|-----|--------|----|----|------|-----|
| TI_connect / schema / auto-create | ✅ | ✅ | ✅ | ✅ | ✅ |
| TI_queue_enqueue/dequeue/pending | ✅ | ✅ | ✅ | ✅ | ✅ |
| TI_lock_acquire/release/with | ✅ | ✅ | ✅ | ✅ | ✅ |
| TI_task_post/wait/subscribe/unsubscribe | ✅ | ✅ | ✅ | ✅ | ✅ |
| 任务对象(post/on/off/wait) | ✅ | ✅ | ✅ | ✅ | ✅ |
| 开放扩展字段(metadata) | ✅ | ✅ | ✅ | ✅ | ✅ |
| TI_log rolling + error report | ✅ | ✅ | ✅ | ✅ | ✅ |

> 五门语言 TIANTA 层均实现并通过测试;跨语言互操作已验证
> (C++ ↔ Python 共享同一文件,见 `tests/test_tianta_interop.py`)。

---

## 7. 参考实现文件

- 规范:`sdk/DESIGN.md`(本文件)
- Python:`python/tianta.py` + `python/tianta_tasks.py`
- Go:`go/tianta.go` + `go/tianta_tasks.go`(`modernc.org/sqlite`)
- Rust:`rust/src/tianta.rs` + `rust/src/tianta_tasks.rs`(`rusqlite`)
- Java:`java/sqliteipc/Tianta.java` + `java/sqliteipc/TiantaTasks.java`(Xerial `sqlite-jdbc`)
- C++:`cpp/tianta.hpp`(`sqlite3` C API)
- 测试:各语言自有测试 + `tests/test_tianta_{tasks,interop}.py`
