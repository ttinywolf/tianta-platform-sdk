//! TIANTA unified IPC layer (Rust port of python/tianta.py).
//!
//! Reference spec: ../sdk/DESIGN.md. Exposes the same `TI_*` API names as the
//! Python reference implementation so processes in different languages can
//! interoperate on one shared SQLite file.
//!
//!   Basic:  TI_connect, TI_queue_enqueue/dequeue/pending,
//!           TI_lock_acquire/release/with
//!   Task:   TI_task_post/wait/subscribe/unsubscribe (+ TI_KEY_* constants)
//!   Log:    TI_log_setup, TI_log_info/warn/error, TI_log_report_error
//!
//! Storage defaults: /opt/tianta/sys/platform.dat (auto-created),
//! log at /opt/tianta/sys/<app>.log (size-based rolling).

use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Task keys (identical names/values in every language port)
// ---------------------------------------------------------------------------
pub const TI_KEY_TM_DONE: &str = "tm_record_done";
pub const TI_KEY_CMD_DONE: &str = "cmd_file_done";
pub const TI_KEY_DL_REQ: &str = "download_request";
pub const TI_KEY_SYS_NTF: &str = "system_notification";

const DEFAULT_DIR: &str = "/opt/tianta/sys";

const SCHEMA: &str = "
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
";

/// Per-process database handle used by all TIANTA functions.
pub struct TiHandle {
    pub db_path: PathBuf,
    pub conn: Connection,
}

fn default_db() -> PathBuf {
    std::env::var("TIANTA_PLATFORM_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_DIR).join("platform.dat"))
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn ensure_dir(path: &Path) {
    if let Some(d) = path.parent() {
        let _ = fs::create_dir_all(d);
        let _ = fs::set_permissions(d, fs::Permissions::from_mode(0o750));
    }
}

// ---------------------------------------------------------------------------
// connect
// ---------------------------------------------------------------------------
pub fn TI_connect(db: Option<&str>) -> rusqlite::Result<TiHandle> {
    let path = db.map(PathBuf::from).unwrap_or_else(default_db);
    ensure_dir(&path);
    let conn = Connection::open(&path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    let _ = conn.pragma_update(None, "journal_mode", &"WAL");
    let _ = conn.pragma_update(None, "synchronous", &"NORMAL");
    run_schema(&conn)?;
    Ok(TiHandle { db_path: path, conn })
}

fn run_schema(conn: &Connection) -> rusqlite::Result<()> {
    for _ in 0..10 {
        match conn.execute_batch(SCHEMA) {
            Ok(()) => return Ok(()),
            Err(e) if is_busy(&e) => thread::sleep(Duration::from_millis(20)),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Basic API — queue
// ---------------------------------------------------------------------------
pub fn TI_queue_enqueue(ch: &TiHandle, key: &str, payload: &str) -> rusqlite::Result<i64> {
    retry_busy(|| {
        ch.conn.execute(
            "INSERT INTO queue (key, payload, created_at) VALUES (?1, ?2, ?3)",
            params![key, payload, now_ms()],
        )?;
        Ok(ch.conn.last_insert_rowid())
    })
}

/// Returns (id, key, payload) or None when empty. Blocks up to timeout_ms.
pub fn TI_queue_dequeue(
    ch: &TiHandle,
    key: Option<&str>,
    timeout_ms: u64,
) -> rusqlite::Result<Option<(i64, String, String)>> {
    let start = Instant::now();
    loop {
        let hit: Option<(i64, String, String)> = retry_busy(|| {
            begin_immediate(&ch.conn)?;
            let row = match key {
                Some(k) => ch.conn.query_row(
                    "SELECT id, key, payload FROM queue WHERE status='pending' AND key=?1 ORDER BY id LIMIT 1",
                    params![k],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
                ),
                None => ch.conn.query_row(
                    "SELECT id, key, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1",
                    [],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
                ),
            };
            match row {
                Ok(item) => {
                    ch.conn.execute("UPDATE queue SET status='done' WHERE id=?1", params![item.0])?;
                    commit(&ch.conn)?;
                    Ok(Some(item))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    let _ = commit(&ch.conn);
                    Ok(None)
                }
                Err(e) => {
                    let _ = rollback(&ch.conn);
                    Err(e)
                }
            }
        })?;
        if hit.is_some() || timeout_ms == 0 {
            return Ok(hit);
        }
        if start.elapsed() >= Duration::from_millis(timeout_ms) {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub fn TI_queue_pending(ch: &TiHandle, key: Option<&str>) -> rusqlite::Result<i64> {
    match key {
        Some(k) => ch.conn.query_row(
            "SELECT COUNT(*) FROM queue WHERE status='pending' AND key=?1",
            params![k],
            |r| r.get(0),
        ),
        None => ch.conn.query_row("SELECT COUNT(*) FROM queue WHERE status='pending'", [], |r| r.get(0)),
    }
}

fn begin_immediate(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")
}
fn commit(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("COMMIT")
}
fn rollback(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("ROLLBACK")
}

// ---------------------------------------------------------------------------
// Basic API — lock
// ---------------------------------------------------------------------------
const LOCK_STALE_MS: i64 = 60_000;

fn ensure_lock_row(ch: &TiHandle, name: &str) -> rusqlite::Result<()> {
    ch.conn.execute(
        "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?1, NULL, NULL)",
        params![name],
    )?;
    Ok(())
}

pub fn TI_lock_acquire(ch: &TiHandle, name: &str, timeout_ms: u64) -> rusqlite::Result<bool> {
    ensure_lock_row(ch, name)?;
    let holder = format!("pid-{}", std::process::id());
    let start = Instant::now();
    loop {
        let took = retry_busy(|| {
            begin_immediate(&ch.conn)?;
            let held: Option<i64> = ch.conn
                .query_row(
                    "SELECT acquired FROM locks WHERE name=?1 AND acquired IS NOT NULL",
                    params![name],
                    |r| r.get(0),
                )
                .ok();
            let current = now_ms();
            let cutoff = current - LOCK_STALE_MS;
            let ok = match held {
                None => {
                    ch.conn.execute(
                        "UPDATE locks SET holder=?1, acquired=?2 WHERE name=?3",
                        params![holder, current, name],
                    )?;
                    true
                }
                Some(a) if a <= cutoff => {
                    let n = ch.conn.execute(
                        "UPDATE locks SET holder=?1, acquired=?2 WHERE name=?3 AND acquired=?4",
                        params![holder, current, name, a],
                    )?;
                    n > 0
                }
                Some(_) => false,
            };
            commit(&ch.conn)?;
            Ok(ok)
        })?;
        if took || timeout_ms == 0 {
            return Ok(took);
        }
        if start.elapsed() >= Duration::from_millis(timeout_ms) {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub fn TI_lock_release(ch: &TiHandle, name: &str) -> rusqlite::Result<()> {
    ch.conn.execute("UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?1", params![name])?;
    Ok(())
}

pub fn TI_lock_with<R>(ch: &TiHandle, name: &str, timeout_ms: u64, f: impl FnOnce() -> R) -> rusqlite::Result<R> {
    if !TI_lock_acquire(ch, name, timeout_ms)? {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let r = f();
    TI_lock_release(ch, name)?;
    Ok(r)
}

// ---------------------------------------------------------------------------
// Task API — post / wait / subscribe / unsubscribe
// ---------------------------------------------------------------------------
pub fn TI_task_post(ch: &TiHandle, key: &str, payload: &str) -> rusqlite::Result<i64> {
    TI_queue_enqueue(ch, key, payload)
}

pub fn TI_task_wait(ch: &TiHandle, key: &str, timeout_ms: u64) -> rusqlite::Result<Option<String>> {
    Ok(TI_queue_dequeue(ch, Some(key), timeout_ms)?.map(|item| item.2))
}

type SubHandle = (Arc<AtomicBool>, Arc<thread::JoinHandle<()>>);

static SUBS: OnceLock<Mutex<HashMap<(String, u64), SubHandle>>> = OnceLock::new();
static SUB_NEXT: AtomicU64 = AtomicU64::new(1);

fn subs() -> &'static Mutex<HashMap<(String, u64), SubHandle>> {
    SUBS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Subscribe to `key`; a background thread calls callback(payload) per message.
/// Returns a tag usable with TI_task_unsubscribe.
pub fn TI_task_subscribe(
    ch: &TiHandle,
    key: &str,
    callback: Box<dyn Fn(String) + Send + Sync + 'static>,
) -> u64 {
    let tag = SUB_NEXT.fetch_add(1, Ordering::SeqCst);
    let db_path = ch.db_path.clone();
    let key = key.to_string();
    let key_for_map = key.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let jh = thread::spawn(move || {
        if let Ok(sub_ch) = TI_connect(Some(db_path.to_str().unwrap())) {
            while !stop2.load(Ordering::SeqCst) {
                match TI_queue_dequeue(&sub_ch, Some(&key), 200) {
                    Ok(Some(item)) => callback(item.2),
                    Ok(None) => {}
                    Err(_) => thread::sleep(Duration::from_millis(50)),
                }
            }
            let _ = sub_ch.conn.close();
        }
    });
    subs().lock().unwrap().insert((key_for_map, tag), (stop, Arc::new(jh)));
    tag
}

pub fn TI_task_unsubscribe(_ch: &TiHandle, key: &str, tag: u64) {
    let entry = subs().lock().unwrap().remove(&(key.to_string(), tag));
    if let Some((stop, jh)) = entry {
        stop.store(true, Ordering::SeqCst);
        // JoinHandle can't be moved out of Arc; just drop the handle.
        drop(jh);
    }
}

// ---------------------------------------------------------------------------
// Logging: rolling log file + error report
// ---------------------------------------------------------------------------
struct Logger {
    path: Mutex<Option<PathBuf>>,
    max_bytes: Mutex<u64>,
    backups: Mutex<usize>,
}

static LOGGER: Logger = Logger {
    path: Mutex::new(None),
    max_bytes: Mutex::new(5 * 1024 * 1024),
    backups: Mutex::new(3),
};

pub fn TI_log_setup(app: &str, dir: Option<&str>, max_bytes: Option<u64>, backups: Option<usize>) -> PathBuf {
    let d = dir
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var("TIANTA_PLATFORM_DIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from(DEFAULT_DIR))
        });
    let _ = fs::create_dir_all(&d);
    if let Some(mb) = max_bytes {
        *LOGGER.max_bytes.lock().unwrap() = mb;
    }
    if let Some(bk) = backups {
        *LOGGER.backups.lock().unwrap() = bk;
    }
    let path = d.join(format!("{app}.log"));
    *LOGGER.path.lock().unwrap() = Some(path.clone());
    path
}

fn log_write(level: &str, msg: &str) {
    let p = LOGGER.path.lock().unwrap().clone();
    if let Some(path) = p {
        let max = *LOGGER.max_bytes.lock().unwrap();
        let backups = *LOGGER.backups.lock().unwrap();
        rotate(&path, max, backups);
        let line = format!("[{}] [{}] {}\n", now_ms(), level, msg);
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&path) {
            let _ = f.write_all(line.as_bytes());
        }
    }
}

fn rotate(path: &Path, max_bytes: u64, backups: usize) {
    if let Ok(md) = fs::metadata(path) {
        if md.len() < max_bytes {
            return;
        }
        for i in (1..=backups).rev() {
            let src = PathBuf::from(format!("{}.{}", path.display(), i));
            let dst = PathBuf::from(format!("{}.{}", path.display(), i + 1));
            if src.exists() {
                let _ = fs::rename(&src, &dst);
            }
        }
        let b = PathBuf::from(format!("{}.1", path.display()));
        let _ = fs::rename(path, &b);
    }
}

pub fn TI_log_info(msg: &str) { log_write("INFO", msg); }
pub fn TI_log_warn(msg: &str) { log_write("WARN", msg); }
pub fn TI_log_error(msg: &str) { log_write("ERROR", msg); }
pub fn TI_log_report_error(msg: &str) { log_write("ERROR", msg); }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
fn retry_busy<T>(mut f: impl FnMut() -> rusqlite::Result<T>) -> rusqlite::Result<T> {
    let mut last = None;
    for _ in 0..10 {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if is_busy(&e) => {
                last = Some(e);
                thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| rusqlite::Error::InvalidQuery))
}

fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error { code: rusqlite::ffi::ErrorCode::DatabaseBusy, .. }, _))
}
