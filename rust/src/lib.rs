//! Cross-process message queue and mutex lock backed by SQLite.
//!
//! Shares the same schema and semantics as the other language ports
//! (python/, go/, java/, cpp/) so processes written in different languages can
//! operate on the same database file.
//!
//! Design: WAL mode + busy_timeout for write concurrency; `BEGIN IMMEDIATE`
//! transactions for atomic dequeue and lock acquisition (no lost / duplicate
//! consumption, mutual exclusion across processes).

pub mod tianta;
pub mod tianta_tasks;

use rusqlite::{Connection, Transaction, params};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    payload    TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, id);
CREATE TABLE IF NOT EXISTS locks (
    name     TEXT PRIMARY KEY,
    holder   TEXT,
    acquired INTEGER
);
";

/// Opens (creating if needed) the database and applies the shared schema.
pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.pragma_update(None, "journal_mode", &"WAL")?;
    conn.pragma_update(None, "synchronous", &"NORMAL")?;
    exec_with_retry(&conn, SCHEMA)?;
    Ok(conn)
}

/// A minimal FIFO message queue.
pub struct Queue {
    conn: Connection,
    last_id: AtomicI64,
}

impl Queue {
    pub fn new(conn: Connection) -> Self {
        Queue {
            conn,
            last_id: AtomicI64::new(0),
        }
    }

    /// Appends a message, returning its id.
    pub fn enqueue(&mut self, payload: &str) -> rusqlite::Result<i64> {
        // Single INSERT in autocommit mode; retry absorbed by busy_timeout.
        retry_busy(|| {
            self.conn.execute(
                "INSERT INTO queue (payload, created_at) VALUES (?1, ?2)",
                params![payload, now_ms()],
            )
        })?;
        let id = self.conn.last_insert_rowid();
        self.last_id.store(id, Ordering::SeqCst);
        Ok(id)
    }

    /// Claims and returns the oldest pending item as (id, payload), or `None`
    /// when the queue is empty. The item is atomically marked done under an
    /// IMMEDIATE transaction so no two consumers obtain it.
    pub fn dequeue(&mut self) -> rusqlite::Result<Option<(i64, String)>> {
        retry_busy(|| {
            let tx = self.conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            match tx_dequeue(&tx)? {
                Some((id, payload)) => {
                    tx.commit()?;
                    Ok(Some((id, payload)))
                }
                None => {
                    tx.commit()?;
                    Ok(None)
                }
            }
        })
    }

    pub fn pending_count(&self) -> rusqlite::Result<i64> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM queue WHERE status='pending'",
            [],
            |r| r.get(0),
        )?;
        Ok(n)
    }
}

fn tx_dequeue(tx: &Transaction) -> rusqlite::Result<Option<(i64, String)>> {
    let row = tx.query_row(
        "SELECT id, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1",
        [],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
    );
    match row {
        Ok((id, payload)) => {
            tx.execute("UPDATE queue SET status='done' WHERE id=?1", params![id])?;
            Ok(Some((id, payload)))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

/// A named cross-process mutex.
pub struct Lock {
    conn: Connection,
    name: String,
}

const DEFAULT_STALE_AFTER_MS: i64 = 60_000;

impl Lock {
    pub fn new(conn: Connection, name: impl Into<String>) -> Self {
        let name = name.into();
        // Ensure the lock row exists so that the subsequent UPDATE-based
        // acquisition is effective (an UPDATE on a missing row is a no-op).
        let _ = conn.execute(
            "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?1, NULL, NULL)",
            params![name],
        );
        Lock { conn, name }
    }

    /// Tries to acquire the lock, retrying up to `timeout`. Returns false on
    /// timeout (or immediately if `timeout` is zero).
    pub fn acquire(&mut self, timeout: Duration) -> rusqlite::Result<bool> {
        let holder = format!("pid-{}", std::process::id());
        let start = Instant::now();
        loop {
            if self.try_acquire(&holder)? {
                return Ok(true);
            }
            if timeout.is_zero() || start.elapsed() >= timeout {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn try_acquire(&mut self, holder: &str) -> rusqlite::Result<bool> {
        let now = now_ms();
        let cutoff = now - DEFAULT_STALE_AFTER_MS;
        retry_busy(|| {
            let tx = self.conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let held: Option<(String, i64)> = tx
                .query_row(
                    "SELECT holder, acquired FROM locks WHERE name=?1 AND acquired IS NOT NULL",
                    params![self.name],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok();

            match held {
                None => {
                    tx.execute(
                        "UPDATE locks SET holder=?1, acquired=?2 WHERE name=?3",
                        params![holder, now, self.name],
                    )?;
                    tx.commit()?;
                    Ok(true)
                }
                Some((_, acquired_at)) if acquired_at <= cutoff => {
                    // stale lock from a crashed process -> steal it
                    let n = tx.execute(
                        "UPDATE locks SET holder=?1, acquired=?2 WHERE name=?3 AND acquired=?4",
                        params![holder, now, self.name, acquired_at],
                    )?;
                    let ok = n > 0;
                    tx.commit()?;
                    Ok(ok)
                }
                Some(_) => Ok(false),
            }
        })
    }

    /// Releases the lock. Safe even if not currently held.
    pub fn release(&mut self) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?1",
            params![self.name],
        )?;
        Ok(())
    }

    /// Runs `f` while holding the lock, blocking up to `timeout`.
    pub fn with_lock<F, T>(&mut self, timeout: Duration, f: F) -> rusqlite::Result<Option<T>>
    where
        F: FnOnce() -> rusqlite::Result<T>,
    {
        if !self.acquire(timeout)? {
            return Ok(None);
        }
        let res = f();
        self.release()?;
        res.map(Some)
    }
}

fn retry_busy<T>(mut f: impl FnMut() -> rusqlite::Result<T>) -> rusqlite::Result<T> {
    // busy_timeout already blocks; this is a safety net for very rare races.
    let mut last = None;
    for _ in 0..10 {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if is_busy(&e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap())
}

fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error { code: rusqlite::ffi::ErrorCode::DatabaseBusy, .. },
            _
        )
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn exec_with_retry(conn: &Connection, sql: &str) -> rusqlite::Result<()> {
    let mut last = None;
    for _ in 0..10 {
        match conn.execute_batch(sql) {
            Ok(()) => return Ok(()),
            Err(e) if is_busy(&e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap())
}
