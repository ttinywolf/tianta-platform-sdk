// Package sqliteipc provides a cross-process message queue and mutex lock
// backed by SQLite, sharing the same schema as the other language ports
// (python/, rust/, java/, cpp/) so processes in different languages can talk
// to the same database file.
//
// Design: WAL mode + busy_timeout for write concurrency; `BEGIN IMMEDIATE`
// transactions for atomic dequeue and lock acquisition (no lost/duplicate
// consumption, mutual exclusion across processes).
package sqliteipc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
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
`

// Open opens (creating if needed) the database and applies the shared schema.
// It must be called once per process; pass the returned *sql.DB to NewQueue /
// NewLock.
func Open(path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Serialize SQLite ops through one connection: correct for the low-concurrency
	// transaction primitives (BEGIN IMMEDIATE queue claim / lock) and avoids
	// pool-induced SQLITE_BUSY. Cross-process reads still run concurrently.
	db.SetMaxOpenConns(1)
	// Apply schema with retries to absorb transient SQLITE_BUSY during setup.
	for i := 0; i < 10; i++ {
		_, err = db.Exec(schema)
		if err == nil {
			return db, nil
		}
		if isBusy(err) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return nil, err
	}
	return nil, err
}

// Queue is a minimal FIFO message queue.
type Queue struct {
	db *sql.DB
}

func NewQueue(db *sql.DB) *Queue { return &Queue{db: db} }

// Enqueue appends a message and returns its id.
func (q *Queue) Enqueue(ctx context.Context, payload string) (int64, error) {
	res, err := q.db.ExecContext(ctx,
		"INSERT INTO queue (payload, created_at) VALUES (?, ?)",
		payload, time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Dequeue claims and returns the oldest pending item as (id, payload, true),
// or (id empty, false) when the queue is empty. The item is atomically marked
// done under an IMMEDIATE transaction so no two consumers get it.
func (q *Queue) Dequeue(ctx context.Context) (id int64, payload string, ok bool, err error) {
	tx, err := q.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, "", false, err
	}
	defer tx.Rollback() // no-op after commit

	var rowID int64
	var pl string
	err = tx.QueryRowContext(ctx,
		"SELECT id, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1",
	).Scan(&rowID, &pl)
	if err == sql.ErrNoRows {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, err
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE queue SET status='done' WHERE id=?", rowID); err != nil {
		return 0, "", false, err
	}
	if err := tx.Commit(); err != nil {
		// Another writer may have bumped the write lock; retry once by recursion.
		if isBusy(err) {
			time.Sleep(20 * time.Millisecond)
			return q.Dequeue(ctx)
		}
		return 0, "", false, err
	}
	return rowID, pl, true, nil
}

// PendingCount returns the number of unprocessed messages.
func (q *Queue) PendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := q.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM queue WHERE status='pending'").Scan(&n)
	return n, err
}

// Lock is a named cross-process mutex.
type Lock struct {
	db   *sql.DB
	name string
}

const defaultStaleAfter = 60 * time.Second

func NewLock(db *sql.DB, name string) *Lock {
	// Ensure the lock row exists so subsequent UPDATE-based acquisition works
	// (an UPDATE on a missing row is a no-op).
	_, _ = db.Exec(
		"INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?, NULL, NULL)",
		name,
	)
	return &Lock{db: db, name: name}
}

// Acquire tries to take the lock. It returns true on success. With timeout>0
// it retries until success or timeout; with timeout==0 it tries once.
func (l *Lock) Acquire(ctx context.Context, timeout time.Duration) (bool, error) {
	holder := fmt.Sprintf("pid-%d", os.Getpid())
	deadline := time.Now().Add(timeout)
	for {
		got, err := l.tryAcquire(ctx, holder)
		if err != nil {
			return false, err
		}
		if got {
			return true, nil
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (l *Lock) tryAcquire(ctx context.Context, holder string) (bool, error) {
	now := time.Now().UnixMilli()
	cutoff := now - defaultStaleAfter.Milliseconds()
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var acquired sql.NullInt64
	err = tx.QueryRowContext(ctx,
		"SELECT acquired FROM locks WHERE name=? AND acquired IS NOT NULL",
		l.name).Scan(&acquired)
	if err == sql.ErrNoRows {
		// free to take
		if _, err := tx.ExecContext(ctx,
			"UPDATE locks SET holder=?, acquired=? WHERE name=?",
			holder, now, l.name); err != nil {
			return false, err
		}
		return tx.Commit() == nil, nil
	}
	if err != nil {
		return false, err
	}
	if acquired.Valid && acquired.Int64 <= cutoff {
		// stale lock from a crashed process -> steal it
		res, err := tx.ExecContext(ctx,
			"UPDATE locks SET holder=?, acquired=? WHERE name=? AND acquired=?",
			holder, now, l.name, acquired.Int64)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			return tx.Commit() == nil, nil
		}
		return false, nil
	}
	return false, nil
}

// Release unlocks. Release is safe even if the lock is not currently held.
func (l *Lock) Release(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx,
		"UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?", l.name)
	return err
}

// WithLock runs fn while holding the named lock, blocking up to timeout.
func (l *Lock) WithLock(ctx context.Context, timeout time.Duration, fn func() error) error {
	ok, err := l.Acquire(ctx, timeout)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("could not acquire lock %q within %v", l.name, timeout)
	}
	defer l.Release(ctx)
	return fn()
}

func isBusy(err error) bool {
	if err == nil {
		return false
	}
	return contains(err.Error(), "database is locked") || contains(err.Error(), "busy")
}

func contains(s, sub string) bool { return len(sub) > 0 && indexOf(s, sub) >= 0 }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
