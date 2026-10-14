// TIANTA unified IPC layer (Go port of python/tianta.py).
//
// Reference spec: ../sdk/DESIGN.md. Exposes the same TI_* API names/semantics
// as the Python reference so processes in different languages interoperate on
// one shared SQLite file.
//
//   Basic: TI_connect, TI_queue_enqueue/dequeue/pending,
//          TI_lock_acquire/release/with
//   Task:  TI_task_post/wait/subscribe/unsubscribe (+ TI_KEY_* constants)
//   Log:   TI_log_setup, TI_log_info/warn/error, TI_log_report_error
//
// Storage defaults: /opt/tianta/sys/platform.dat (auto-created), log at
// /opt/tianta/sys/<app>.log (size-based rolling).
package sqliteipc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Task message keys (identical names/values in every language port)
const (
	TIKeyTMDone  = "tm_record_done"      // (1) 卫星遥测数据文件记录完成
	TIKeyCmdDone = "cmd_file_done"       // (2) 新的指令文件存储完成
	TIKeyDLReq   = "download_request"    // (3) 有下传申请
	TIKeySysNtf  = "system_notification" // (4) 有系统通知
)

const tiDefaultDir = "/opt/tianta/sys"

const tiSchema = `
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
`

// TiHandle is the per-process handle used by all TIANTA functions.
type TiHandle struct {
	DB    *sql.DB
	Path  string
	subMu sync.Mutex
	subs  map[string]func()
}

func tiDefaultDB() string {
	if v := os.Getenv("TIANTA_PLATFORM_DB"); v != "" {
		return v
	}
	return filepath.Join(tiDefaultDir, "platform.dat")
}

func tiNowMs() int64 { return time.Now().UnixMilli() }

// TIConnect opens/creates the platform database (default /opt/tianta/sys/platform.dat).
func TIConnect(dbPath string) (*TiHandle, error) {
	path := dbPath
	if path == "" {
		path = tiDefaultDB()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection per process: correct for BEGIN IMMEDIATE primitives and
	// avoids pool-induced SQLITE_BUSY. Cross-process reads still run in parallel.
	db.SetMaxOpenConns(1)
	// apply schema with retries to absorb transient busy during setup
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(tiSchema); err == nil {
			break
		} else if i == 9 {
			db.Close()
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return &TiHandle{DB: db, Path: path, subs: map[string]func(){}}, nil
}

func (h *TiHandle) Close() { h.DB.Close() }

// ---------------------------------------------------------------------------
// Basic API — queue
// ---------------------------------------------------------------------------
func TIQueueEnqueue(ch *TiHandle, key, payload string) (int64, error) {
	res, err := ch.DB.Exec("INSERT INTO queue (key, payload, created_at) VALUES (?, ?, ?)",
		key, payload, tiNowMs())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// QueueItem is a dequeue result.
type QueueItem struct {
	ID      int64
	Key     string
	Payload string
}

// TIQueueDequeue takes one pending message; key=="" matches any.
// Blocks up to timeoutMs (0 = try once). Returns (nil,false) when empty.
func TIQueueDequeue(ch *TiHandle, key string, timeoutMs int) (*QueueItem, bool, error) {
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		item, found, err := tiDequeueOnce(ch, key)
		if err != nil {
			return nil, false, err
		}
		if found {
			return item, true, nil
		}
		if timeoutMs <= 0 || time.Now().After(deadline) {
			return nil, false, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func tiDequeueOnce(ch *TiHandle, key string) (*QueueItem, bool, error) {
	tx, err := ch.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	var item QueueItem
	var row *sql.Row
	if key == "" {
		row = tx.QueryRow("SELECT id, key, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1")
	} else {
		row = tx.QueryRow("SELECT id, key, payload FROM queue WHERE status='pending' AND key=? ORDER BY id LIMIT 1", key)
	}
	err = row.Scan(&item.ID, &item.Key, &item.Payload)
	if err == sql.ErrNoRows {
		tx.Commit()
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec("UPDATE queue SET status='done' WHERE id=?", item.ID); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return &item, true, nil
}

func TIQueuePending(ch *TiHandle, key string) (int64, error) {
	var n int64
	var err error
	if key == "" {
		err = ch.DB.QueryRow("SELECT COUNT(*) FROM queue WHERE status='pending'").Scan(&n)
	} else {
		err = ch.DB.QueryRow("SELECT COUNT(*) FROM queue WHERE status='pending' AND key=?", key).Scan(&n)
	}
	return n, err
}

// ---------------------------------------------------------------------------
// Basic API — lock
// ---------------------------------------------------------------------------
const tiLockStaleMs = 60_000

func tiEnsureLockRow(ch *TiHandle, name string) error {
	_, err := ch.DB.Exec("INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?, NULL, NULL)", name)
	return err
}

func TILockAcquire(ch *TiHandle, name string, timeoutMs int) (bool, error) {
	if err := tiEnsureLockRow(ch, name); err != nil {
		return false, err
	}
	holder := "pid-" + strconv.Itoa(os.Getpid())
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		ok, err := tiLockTry(ch, name, holder)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		if timeoutMs <= 0 || time.Now().After(deadline) {
			return false, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tiLockTry(ch *TiHandle, name, holder string) (bool, error) {
	tx, err := ch.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	now := tiNowMs()
	var held sql.NullInt64
	err = tx.QueryRow("SELECT acquired FROM locks WHERE name=? AND acquired IS NOT NULL", name).Scan(&held)
	cutoff := now - tiLockStaleMs
	var ok bool
	if err == sql.ErrNoRows {
		if _, err := tx.Exec("UPDATE locks SET holder=?, acquired=? WHERE name=?", holder, now, name); err != nil {
			return false, err
		}
		ok = true
	} else if err != nil {
		return false, err
	} else if held.Valid && held.Int64 <= cutoff {
		res, err := tx.Exec("UPDATE locks SET holder=?, acquired=? WHERE name=? AND acquired=?", holder, now, name, held.Int64)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		ok = n > 0
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return ok, nil
}

func TILockRelease(ch *TiHandle, name string) error {
	_, err := ch.DB.Exec("UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?", name)
	return err
}

func TILockWith(ch *TiHandle, name string, timeoutMs int, fn func() error) error {
	ok, err := TILockAcquire(ch, name, timeoutMs)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("could not acquire lock %q", name)
	}
	defer TILockRelease(ch, name)
	return fn()
}

// ---------------------------------------------------------------------------
// Task API — post / wait / subscribe / unsubscribe
// ---------------------------------------------------------------------------
func TITaskPost(ch *TiHandle, key, payload string) (int64, error) {
	return TIQueueEnqueue(ch, key, payload)
}

func TITaskWait(ch *TiHandle, key string, timeoutMs int) (string, bool, error) {
	item, found, err := TIQueueDequeue(ch, key, timeoutMs)
	if err != nil || !found {
		return "", false, err
	}
	return item.Payload, true, nil
}

var tiSubSeq int64

func TITaskSubscribe(ch *TiHandle, key string, cb func(payload string)) int64 {
	tiSubSeq++
	tag := tiSubSeq
	stop := make(chan struct{})
	ch.subMu.Lock()
	ch.subs[strconv.FormatInt(tag, 10)] = func() { close(stop) }
	ch.subMu.Unlock()

	path := ch.Path
	go func() {
		subCh, err := TIConnect(path)
		if err != nil {
			return
		}
		defer subCh.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			item, found, err := TIQueueDequeue(subCh, key, 200)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if found {
				cb(item.Payload)
			}
		}
	}()
	return tag
}

func TITaskUnsubscribe(ch *TiHandle, key string, tag int64) {
	ch.subMu.Lock()
	keyName := strconv.FormatInt(tag, 10)
	stop, ok := ch.subs[keyName]
	delete(ch.subs, keyName)
	ch.subMu.Unlock()
	if ok {
		stop()
		_ = key // tag is enough; key kept for API parity
	}
}

// ---------------------------------------------------------------------------
// Logging: rolling log file + error report
// ---------------------------------------------------------------------------
var (
	logMu       sync.Mutex
	logPath     string
	logMaxBytes int64 = 5 * 1024 * 1024
	logBackups  int   = 3
)

// TILogSetup initializes the rolling log; returns the log path.
func TILogSetup(app string, dir string, maxBytes int64, backups int) string {
	if dir == "" {
		if v := os.Getenv("TIANTA_PLATFORM_DIR"); v != "" {
			dir = v
		} else {
			dir = tiDefaultDir
		}
	}
	_ = os.MkdirAll(dir, 0o750)
	if maxBytes > 0 {
		logMaxBytes = maxBytes
	}
	if backups > 0 {
		logBackups = backups
	}
	p := filepath.Join(dir, app+".log")
	logMu.Lock()
	logPath = p
	logMu.Unlock()
	return p
}

func tiLogWrite(level, msg string) {
	logMu.Lock()
	defer logMu.Unlock()
	if logPath == "" {
		return
	}
	tiLogRotate()
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	fmt.Fprintf(f, "[%s] [%s] %s\n", ts, level, msg)
	f.Close()
}

func tiLogRotate() {
	fi, err := os.Stat(logPath)
	if err != nil || fi.Size() < logMaxBytes {
		return
	}
	for i := logBackups; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", logPath, i)
		dst := fmt.Sprintf("%s.%d", logPath, i+1)
		if _, err := os.Stat(src); err == nil {
			os.Rename(src, dst)
		}
	}
	os.Rename(logPath, fmt.Sprintf("%s.1", logPath))
}

func TILogInfo(msg string)   { tiLogWrite("INFO", msg) }
func TILogWarn(msg string)   { tiLogWrite("WARN", msg) }
func TILogError(msg string)  { tiLogWrite("ERROR", msg) }
func TILogReportError(msg string) { tiLogWrite("ERROR", msg) }
