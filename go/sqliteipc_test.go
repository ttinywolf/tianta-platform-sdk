package sqliteipc

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func openTest(t *testing.T) (string, *sql.DB, context.Context) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return path, db, context.Background()
}

func TestQueueMultiprocess(t *testing.T) {
	// Simulate concurrent producers/consumers with goroutines sharing the
	// same *sql.DB (the driver serializes on the SQLite write lock, same as
	// separate processes would).
	_, db, ctx := openTest(t)
	q := NewQueue(db)

	var wg sync.WaitGroup
	// producers
	nProducers, nPer := 4, 50
	for p := 0; p < nProducers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < nPer; i++ {
				if _, err := q.Enqueue(ctx, "msg-"+strconv.Itoa(p)+"-"+strconv.Itoa(i)); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	total := nProducers * nPer
	n, err := q.PendingCount(ctx)
	if err != nil || n != int64(total) {
		t.Fatalf("pending = %d want %d err=%v", n, total, err)
	}

	// consumers: claim until empty, dedupe
	seen := map[string]bool{}
	var mu sync.Mutex
	var cwg sync.WaitGroup
	for c := 0; c < 3; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				_, pl, ok, err := q.Dequeue(ctx)
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				if !ok {
					return // empty
				}
				mu.Lock()
				if seen[pl] {
					t.Errorf("duplicate consumption: %s", pl)
				}
				seen[pl] = true
				mu.Unlock()
			}
		}()
	}
	cwg.Wait()
	if len(seen) != total {
		t.Fatalf("consumed %d want %d", len(seen), total)
	}
}

func TestLockMultiprocess(t *testing.T) {
	_, db, ctx := openTest(t)
	// create the counter table
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT OR REPLACE INTO counter (id, value) VALUES (1, 0)"); err != nil {
		t.Fatal(err)
	}

	l := NewLock(db, "test_lock")
	nProcs, increments := 4, 200
	var wg sync.WaitGroup
	for p := 0; p < nProcs; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				err := l.WithLock(ctx, 10*time.Second, func() error {
					var v int
					if err := db.QueryRowContext(ctx, "SELECT value FROM counter WHERE id=1").Scan(&v); err != nil {
						return err
					}
					_, err := db.ExecContext(ctx, "UPDATE counter SET value=? WHERE id=1", v+1)
					return err
				})
				if err != nil {
					t.Errorf("WithLock: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	var v int
	if err := db.QueryRowContext(ctx, "SELECT value FROM counter WHERE id=1").Scan(&v); err != nil {
		t.Fatal(err)
	}
	expected := nProcs * increments
	if v != expected {
		t.Fatalf("counter = %d want %d (lost updates)", v, expected)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
