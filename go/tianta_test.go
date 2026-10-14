package sqliteipc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempDB(t *testing.T, tag string) *TiHandle {
	dir := t.TempDir()
	db := filepath.Join(dir, "platform.dat")
	h, err := TIConnect(db)
	if err != nil {
		t.Fatalf("TIConnect: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func TestTiBasicQueueLock(t *testing.T) {
	h := tempDB(t, "basic")
	if _, err := TIQueueEnqueue(h, "alpha", "A1"); err != nil {
		t.Fatal(err)
	}
	if _, err := TIQueueEnqueue(h, "beta", "B1"); err != nil {
		t.Fatal(err)
	}
	if n, _ := TIQueuePending(h, ""); n != 2 {
		t.Fatalf("pending=%d want 2", n)
	}
	item, found, err := TIQueueDequeue(h, "alpha", 1000)
	if err != nil || !found || item.Key != "alpha" || item.Payload != "A1" {
		t.Fatalf("dequeue: %v found=%v item=%+v", err, found, item)
	}
	if n, _ := TIQueuePending(h, "alpha"); n != 0 {
		t.Fatalf("alpha pending=%d", n)
	}

	if ok, _ := TILockAcquire(h, "lk1", 0); !ok {
		t.Fatal("acquire lk1 failed")
	}
	if ok, _ := TILockAcquire(h, "lk2", 0); !ok {
		t.Fatal("acquire lk2 failed")
	}
	if err := TILockRelease(h, "lk1"); err != nil {
		t.Fatal(err)
	}
	t.Log("basic queue+lock OK")
}

func TestTiTaskPostWait(t *testing.T) {
	h := tempDB(t, "task")
	// base task
	if _, err := TITaskPost(h, TIKeySysNtf, `{"level":"warn"}`); err != nil {
		t.Fatal(err)
	}
	raw, found, err := TITaskWait(h, TIKeySysNtf, 1000)
	if err != nil || !found || raw != `{"level":"warn"}` {
		t.Fatalf("wait: %v found=%v raw=%s", err, found, raw)
	}
	// high-level object
	ev := TmRecordDone.Wait(h, 100)
	// no event yet; post one
	if _, err := TmRecordDone.Post(h, map[string]interface{}{
		"file_path": "/data/tm.bin",
		"size":      99,
		"station":   "STA",
	}); err != nil {
		t.Fatal(err)
	}
	ev = TmRecordDone.Wait(h, 1000)
	if ev == nil {
		t.Fatal("no tm event")
	}
	if ev.GetString("file_path") != "/data/tm.bin" || ev.GetInt64("size") != 99 {
		t.Fatalf("bad fields: %+v", ev.Data)
	}
	t.Log("task post/wait OK")
}

func TestTiTaskExtensionFields(t *testing.T) {
	h := tempDB(t, "ext")
	if _, err := TmRecordDone.Post(h, map[string]interface{}{
		"file_path":   "tm.bin",
		"mission_id":  "MISS-2026",
		"orbit_height": 600,
	}); err != nil {
		t.Fatal(err)
	}
	ev := TmRecordDone.Wait(h, 1000)
	if ev == nil {
		t.Fatal("no event")
	}
	if ev.Get("mission_id") != "MISS-2026" || ev.GetInt64("orbit_height") != 600 {
		t.Fatalf("custom fields lost: %+v", ev.Data)
	}
	md := ev.Metadata()
	if md["mission_id"] != "MISS-2026" {
		t.Fatalf("metadata missing custom: %+v", md)
	}
	if _, ok := md["file_path"]; ok {
		t.Fatalf("predefined leaked into metadata: %+v", md)
	}
	t.Log("task extension fields OK")
}

func TestTiTaskSubscribeOn(t *testing.T) {
	h := tempDB(t, "sub")
	got := make(chan string, 4)
	tag := DownloadReq.On(h, func(ev *TaskEvent) {
		got <- ev.GetString("req_id")
	})
	time.Sleep(300 * time.Millisecond)
	DownloadReq.Post(h, map[string]interface{}{"req_id": "r1", "data_type": "img"})
	select {
	case id := <-got:
		if id != "r1" {
			t.Fatalf("got %q", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no subscribe delivery")
	}
	DownloadReq.Off(h, tag)
	t.Log("task subscribe on/off OK")
}

func TestTiLoggingRolling(t *testing.T) {
	dir := t.TempDir()
	p := TILogSetup("platform", dir, 300, 2)
	for i := 0; i < 80; i++ {
		TILogInfo("row")
		TILogError("boom critical")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("log not created: %v", err)
	}
	if _, err := os.Stat(p + ".1"); err != nil {
		t.Fatalf("no rotation: %v", err)
	}
	t.Log("logging rolling OK")
}
