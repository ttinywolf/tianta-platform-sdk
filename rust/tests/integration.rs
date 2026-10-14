//! Cross-process integration tests. Builds the worker binary, spawns multiple
//! OS processes, and verifies queue correctness and lock mutual exclusion.
//!
//! Run: cargo test --test integration -- --test-threads=1

use sqliteipc;
use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn worker_bin() -> PathBuf {
    // The test harness sets CARGO_BIN_EXE_worker when building with cargo.
    PathBuf::from(env!("CARGO_BIN_EXE_worker"))
}

fn spawn_worker(args: &[&str]) -> Child {
    Command::new(worker_bin())
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker")
}

fn temp_db(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqliteipc-{}-{}", std::process::id(), name));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir.join("test.db")
}

#[test]
fn queue_multiprocess() {
    let db = temp_db("queue");
    // seed schema
    let conn = sqliteipc::open(&db).unwrap();
    drop(conn);

    let n_producers = 4;
    let n_per = 50;
    let mut kids = Vec::new();
    for _ in 0..n_producers {
        kids.push(spawn_worker(&["produce", db.to_str().unwrap(), &n_per.to_string()]));
    }
    let mut failed = false;
    for mut k in kids {
        if !k.wait().unwrap().success() {
            failed = true;
        }
    }
    assert!(!failed, "producer failed");

    // verify pending count
    let conn = sqliteipc::open(&db).unwrap();
    let q = sqliteipc::Queue::new(conn);
    let pending = q.pending_count().unwrap();
    assert_eq!(pending, (n_producers * n_per) as i64);
    drop(q);

    // consumers
    let outfile = db.with_extension("out");
    let n_consumers = 3;
    let mut kids = Vec::new();
    for _ in 0..n_consumers {
        kids.push(spawn_worker(&["consume", db.to_str().unwrap(), outfile.to_str().unwrap()]));
    }
    for mut k in kids {
        k.wait().unwrap();
    }

    let content = fs::read_to_string(&outfile).unwrap();
    let items: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(items.len(), n_producers * n_per, "consumed wrong count");
    let unique: std::collections::HashSet<&str> = items.iter().copied().collect();
    assert_eq!(unique.len(), items.len(), "duplicate consumption");

    fs::remove_dir_all(db.parent().unwrap()).ok();
}

#[test]
fn lock_multiprocess() {
    let db = temp_db("lock");
    // seed schema
    let conn = sqliteipc::open(&db).unwrap();
    drop(conn);

    let countfile = db.with_extension("count");
    fs::write(&countfile, "0").unwrap();

    let n_procs = 4;
    let increments = 200;
    let mut kids = Vec::new();
    for _ in 0..n_procs {
        kids.push(spawn_worker(&[
            "lock",
            db.to_str().unwrap(),
            countfile.to_str().unwrap(),
            &increments.to_string(),
        ]));
    }
    let mut failed = false;
    for mut k in kids {
        if !k.wait().unwrap().success() {
            failed = true;
        }
    }
    assert!(!failed, "lock worker failed");

    let v: i64 = fs::read_to_string(&countfile).unwrap().trim().parse().unwrap();
    let expected = (n_procs * increments) as i64;
    assert_eq!(v, expected, "lost updates: {} != {}", v, expected);

    fs::remove_dir_all(db.parent().unwrap()).ok();
}

// Silence unused warnings for Duration import on some platforms.
#[allow(dead_code)]
fn _unused(_d: Duration) {}
