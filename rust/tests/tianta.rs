//! Tests for the Rust TIANTA layer + high-level task objects.
//!
//! Run: cargo test --test tianta -- --test-threads=1

use serde_json::json;
use sqliteipc::tianta::{self, TI_KEY_TM_DONE};
use sqliteipc::tianta_tasks::{self, TaskEvent};
use std::time::Duration;

fn temp_db(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("tianta-rust-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("platform.dat")
}

#[test]
fn task_key_constants() {
    assert_eq!(TI_KEY_TM_DONE, "tm_record_done");
}

#[test]
fn basic_queue_lock() {
    let db = temp_db("basic");
    let ch = tianta::TI_connect(Some(db.to_str().unwrap())).unwrap();

    tianta::TI_queue_enqueue(&ch, "alpha", "A1").unwrap();
    tianta::TI_queue_enqueue(&ch, "beta", "B1").unwrap();
    assert_eq!(tianta::TI_queue_pending(&ch, None).unwrap(), 2);
    let item = tianta::TI_queue_dequeue(&ch, Some("alpha"), 1000).unwrap();
    assert!(item.is_some());
    let (_, k, p) = item.unwrap();
    assert_eq!(k, "alpha");
    assert_eq!(p, "A1");
    assert_eq!(tianta::TI_queue_pending(&ch, Some("alpha")).unwrap(), 0);

    assert!(tianta::TI_lock_acquire(&ch, "lk1", 0).unwrap());
    assert!(tianta::TI_lock_acquire(&ch, "lk2", 0).unwrap());
    tianta::TI_lock_release(&ch, "lk1").unwrap();
    tianta::TI_lock_release(&ch, "lk2").unwrap();

    let _ = ch.conn.close();
    println!("basic queue+lock OK");
}

#[test]
fn task_post_wait() {
    let db = temp_db("task");
    let ch = tianta::TI_connect(Some(db.to_str().unwrap())).unwrap();
    // base task API
    tianta::TI_task_post(&ch, TI_KEY_TM_DONE, r#"{"f":"t.bin"}"#).unwrap();
    let raw = tianta::TI_task_wait(&ch, TI_KEY_TM_DONE, 1000).unwrap();
    assert_eq!(raw.unwrap(), r#"{"f":"t.bin"}"#);

    // high-level object post/wait
    let tm = tianta_tasks::tm_record_done();
    tm.post(&ch, &[("file_path".into(), json!("/data/tm.bin")),
                   ("size".into(), json!(99)),
                   ("station".into(), json!("STA"))]).unwrap();
    let ev = tm.wait(&ch, 1000).expect("no event");
    assert_eq!(ev.get_str("file_path").unwrap(), "/data/tm.bin");
    assert_eq!(ev.get_i64("size").unwrap(), 99);
    assert_eq!(ev.get_str("station").unwrap(), "STA");
    let _ = ch.conn.close();
    println!("task post/wait OK");
}

#[test]
fn task_extension_fields() {
    let db = temp_db("ext");
    let ch = tianta::TI_connect(Some(db.to_str().unwrap())).unwrap();
    let tm = tianta_tasks::tm_record_done();
    tm.post(&ch, &[
        ("file_path".into(), json!("tm.bin")),
        ("mission_id".into(), json!("MISS-2026")), // custom
        ("freq".into(), json!(145.8)),             // custom
    ]).unwrap();
    let ev = tm.wait(&ch, 1000).expect("no event");
    assert_eq!(ev.get_str("mission_id").unwrap(), "MISS-2026");
    assert_eq!(ev.get("freq").unwrap().as_f64().unwrap(), 145.8);
    // metadata() returns only custom fields
    let md = ev.metadata();
    assert!(md.contains_key("mission_id"), "{:?}", md);
    assert!(!md.contains_key("file_path"), "predefined leaked: {:?}", md);
    println!("task extension fields OK");
}

#[test]
fn task_subscribe_on() {
    let db = temp_db("sub");
    let ch = tianta::TI_connect(Some(db.to_str().unwrap())).unwrap();
    let dl = tianta_tasks::download_req();
    let got: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(vec![]);
    let g = std::sync::Arc::new(got);
    let g2 = g.clone();
    let tag = dl.on(&ch, move |ev: TaskEvent| {
        g2.lock().unwrap().push(ev.get_str("req_id").unwrap_or_default());
    });
    std::thread::sleep(Duration::from_millis(400));
    dl.post(&ch, &[("req_id".into(), json!("r1")), ("data_type".into(), json!("img"))]).unwrap();
    std::thread::sleep(Duration::from_millis(800));
    dl.off(&ch, tag);
    let got = g.lock().unwrap().clone();
    assert!(got.contains(&"r1".to_string()), "{:?}", got);
    let _ = ch.conn.close();
    println!("task subscribe on/off OK");
}

#[test]
fn logging_rolling() {
    let dir = std::env::temp_dir().join(format!("tianta-log-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let p = tianta::TI_log_setup("platform", Some(dir.to_str().unwrap()), Some(300), Some(2));
    for i in 0..80 {
        tianta::TI_log_info(&format!("row {} yyyyyyyyyyyyyyyyyyyy", i));
    }
    tianta::TI_log_error("boom critical");
    assert!(p.exists());
    assert!(std::path::Path::new(&format!("{}.1", p.display())).exists(), "no rotation");
    println!("logging rolling OK");
}
