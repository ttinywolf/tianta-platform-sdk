//! Worker binary used by the multi-process test.
//!
//! Modes (arg 1):
//!   produce <db> <count>    -> enqueue `count` messages with this pid prefix
//!   consume <db> <outfile>  -> enqueue... dequeue until empty, append to outfile
//!   lock    <db> <count>    -> increment a counter protected by the lock `count` times
//!
//! The counter table is init'd by the test; the counter is written to <db>.count
//! after each increment via the same connection? No - simpler: the counter is a
//! separate file that we read-modify-write under the lock.

use sqliteipc::{Lock, Queue};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mode = &args[1];
    let db = &args[2];

    match mode.as_str() {
        "produce" => {
            let n: usize = args[3].parse().unwrap();
            let mut q = Queue::new(sqliteipc::open(db).unwrap());
            for i in 0..n {
                q.enqueue(&format!("produce-{}-{i}", std::process::id())).unwrap();
            }
        }
        "consume" => {
            let outfile = &args[3];
            let mut q = Queue::new(sqliteipc::open(db).unwrap());
            let mut f = OpenOptions::new().create(true).append(true).open(outfile).unwrap();
            loop {
                match q.dequeue().unwrap() {
                    Some((_, payload)) => {
                        writeln!(f, "{payload}").unwrap();
                    }
                    None => break,
                }
            }
        }
        "lock" => {
            let countfile = &args[3];
            let n: usize = args[4].parse().unwrap();
            let mut lk = Lock::new(sqliteipc::open(db).unwrap(), "test_lock");
            for _ in 0..n {
                let _ = lk.with_lock(Duration::from_secs(10), || {
                    let v: i64 = match fs::read_to_string(countfile) {
                        Ok(s) => s.trim().parse().unwrap_or(0),
                        Err(_) => 0,
                    };
                    let mut f = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(countfile)
                        .unwrap();
                    writeln!(f, "{}", v + 1).unwrap();
                    Ok(())
                });
            }
        }
        other => {
            eprintln!("unknown mode {other}");
            std::process::exit(2);
        }
    }
}
