//! TIANTA high-level task objects (Rust port of python/tianta_tasks.py).
//!
//! Four business-specific task types with structured payload + subscribe/on +
//! blocking wait, plus open-ended extension fields:
//!
//!   TM_DONE / CMD_DONE / DL_REQ / SYS_NTF
//!
//! Extension fields are any keys beyond the predefined FIELDS; they are stored
//! at the top level of the JSON payload and preserved on round-trip. Use
//! [`TaskEvent::metadata`] to read only the custom (non-predefined) fields and
//! [`TaskEvent::add_metadata`] to append one.

use crate::tianta::{self, TiHandle};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

// ---------------------------------------------------------------------------
// TaskEvent — mirrors python TaskEvent (get/get_str + metadata helpers)
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct TaskEvent {
    /// Underlying payload map (top-level fields, including predefined + custom).
    pub payload: Map<String, Value>,
    /// Predefined (non-custom) field names of the owning task type.
    predefined: BTreeSet<String>,
}

impl TaskEvent {
    fn new(payload: Map<String, Value>, predefined: &[&str]) -> Self {
        let mut set: BTreeSet<String> = predefined.iter().map(|s| s.to_string()).collect();
        set.insert("_ts".into());
        set.insert("_predefined".into());
        TaskEvent { payload, predefined: set }
    }

    /// Get a field by name (any field, predefined or custom).
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.payload.get(key)
    }

    pub fn get_str(&self, key: &str) -> Option<String> {
        self.payload.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.payload.get(key).and_then(|v| v.as_i64())
    }

    /// Append/overwrite a custom extension field.
    pub fn add_metadata(&mut self, key: &str, value: Value) -> &mut Self {
        if key == "_ts" || key == "_predefined" {
            return self;
        }
        self.payload.insert(key.to_string(), value);
        self
    }

    /// Read only the custom (non-predefined) fields.
    pub fn metadata(&self) -> Map<String, Value> {
        self.payload.iter()
            .filter(|(k, _)| !self.predefined.contains(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Base task — mirrors python _BaseTask
// ---------------------------------------------------------------------------
pub struct TaskSpec {
    pub key: &'static str,
    pub fields: Vec<(&'static str, Value)>, // predefined field defaults
}

impl TaskSpec {
    /// Structured post: merge given fields over defaults, serialize to JSON.
    pub fn post(&self, ch: &TiHandle, fields: &[(String, Value)]) -> rusqlite::Result<i64> {
        let mut map = Map::new();
        map.insert("_ts".into(), json!(now_ms()));
        for (k, v) in &self.fields {
            map.insert(k.to_string(), v.clone());
        }
        for (k, v) in fields {
            map.insert(k.clone(), v.clone());
        }
        let payload = serde_json::to_string(&map).unwrap_or_default();
        tianta::TI_task_post(ch, self.key, &payload)
    }

    pub fn wait(&self, ch: &TiHandle, timeout_ms: u64) -> Option<TaskEvent> {
        let raw = tianta::TI_task_wait(ch, self.key, timeout_ms).ok()??;
        let v: Value = serde_json::from_str(&raw).ok()?;
        let map = v.as_object()?.clone();
        Some(TaskEvent::new(map, &self.predefined_names()))
    }

    pub fn on<F>(&self, ch: &TiHandle, callback: F) -> u64
    where
        F: Fn(TaskEvent) + Send + Sync + 'static,
    {
        let predefined = self.predefined_names();
        tianta::TI_task_subscribe(ch, self.key, Box::new(move |raw| {
            if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                if let Some(map) = v.as_object() {
                    callback(TaskEvent::new(map.clone(), &predefined));
                }
            }
        }))
    }

    pub fn off(&self, ch: &TiHandle, tag: u64) {
        tianta::TI_task_unsubscribe(ch, self.key, tag);
    }

    fn predefined_names(&self) -> Vec<&'static str> {
        self.fields.iter().map(|(k, _)| *k).collect()
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The four task objects (fields mirror python/tianta_tasks.py)
// ---------------------------------------------------------------------------
use std::sync::OnceLock;

fn tm_done() -> &'static TaskSpec {
    static S: OnceLock<TaskSpec> = OnceLock::new();
    S.get_or_init(|| TaskSpec {
        key: tianta::TI_KEY_TM_DONE,
        fields: vec![
            ("file_path", json!("")),
            ("size", json!(0)),
            ("start_utc", json!("")),
            ("end_utc", json!("")),
            ("station", json!("")),
        ],
    })
}

fn cmd_done() -> &'static TaskSpec {
    static S: OnceLock<TaskSpec> = OnceLock::new();
    S.get_or_init(|| TaskSpec {
        key: tianta::TI_KEY_CMD_DONE,
        fields: vec![
            ("cmd_id", json!("")),
            ("file_path", json!("")),
            ("cmd_type", json!("")),
            ("priority", json!(5)),
        ],
    })
}

fn dl_req() -> &'static TaskSpec {
    static S: OnceLock<TaskSpec> = OnceLock::new();
    S.get_or_init(|| TaskSpec {
        key: tianta::TI_KEY_DL_REQ,
        fields: vec![
            ("req_id", json!("")),
            ("data_type", json!("")),
            ("target", json!("")),
            ("priority", json!(5)),
        ],
    })
}

fn sys_ntf() -> &'static TaskSpec {
    static S: OnceLock<TaskSpec> = OnceLock::new();
    S.get_or_init(|| TaskSpec {
        key: tianta::TI_KEY_SYS_NTF,
        fields: vec![
            ("level", json!("info")),
            ("title", json!("")),
            ("body", json!("")),
            ("code", json!("")),
        ],
    })
}

/// Public accessors mirroring python's object names (TmRecordDone etc).
pub fn tm_record_done() -> &'static TaskSpec { tm_done() }
pub fn cmd_file_done() -> &'static TaskSpec { cmd_done() }
pub fn download_req() -> &'static TaskSpec { dl_req() }
pub fn system_notice() -> &'static TaskSpec { sys_ntf() }
