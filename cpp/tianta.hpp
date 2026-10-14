// TIANTA unified IPC layer (C++ port of python/tianta.py).
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
//
// JSON payloads are represented as std::map<std::string,std::string> (simple,
// extension-friendly). Compile/link with -lsqlite3 and -pthread.
#pragma once

#include <sqlite3.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace tianta {

// ---------------------------------------------------------------------------
// Task keys (identical names/values in every language port)
// ---------------------------------------------------------------------------
inline const char* TI_KEY_TM_DONE = "tm_record_done";
inline const char* TI_KEY_CMD_DONE = "cmd_file_done";
inline const char* TI_KEY_DL_REQ = "download_request";
inline const char* TI_KEY_SYS_NTF = "system_notification";

inline const char* TI_DEFAULT_DIR = "/opt/tianta/sys";

// Represents a JSON object as a flat key->string map (extension-friendly).
using Json = std::map<std::string, std::string>;

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------
class TiHandle {
public:
    sqlite3* db = nullptr;
    std::string path;
    TiHandle(sqlite3* db, const std::string& path) : db(db), path(path) {}
    ~TiHandle() { if (db) sqlite3_close(db); }
    TiHandle(const TiHandle&) = delete;
    TiHandle& operator=(const TiHandle&) = delete;
};

inline std::string TI_default_db() {
    if (const char* e = std::getenv("TIANTA_PLATFORM_DB")) return e;
    return std::string(TI_DEFAULT_DIR) + "/platform.dat";
}

inline int64_t TI_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// connect
// ---------------------------------------------------------------------------
inline TiHandle* TI_connect(const std::string& db = "") {
    std::string path = db.empty() ? TI_default_db() : db;
    // auto-create parent dir
    size_t slash = path.find_last_of('/');
    if (slash != std::string::npos) {
        std::string cmd = "mkdir -p '" + path.substr(0, slash) + "'";
        // use system call guarded; ignore failure
        (void)!::system(cmd.c_str());
    }
    sqlite3* raw = nullptr;
    if (sqlite3_open(path.c_str(), &raw) != SQLITE_OK) {
        std::string e = raw ? sqlite3_errmsg(raw) : "open failed";
        if (raw) sqlite3_close(raw);
        throw std::runtime_error("TI_connect: " + e);
    }
    sqlite3_busy_timeout(raw, 30000);
    char* err = nullptr;
    sqlite3_exec(raw, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;", nullptr, nullptr, &err);
    if (err) { sqlite3_free(err); err = nullptr; }
    const char* schema =
        "CREATE TABLE IF NOT EXISTS queue ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  key TEXT NOT NULL DEFAULT 'default',"
        "  payload TEXT NOT NULL,"
        "  status TEXT NOT NULL DEFAULT 'pending',"
        "  created_at INTEGER NOT NULL);"
        "CREATE INDEX IF NOT EXISTS idx_queue_key_status ON queue(key, status, id);"
        "CREATE TABLE IF NOT EXISTS locks ("
        "  name TEXT PRIMARY KEY, holder TEXT, acquired INTEGER);";
    sqlite3_exec(raw, schema, nullptr, nullptr, &err);
    if (err) {
        std::string e = err;
        sqlite3_free(err);
        sqlite3_close(raw);
        throw std::runtime_error("TI_connect schema: " + e);
    }
    return new TiHandle(raw, path);
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
inline void TI_exec(TiHandle* h, const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(h->db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string e = err ? err : "unknown";
        if (err) sqlite3_free(err);
        throw std::runtime_error(e);
    }
}

inline void TI_begin_immediate(TiHandle* h) { TI_exec(h, "BEGIN IMMEDIATE"); }
inline void TI_commit(TiHandle* h) { TI_exec(h, "COMMIT"); }
inline void TI_rollback(TiHandle* h) { TI_exec(h, "ROLLBACK"); }

inline std::string TI_quote(const std::string& s) {
    std::string out;
    for (char c : s) {
        if (c == '\'') out += "''";
        else out += c;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Basic API — queue
// ---------------------------------------------------------------------------
inline int64_t TI_queue_enqueue(TiHandle* h, const std::string& key, const std::string& payload) {
    std::string sql = "INSERT INTO queue (key, payload, created_at) VALUES ('" +
                      TI_quote(key) + "', '" + TI_quote(payload) + "', " +
                      std::to_string(TI_now_ms()) + ");";
    char* err = nullptr;
    if (sqlite3_exec(h->db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string e = err ? err : "enqueue failed";
        if (err) sqlite3_free(err);
        throw std::runtime_error(e);
    }
    return sqlite3_last_insert_rowid(h->db);
}

struct QueueItem {
    int64_t id;
    std::string key;
    std::string payload;
};

// TI_queue_dequeue(h, "", timeout) -> any key; key non-empty -> filter.
inline bool TI_queue_dequeue(TiHandle* h, const std::string& key, int timeout_ms, QueueItem& out) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
        TI_begin_immediate(h);
        std::string cond = key.empty() ? "" : " AND key='" + TI_quote(key) + "'";
        std::string sql = "SELECT id, key, payload FROM queue WHERE status='pending'" + cond +
                          " ORDER BY id LIMIT 1;";
        bool found = false;
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(h->db, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) {
                out.id = sqlite3_column_int64(st, 0);
                out.key = reinterpret_cast<const char*>(sqlite3_column_text(st, 1));
                out.payload = reinterpret_cast<const char*>(sqlite3_column_text(st, 2));
                found = true;
            }
            sqlite3_finalize(st);
        }
        if (found) {
            std::string upd = "UPDATE queue SET status='done' WHERE id=" + std::to_string(out.id) + ";";
            sqlite3_exec(h->db, upd.c_str(), nullptr, nullptr, nullptr);
            TI_commit(h);
            return true;
        }
        TI_commit(h);
        if (timeout_ms <= 0 || std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

inline int64_t TI_queue_pending(TiHandle* h, const std::string& key) {
    std::string cond = key.empty() ? "" : " WHERE key='" + TI_quote(key) + "' AND status='pending'";
    if (key.empty()) cond = " WHERE status='pending'";
    std::string sql = "SELECT COUNT(*) FROM queue" + cond + ";";
    sqlite3_stmt* st = nullptr;
    int64_t n = 0;
    if (sqlite3_prepare_v2(h->db, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    return n;
}

// ---------------------------------------------------------------------------
// Basic API — lock
// ---------------------------------------------------------------------------
inline constexpr int64_t TI_LOCK_STALE_MS = 60000;

inline void TI_ensure_lock_row(TiHandle* h, const std::string& name) {
    std::string sql = "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES ('" +
                      TI_quote(name) + "', NULL, NULL);";
    sqlite3_exec(h->db, sql.c_str(), nullptr, nullptr, nullptr);
}

inline bool TI_lock_acquire(TiHandle* h, const std::string& name, int timeout_ms) {
    TI_ensure_lock_row(h, name);
    std::string holder = "pid-" + std::to_string(::getpid());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
        TI_begin_immediate(h);
        int64_t now = TI_now_ms();
        int64_t cutoff = now - TI_LOCK_STALE_MS;
        // query held
        bool held = false;
        int64_t heldAt = 0;
        {
            std::string sql = "SELECT acquired FROM locks WHERE name='" + TI_quote(name) +
                              "' AND acquired IS NOT NULL;";
            sqlite3_stmt* st = nullptr;
            if (sqlite3_prepare_v2(h->db, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
                if (sqlite3_step(st) == SQLITE_ROW) { held = true; heldAt = sqlite3_column_int64(st, 0); }
                sqlite3_finalize(st);
            }
        }
        bool took = false;
        if (!held) {
            std::string upd = "UPDATE locks SET holder='" + TI_quote(holder) +
                              "', acquired=" + std::to_string(now) +
                              " WHERE name='" + TI_quote(name) + "';";
            sqlite3_exec(h->db, upd.c_str(), nullptr, nullptr, nullptr);
            took = sqlite3_changes(h->db) > 0;
        } else if (heldAt <= cutoff) {
            std::string upd = "UPDATE locks SET holder='" + TI_quote(holder) +
                              "', acquired=" + std::to_string(now) +
                              " WHERE name='" + TI_quote(name) + "' AND acquired=" +
                              std::to_string(heldAt) + ";";
            sqlite3_exec(h->db, upd.c_str(), nullptr, nullptr, nullptr);
            took = sqlite3_changes(h->db) > 0;
        }
        TI_commit(h);
        if (took) return true;
        if (timeout_ms <= 0 || std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

inline void TI_lock_release(TiHandle* h, const std::string& name) {
    std::string sql = "UPDATE locks SET holder=NULL, acquired=NULL WHERE name='" +
                      TI_quote(name) + "';";
    sqlite3_exec(h->db, sql.c_str(), nullptr, nullptr, nullptr);
}

template <typename Fn>
inline void TI_lock_with(TiHandle* h, const std::string& name, int timeout_ms, Fn fn) {
    if (!TI_lock_acquire(h, name, timeout_ms)) {
        throw std::runtime_error("could not acquire lock: " + name);
    }
    try {
        fn();
    } catch (...) {
        TI_lock_release(h, name);
        throw;
    }
    TI_lock_release(h, name);
}

// ---------------------------------------------------------------------------
// Task API — post / wait / subscribe / unsubscribe
// ---------------------------------------------------------------------------
inline int64_t TI_task_post(TiHandle* h, const std::string& key, const std::string& payload) {
    return TI_queue_enqueue(h, key, payload);
}

inline bool TI_task_wait(TiHandle* h, const std::string& key, int timeout_ms, std::string& payload) {
    QueueItem item;
    if (TI_queue_dequeue(h, key, timeout_ms, item)) {
        payload = item.payload;
        return true;
    }
    return false;
}

// Simple JSON escape/unescape (suffix-level correctness; keys/values are the
// pre-escaped C++ values, so we only need minimal escaping).
inline std::string TI_json_str(const std::string& s) {
    std::ostringstream o;
    o << '"';
    for (char c : s) {
        if (c == '"') o << "\\\"";
        else if (c == '\\') o << "\\\\";
        else o << c;
    }
    o << '"';
    return o.str();
}

// Serialize a Json map into a JSON object string.
inline std::string TI_json_encode(const Json& m) {
    std::ostringstream o;
    o << '{';
    bool first = true;
    for (const auto& kv : m) {
        if (!first) o << ',';
        first = false;
        o << TI_json_str(kv.first) << ':' << TI_json_str(kv.second);
    }
    o << '}';
    return o.str();
}

// Minimal JSON object parser into a key->string map (flat, extension-friendly).
inline Json TI_json_decode(const std::string& s) {
    Json out;
    std::istringstream in(s);
    char c;
    in >> c; // skip '{'
    std::string key, val;
    bool infield = true;
    auto readStr = [&in](std::string& dst) {
        char q;
        in >> q; // '"'
        dst.clear();
        char ch;
        while (in.get(ch)) {
            if (ch == '\\') { in.get(ch); dst += ch; }
            else if (ch == '"') break;
            else dst += ch;
        }
    };
    for (;;) {
        char t = in.peek();
        if (t == '}' || in.eof()) break;
        readStr(key);
        in >> c; // ':'
        // determine value type: string or number/bool
        char p = in.peek();
        if (p == '"') {
            readStr(val);
        } else {
            // read raw token until , or }
            std::string tok;
            while (in.get(t) && t != ',' && t != '}') tok += t;
            val = tok;
            out[key] = val;
            if (t == '}') break;
            continue;
        }
        out[key] = val;
        char sep = in.peek();
        if (sep == ',') in >> sep;
    }
    return out;
}

// subscribe registry
inline struct SubRegistry {
    std::mutex mu;
    std::map<uint64_t, std::atomic<bool>*> stops;
} g_subs;
inline std::atomic<uint64_t> g_sub_seq{0};

inline uint64_t TI_task_subscribe(TiHandle* h, const std::string& key,
                                  void (*callback)(const Json&)) {
    uint64_t tag = g_sub_seq.fetch_add(1) + 1;
    auto* stop = new std::atomic<bool>(false);
    g_subs.mu.lock();
    g_subs.stops[tag] = stop;
    g_subs.mu.unlock();

    std::string db_path = h->path;
    std::string key2 = key;
    std::thread t([db_path, key2, stop, callback]() {
        TiHandle* sub = TI_connect(db_path);
        while (!stop->load()) {
            QueueItem item;
            if (TI_queue_dequeue(sub, key2, 200, item)) {
                callback(TI_json_decode(item.payload));
            }
        }
        delete sub;
    });
    t.detach();
    return tag;
}

inline void TI_task_unsubscribe(TiHandle*, const std::string&, uint64_t tag) {
    g_subs.mu.lock();
    auto it = g_subs.stops.find(tag);
    if (it != g_subs.stops.end()) {
        it->second->store(true);
        delete it->second;
        g_subs.stops.erase(it);
    }
    g_subs.mu.unlock();
}

// ---------------------------------------------------------------------------
// Logging: rolling log file + error report
// ---------------------------------------------------------------------------
inline struct Logger {
    std::mutex mu;
    std::string path;
    int64_t max_bytes = 5 * 1024 * 1024;
    int backups = 3;
} g_logger;

inline std::string TI_log_setup(const std::string& app, const std::string& dir = "",
                                int64_t max_bytes = 0, int backups = 0) {
    std::string d = dir;
    if (d.empty()) {
        if (const char* e = std::getenv("TIANTA_PLATFORM_DIR")) d = e;
        else d = TI_DEFAULT_DIR;
    }
    std::string cmd = "mkdir -p '" + d + "'";
    (void)!::system(cmd.c_str());
    std::lock_guard<std::mutex> lk(g_logger.mu);
    if (max_bytes > 0) g_logger.max_bytes = max_bytes;
    if (backups > 0) g_logger.backups = backups;
    g_logger.path = d + "/" + app + ".log";
    return g_logger.path;
}

inline void TI_log_rotate() {
    std::ifstream in(g_logger.path, std::ios::binary | std::ios::ate);
    if (!in) return;
    if (in.tellg() < g_logger.max_bytes) return;
    in.close();
    for (int i = g_logger.backups; i >= 1; i--) {
        std::string src = g_logger.path + "." + std::to_string(i);
        std::string dst = g_logger.path + "." + std::to_string(i + 1);
        (void)!::rename(src.c_str(), dst.c_str());
    }
    (void)!::rename(g_logger.path.c_str(), (g_logger.path + ".1").c_str());
}

inline void TI_log_write(const std::string& level, const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_logger.mu);
    if (g_logger.path.empty()) return;
    TI_log_rotate();
    std::ofstream f(g_logger.path, std::ios::app);
    f << "[" << TI_now_ms() << "] [" << level << "] " << msg << "\n";
}

inline void TI_log_info(const std::string& m) { TI_log_write("INFO", m); }
inline void TI_log_warn(const std::string& m) { TI_log_write("WARN", m); }
inline void TI_log_error(const std::string& m) { TI_log_write("ERROR", m); }
inline void TI_log_report_error(const std::string& m) { TI_log_write("ERROR", m); }

}  // namespace tianta
