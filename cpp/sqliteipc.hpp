// Cross-process message queue and mutex lock backed by SQLite.
//
// Shares the same schema and semantics as the other language ports (python/,
// go/, rust/, java/) so processes written in different languages can operate
// on the same database file.
//
// Design: WAL mode + busy_timeout for write concurrency; BEGIN IMMEDIATE
// transactions for atomic dequeue and lock acquisition.
//
// Compile (macOS/Linux with libsqlite3 dev files):
//   clang++ -std=c++17 -pthread sqliteipc.cpp -lsqlite3 -o /tmp/sqliteipc_test
#pragma once

#include <sqlite3.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

namespace sqliteipc {

// Opens (creating if needed) the database and applies the shared schema.
inline sqlite3* Open(const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
        std::string msg = sqlite3_errmsg(db);
        sqlite3_close(db);
        throw std::runtime_error("open failed: " + msg);
    }
    sqlite3_busy_timeout(db, 30000);
    char* err = nullptr;
    if (sqlite3_exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;", nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "unknown";
        sqlite3_free(err);
        sqlite3_close(db);
        throw std::runtime_error("pragma failed: " + msg);
    }
    const char* schema =
        "CREATE TABLE IF NOT EXISTS queue ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  payload TEXT NOT NULL,"
        "  status TEXT NOT NULL DEFAULT 'pending',"
        "  created_at INTEGER NOT NULL);"
        "CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, id);"
        "CREATE TABLE IF NOT EXISTS locks ("
        "  name TEXT PRIMARY KEY, holder TEXT, acquired INTEGER);";
    if (sqlite3_exec(db, schema, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "unknown";
        sqlite3_free(err);
        sqlite3_close(db);
        throw std::runtime_error("schema failed: " + msg);
    }
    return db;
}

inline int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Minimal FIFO message queue. Uses one sqlite3* (owned by caller).
struct Queue {
    explicit Queue(sqlite3* db) : db_(db) {}

    // Appends a message, returns its id.
    int64_t Enqueue(const std::string& payload) {
        sqlite3_stmt* st;
        if (sqlite3_prepare_v2(db_,
                "INSERT INTO queue (payload, created_at) VALUES (?1, ?2)",
                -1, &st, nullptr) != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
        sqlite3_bind_text(st, 1, payload.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, NowMs());
        int rc = sqlite3_step(st);
        sqlite3_finalize(st);
        if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db_));
        return sqlite3_last_insert_rowid(db_);
    }

    // Claims and returns the oldest pending item, or nullptr if empty.
    // Caller frees the returned struct. Atomically marks done in an IMMEDIATE
    // transaction so exactly one consumer obtains any given item.
    struct Item { int64_t id; std::string payload; };

    Item* Dequeue() {
        exec("BEGIN IMMEDIATE");
        Item* out = nullptr;
        sqlite3_stmt* st;
        if (sqlite3_prepare_v2(db_,
                "SELECT id, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1",
                -1, &st, nullptr) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) {
                out = new Item{sqlite3_column_int64(st, 0),
                               (const char*)sqlite3_column_text(st, 1)};
            }
            sqlite3_finalize(st);
        }
        if (out) {
            // mark done
            sqlite3_stmt* u;
            if (sqlite3_prepare_v2(db_, "UPDATE queue SET status='done' WHERE id=?1",
                    -1, &u, nullptr) == SQLITE_OK) {
                sqlite3_bind_int64(u, 1, out->id);
                sqlite3_step(u);
                sqlite3_finalize(u);
            }
        }
        exec("COMMIT");
        return out;
    }

    int64_t PendingCount() {
        sqlite3_stmt* st;
        int64_t n = 0;
        if (sqlite3_prepare_v2(db_,
                "SELECT COUNT(*) FROM queue WHERE status='pending'",
                -1, &st, nullptr) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
        return n;
    }

private:
    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown";
            sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }
    sqlite3* db_;
};

// A named cross-process mutex.
struct Lock {
    static constexpr int64_t kDefaultStaleAfterMs = 60'000;

    Lock(sqlite3* db, const std::string& name) : db_(db), name_(name) {
        // Ensure the lock row exists so UPDATE-based acquisition is effective.
        std::string sql = "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES ('"
                          + name_ + "', NULL, NULL);";
        char* err = nullptr;
        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown";
            sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }

    bool Acquire(int64_t timeoutMs) {
        std::string holder = "pid-" + std::to_string(::getpid());
        int64_t deadline = timeoutMs > 0 ? NowMs() + timeoutMs : INT64_MAX;
        while (true) {
            if (TryAcquire(holder)) return true;
            if (timeoutMs == 0 || NowMs() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    bool TryAcquire(const std::string& holder) {
        int64_t now = NowMs();
        int64_t cutoff = now - kDefaultStaleAfterMs;
        exec("BEGIN IMMEDIATE");
        // query current holder
        bool held = false;
        int64_t heldAt = 0;
        {
            std::string sql =
                "SELECT acquired FROM locks WHERE name='" + name_ +
                "' AND acquired IS NOT NULL";
            sqlite3_stmt* st;
            if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) == SQLITE_OK) {
                if (sqlite3_step(st) == SQLITE_ROW) { held = true; heldAt = sqlite3_column_int64(st, 0); }
                sqlite3_finalize(st);
            }
        }
        bool took;
        if (!held) {
            std::string sql = "UPDATE locks SET holder='" + holder +
                              "', acquired=" + std::to_string(now) +
                              " WHERE name='" + name_ + "'";
            took = execAffected(sql) > 0;
        } else if (heldAt <= cutoff) {
            std::string sql = "UPDATE locks SET holder='" + holder +
                              "', acquired=" + std::to_string(now) +
                              " WHERE name='" + name_ + "' AND acquired=" +
                              std::to_string(heldAt);
            took = execAffected(sql) > 0;
        } else {
            took = false;
        }
        exec("COMMIT");
        return took;
    }

    void Release() {
        std::string sql = "UPDATE locks SET holder=NULL, acquired=NULL WHERE name='" + name_ + "'";
        char* err = nullptr;
        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown";
            sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }

    template <typename Fn>
    void WithLock(int64_t timeoutMs, Fn fn) {
        if (!Acquire(timeoutMs)) {
            throw std::runtime_error("could not acquire lock: " + name_);
        }
        try {
            fn();
        } catch (...) {
            Release();
            throw;
        }
        Release();
    }

private:
    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "unknown";
            sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }
    int execAffected(const std::string& sql) {
        sqlite3_stmt* st;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &st, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db_));
        int rc = sqlite3_step(st);
        int changed = sqlite3_changes(db_);
        sqlite3_finalize(st);
        if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db_));
        return changed;
    }
    sqlite3* db_;
    std::string name_;
};

}  // namespace sqliteipc
