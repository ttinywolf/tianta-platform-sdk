package sqliteipc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TIANTA unified IPC layer (Java port of python/tianta.py).
 *
 * Reference spec: sdk/DESIGN.md. Exposes the same TI_* API names/semantics as
 * the Python reference so processes in different languages interoperate on one
 * shared SQLite file.
 *
 *   Basic: TI_connect, TI_queue_enqueue/dequeue/pending,
 *          TI_lock_acquire/release/with
 *   Task:  TI_task_post/wait/subscribe/unsubscribe (+ TI_KEY_* constants)
 *   Log:   TI_log_setup, TI_log_info/warn/error, TI_log_report_error
 *
 * Storage defaults: /opt/tianta/sys/platform.dat (auto-created), log at
 * /opt/tianta/sys/&lt;app&gt;.log (size-based rolling).
 *
 * Requires the Xerial sqlite-jdbc driver on the classpath.
 */
public final class Tianta {

    public static final String TI_KEY_TM_DONE = "tm_record_done";
    public static final String TI_KEY_CMD_DONE = "cmd_file_done";
    public static final String TI_KEY_DL_REQ = "download_request";
    public static final String TI_KEY_SYS_NTF = "system_notification";

    static final String DEFAULT_DIR = "/opt/tianta/sys";

    private static final String SCHEMA_QUEUE =
        "CREATE TABLE IF NOT EXISTS queue (" +
        "  id INTEGER PRIMARY KEY AUTOINCREMENT," +
        "  key TEXT NOT NULL DEFAULT 'default'," +
        "  payload TEXT NOT NULL," +
        "  status TEXT NOT NULL DEFAULT 'pending'," +
        "  created_at INTEGER NOT NULL);";

    private static final String SCHEMA_LOCK =
        "CREATE TABLE IF NOT EXISTS locks (" +
        "  name TEXT PRIMARY KEY, holder TEXT, acquired INTEGER);";

    private static final String SCHEMA_IDX =
        "CREATE INDEX IF NOT EXISTS idx_queue_key_status ON queue(key, status, id);";

    private Tianta() {}

    static String defaultDb() {
        String v = System.getenv("TIANTA_PLATFORM_DB");
        return v != null && !v.isEmpty() ? v : DEFAULT_DIR + "/platform.dat";
    }

    /** Opens/creates the platform database (default /opt/tianta/sys/platform.dat). */
    public static TiHandle TI_connect(String dbPath) throws SQLException {
        loadDriver();
        String path = dbPath == null || dbPath.isEmpty() ? defaultDb() : dbPath;
        try {
            Files.createDirectories(Paths.get(path).getParent());
        } catch (IOException ignored) {}
        Connection c = DriverManager.getConnection("jdbc:sqlite:" + path);
        try (Statement st = c.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA busy_timeout=30000");
            st.execute("PRAGMA synchronous=NORMAL");
            // Xerial Statement.execute runs one statement; issue each separately.
            st.execute(SCHEMA_QUEUE);
            st.execute(SCHEMA_LOCK);
            st.execute(SCHEMA_IDX);
        }
        return new TiHandle(c, path);
    }

    private static boolean driverLoaded = false;
    private static synchronized void loadDriver() throws SQLException {
        if (driverLoaded) return;
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new SQLException("sqlite-jdbc driver not on classpath", e);
        }
        driverLoaded = true;
    }

    // ------------------------------------------------------------------
    // Queue
    // ------------------------------------------------------------------
    public static long TI_queue_enqueue(TiHandle h, String key, String payload) throws SQLException {
        try (PreparedStatement ps = h.db.prepareStatement(
                "INSERT INTO queue (key, payload, created_at) VALUES (?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, key);
            ps.setString(2, payload);
            ps.setLong(3, System.currentTimeMillis());
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }

    /** Dequeue result. */
    public static final class QueueItem {
        public final long id;
        public final String key;
        public final String payload;
        QueueItem(long id, String key, String payload) { this.id = id; this.key = key; this.payload = payload; }
    }

    public static QueueItem TI_queue_dequeue(TiHandle h, String key, int timeoutMs) throws SQLException {
        long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
        while (true) {
            QueueItem item = dequeueOnce(h, key);
            if (item != null) return item;
            if (timeoutMs <= 0 || System.currentTimeMillis() >= deadline) return null;
            try { Thread.sleep(20); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted", e);
            }
        }
    }

    private static QueueItem dequeueOnce(TiHandle h, String key) throws SQLException {
        // Keep autocommit ON and drive the transaction manually with explicit
        // BEGIN IMMEDIATE / COMMIT (avoids nested-transaction errors under JDBC).
        try (Statement s = h.db.createStatement()) { s.execute("BEGIN IMMEDIATE"); }
        try {
            long id = -1; String k = null; String payload = null;
            String sql = key == null || key.isEmpty()
                ? "SELECT id, key, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1"
                : "SELECT id, key, payload FROM queue WHERE status='pending' AND key=? ORDER BY id LIMIT 1";
            try (PreparedStatement ps = h.db.prepareStatement(sql)) {
                if (key != null && !key.isEmpty()) ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) { id = rs.getLong(1); k = rs.getString(2); payload = rs.getString(3); }
                }
            }
            if (id >= 0) {
                try (PreparedStatement ps = h.db.prepareStatement("UPDATE queue SET status='done' WHERE id=?")) {
                    ps.setLong(1, id);
                    ps.executeUpdate();
                }
            }
            try (Statement s = h.db.createStatement()) { s.execute("COMMIT"); }
            return id >= 0 ? new QueueItem(id, k, payload) : null;
        } catch (SQLException e) {
            try (Statement s = h.db.createStatement()) { s.execute("ROLLBACK"); } catch (SQLException ignored) {}
            throw e;
        }
    }

    public static long TI_queue_pending(TiHandle h, String key) throws SQLException {
        String sql = key == null || key.isEmpty()
            ? "SELECT COUNT(*) FROM queue WHERE status='pending'"
            : "SELECT COUNT(*) FROM queue WHERE status='pending' AND key=?";
        try (PreparedStatement ps = h.db.prepareStatement(sql)) {
            if (key != null && !key.isEmpty()) ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    // ------------------------------------------------------------------
    // Lock
    // ------------------------------------------------------------------
    static final long LOCK_STALE_MS = 60_000;

    public static boolean TI_lock_acquire(TiHandle h, String name, int timeoutMs) throws SQLException {
        try (PreparedStatement ps = h.db.prepareStatement(
                "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?, NULL, NULL)")) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
        String holder = "pid-" + ProcessHandle.current().pid();
        long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
        while (true) {
            if (lockTry(h, name, holder)) return true;
            if (timeoutMs <= 0 || System.currentTimeMillis() >= deadline) return false;
            try { Thread.sleep(10); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted", e);
            }
        }
    }

    private static boolean lockTry(TiHandle h, String name, String holder) throws SQLException {
        long now = System.currentTimeMillis();
        long cutoff = now - LOCK_STALE_MS;
        try (Statement s = h.db.createStatement()) { s.execute("BEGIN IMMEDIATE"); }
        try {
            long held = -1;
            try (PreparedStatement ps = h.db.prepareStatement(
                    "SELECT acquired FROM locks WHERE name=? AND acquired IS NOT NULL")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) { if (rs.next()) held = rs.getLong(1); }
            }
            boolean took;
            if (held == -1) {
                try (PreparedStatement ps = h.db.prepareStatement(
                        "UPDATE locks SET holder=?, acquired=? WHERE name=?")) {
                    ps.setString(1, holder); ps.setLong(2, now); ps.setString(3, name);
                    took = ps.executeUpdate() > 0;
                }
            } else if (held <= cutoff) {
                try (PreparedStatement ps = h.db.prepareStatement(
                        "UPDATE locks SET holder=?, acquired=? WHERE name=? AND acquired=?")) {
                    ps.setString(1, holder); ps.setLong(2, now); ps.setString(3, name); ps.setLong(4, held);
                    took = ps.executeUpdate() > 0;
                }
            } else {
                took = false;
            }
            try (Statement s = h.db.createStatement()) { s.execute("COMMIT"); }
            return took;
        } catch (SQLException e) {
            try (Statement s = h.db.createStatement()) { s.execute("ROLLBACK"); } catch (SQLException ignored) {}
            throw e;
        }
    }

    public static void TI_lock_release(TiHandle h, String name) throws SQLException {
        try (PreparedStatement ps = h.db.prepareStatement(
                "UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?")) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
    }

    public interface RunnableThrows { void run() throws Exception; }

    public static void TI_lock_with(TiHandle h, String name, int timeoutMs, RunnableThrows fn) throws Exception {
        if (!TI_lock_acquire(h, name, timeoutMs)) {
            throw new IllegalStateException("could not acquire lock: " + name);
        }
        try {
            fn.run();
        } finally {
            TI_lock_release(h, name);
        }
    }

    // ------------------------------------------------------------------
    // Task API
    // ------------------------------------------------------------------
    public static long TI_task_post(TiHandle h, String key, String payload) throws SQLException {
        return TI_queue_enqueue(h, key, payload);
    }

    /** Returns payload, or null on timeout/empty. */
    public static String TI_task_wait(TiHandle h, String key, int timeoutMs) throws SQLException {
        QueueItem item = TI_queue_dequeue(h, key, timeoutMs);
        return item != null ? item.payload : null;
    }

    private static final AtomicLong SUB_SEQ = new AtomicLong(1);
    private static final ConcurrentHashMap<Long, Thread> SUBS = new ConcurrentHashMap<>();

    /** Subscribe to key; background thread calls callback(payload) per message. Returns tag. */
    public static long TI_task_subscribe(TiHandle h, String key, java.util.function.Consumer<String> callback) {
        long tag = SUB_SEQ.getAndIncrement();
        String dbPath = h.path;
        Thread t = new Thread(() -> {
            try (TiHandle sub = TI_connect(dbPath)) {
                while (!Thread.currentThread().isInterrupted()) {
                    QueueItem item = TI_queue_dequeue(sub, key, 200);
                    if (item != null) callback.accept(item.payload);
                }
            } catch (SQLException e) {
                // subscriber exits on db error
            }
        }, "tianta-sub-" + key + "-" + tag);
        t.setDaemon(true);
        SUBS.put(tag, t);
        t.start();
        return tag;
    }

    public static void TI_task_unsubscribe(TiHandle h, String key, long tag) {
        Thread t = SUBS.remove(tag);
        if (t != null) {
            t.interrupt();
            try { t.join(500); } catch (InterruptedException ignored) {}
        }
    }

    // ------------------------------------------------------------------
    // Logging: rolling log file + error report
    // ------------------------------------------------------------------
    private static volatile String logPath = "";
    private static volatile long logMaxBytes = 5 * 1024 * 1024;
    private static volatile int logBackups = 3;

    public static String TI_log_setup(String app, String dir, long maxBytes, int backups) {
        String d = dir == null || dir.isEmpty()
            ? (System.getenv("TIANTA_PLATFORM_DIR") != null && !System.getenv("TIANTA_PLATFORM_DIR").isEmpty()
                ? System.getenv("TIANTA_PLATFORM_DIR") : DEFAULT_DIR)
            : dir;
        try { Files.createDirectories(Paths.get(d)); } catch (IOException ignored) {}
        if (maxBytes > 0) logMaxBytes = maxBytes;
        if (backups > 0) logBackups = backups;
        logPath = d + "/" + app + ".log";
        return logPath;
    }

    private static synchronized void logRotate() {
        File f = new File(logPath);
        if (!f.exists() || f.length() < logMaxBytes) return;
        for (int i = logBackups; i >= 1; i--) {
            File src = new File(logPath + "." + i);
            File dst = new File(logPath + "." + (i + 1));
            if (src.exists()) src.renameTo(dst);
        }
        f.renameTo(new File(logPath + ".1"));
    }

    private static void logWrite(String level, String msg) {
        if (logPath.isEmpty()) return;
        logRotate();
        String line = "[" + System.currentTimeMillis() + "] [" + level + "] " + msg + "\n";
        try (FileOutputStream fos = new FileOutputStream(logPath, true)) {
            fos.write(line.getBytes());
        } catch (IOException ignored) {}
    }

    public static void TI_log_info(String m) { logWrite("INFO", m); }
    public static void TI_log_warn(String m) { logWrite("WARN", m); }
    public static void TI_log_error(String m) { logWrite("ERROR", m); }
    public static void TI_log_report_error(String m) { logWrite("ERROR", m); }
}
