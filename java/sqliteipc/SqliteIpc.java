package sqliteipc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Cross-process message queue and mutex lock backed by SQLite.
 *
 * Shares the same schema and semantics as the other language ports (python/,
 * go/, rust/, cpp/) so processes in different languages can operate on the
 * same database file.
 *
 * Design: WAL mode + busy_timeout (via PRAGMA) for write concurrency;
 * {@code BEGIN IMMEDIATE} transactions for atomic dequeue and lock acquisition.
 *
 * Requires the Xerial sqlite-jdbc driver on the classpath, e.g. Maven:
 *   <dependency>
 *     <groupId>org.xerial</groupId>
 *     <artifactId>sqlite-jdbc</artifactId>
 *     <version>3.45.1.0</version>
 *   </dependency>
 */
public final class SqliteIpc {

    private static final String SCHEMA =
        "CREATE TABLE IF NOT EXISTS queue (" +
        "  id         INTEGER PRIMARY KEY AUTOINCREMENT," +
        "  payload    TEXT    NOT NULL," +
        "  status     TEXT    NOT NULL DEFAULT 'pending'," +
        "  created_at INTEGER NOT NULL" +
        ");" +
        "CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status, id);" +
        "CREATE TABLE IF NOT EXISTS locks (" +
        "  name     TEXT PRIMARY KEY," +
        "  holder   TEXT," +
        "  acquired INTEGER" +
        ");";

    private SqliteIpc() {}

    /** Opens (creating if needed) the database, applies the shared schema. */
    public static Connection open(String path) throws SQLException {
        String url = "jdbc:sqlite:" + path;
        Connection c = DriverManager.getConnection(url);
        try (Statement st = c.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA busy_timeout=30000");
            st.execute("PRAGMA synchronous=NORMAL");
            st.execute(SCHEMA);
        }
        return c;
    }

    /** A minimal FIFO message queue (not thread-safe; one queue per thread). */
    public static final class Queue {
        private final Connection conn;

        public Queue(Connection conn) { this.conn = conn; }

        public long enqueue(String payload) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO queue (payload, created_at) VALUES (?, ?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, payload);
                ps.setLong(2, System.currentTimeMillis());
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    return rs.next() ? rs.getLong(1) : -1;
                }
            }
        }

        /**
         * Claims and returns the oldest pending item as [id, payload], or null
         * if the queue is empty. Atomically marked done under an IMMEDIATE
         * transaction so exactly one consumer gets each item.
         */
        public QueueItem dequeue() throws SQLException {
            // autocommit is off for IMMEDIATE; use manual transaction
            conn.setAutoCommit(false);
            try {
                try (Statement st = conn.createStatement()) {
                    st.execute("BEGIN IMMEDIATE");
                }
                long id = -1;
                String payload = null;
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id, payload FROM queue WHERE status='pending' ORDER BY id LIMIT 1");
                     ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                        payload = rs.getString(2);
                    }
                }
                if (id >= 0) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE queue SET status='done' WHERE id=?")) {
                        ps.setLong(1, id);
                        ps.executeUpdate();
                    }
                }
                conn.commit();
                return id >= 0 ? new QueueItem(id, payload) : null;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }

        public long pendingCount() throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM queue WHERE status='pending'")) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    /** Result of {@link Queue#dequeue()}. */
    public static final class QueueItem {
        public final long id;
        public final String payload;
        QueueItem(long id, String payload) { this.id = id; this.payload = payload; }
    }

    /** A named cross-process mutex. */
    public static final class Lock {
        private static final long DEFAULT_STALE_AFTER_MS = 60_000;
        private final Connection conn;
        private final String name;

        public Lock(Connection conn, String name) throws SQLException {
            // Ensure the lock row exists so UPDATE-based acquisition is effective.
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR IGNORE INTO locks (name, holder, acquired) VALUES (?, NULL, NULL)")) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
            this.conn = conn;
            this.name = name;
        }

        /** Acquires the lock, retrying up to timeoutMs (0 = try once). */
        public boolean acquire(long timeoutMs) throws SQLException {
            long deadline = timeoutMs > 0
                ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
            String holder = "pid-" + ProcessHandle.current().pid();
            while (true) {
                if (tryAcquire(holder)) return true;
                if (timeoutMs == 0 || System.currentTimeMillis() >= deadline) return false;
                try { Thread.sleep(10); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("interrupted", ie);
                }
            }
        }

        private boolean tryAcquire(String holder) throws SQLException {
            long now = System.currentTimeMillis();
            long cutoff = now - DEFAULT_STALE_AFTER_MS;
            conn.setAutoCommit(false);
            try {
                try (Statement st = conn.createStatement()) {
                    st.execute("BEGIN IMMEDIATE");
                }
                long held = -1; // -1 == not held
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT acquired FROM locks WHERE name=? AND acquired IS NOT NULL")) {
                    ps.setString(1, name);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) held = rs.getLong(1);
                    }
                }
                boolean took;
                if (held == -1) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE locks SET holder=?, acquired=? WHERE name=?")) {
                        ps.setString(1, holder);
                        ps.setLong(2, now);
                        ps.setString(3, name);
                        took = ps.executeUpdate() > 0;
                    }
                } else if (held <= cutoff) {
                    // stale lock from a crashed process -> steal it
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE locks SET holder=?, acquired=? WHERE name=? AND acquired=?")) {
                        ps.setString(1, holder);
                        ps.setLong(2, now);
                        ps.setString(3, name);
                        ps.setLong(4, held);
                        took = ps.executeUpdate() > 0;
                    }
                } else {
                    took = false;
                }
                conn.commit();
                return took;
            } catch (SQLException e) {
                try { conn.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }

        public void release() throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE locks SET holder=NULL, acquired=NULL WHERE name=?")) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
        }

        public interface Task { void run() throws Exception; }

        /** Runs task while holding the lock, blocking up to timeoutMs. */
        public void withLock(long timeoutMs, Task task) throws Exception {
            if (!acquire(timeoutMs)) {
                throw new IllegalStateException("could not acquire lock: " + name);
            }
            try {
                task.run();
            } finally {
                release();
            }
        }
    }
}
