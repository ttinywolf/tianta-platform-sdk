package sqliteipc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Per-process database handle used by all Tianta functions. AutoCloseable so it
 * can be used in try-with-resources.
 */
public class TiHandle implements AutoCloseable {
    final Connection db;
    final String path;

    TiHandle(Connection db, String path) {
        this.db = db;
        this.path = path;
    }

    @Override
    public void close() throws SQLException {
        if (db != null && !db.isClosed()) db.close();
    }
}
