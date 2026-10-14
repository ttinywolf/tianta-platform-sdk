package sqliteipc;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

/**
 * Worker process used by {@link TestMain}. Mirrors the Rust worker.
 * Modes:
 *   produce <db> <count>           append `count` messages with this pid prefix
 *   consume <db> <outfile>         dequeue until empty, append to outfile
 *   lock    <db> <countfile> <n>   increment a file counter `n` times under the lock
 */
public final class Worker {
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        String db = args[1];
        switch (mode) {
            case "produce": {
                int n = Integer.parseInt(args[2]);
                try (Connection c = SqliteIpc.open(db)) {
                    SqliteIpc.Queue q = new SqliteIpc.Queue(c);
                    long pid = ProcessHandle.current().pid();
                    for (int i = 0; i < n; i++) {
                        q.enqueue("produce-" + pid + "-" + i);
                    }
                }
                break;
            }
            case "consume": {
                String out = args[2];
                try (Connection c = SqliteIpc.open(db)) {
                    SqliteIpc.Queue q = new SqliteIpc.Queue(c);
                    try (FileWriter fw = new FileWriter(out, true)) {
                        SqliteIpc.QueueItem item;
                        while ((item = q.dequeue()) != null) {
                            fw.write(item.payload + "\n");
                        }
                    }
                }
                break;
            }
            case "lock": {
                String countfile = args[2];
                int n = Integer.parseInt(args[3]);
                try (Connection c = SqliteIpc.open(db)) {
                    SqliteIpc.Lock lk = new SqliteIpc.Lock(c, "test_lock");
                    for (int i = 0; i < n; i++) {
                        lk.withLock(10_000, () -> {
                            int v = Files.exists(Path.of(countfile))
                                ? Integer.parseInt(Files.readString(Path.of(countfile)).trim())
                                : 0;
                            Files.writeString(Path.of(countfile), Integer.toString(v + 1));
                        });
                    }
                }
                break;
            }
            default:
                throw new IllegalArgumentException("unknown mode: " + mode);
        }
    }
}
