package sqliteipc;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Cross-process tests. Build/run with sqlite-jdbc on the classpath.
 *
 *   javac -cp sqlite-jdbc.jar sqliteipc/*.java
 *   java -cp .:sqlite-jdbc.jar sqliteipc.TestMain
 *
 * The test spawns real OS processes via {@link #spawnWorker}.
 */
public final class TestMain {
    private static final String CLASS_PATH = System.getProperty("java.class.path");
    private static final String JAVA_HOME = System.getProperty("java.home");

    public static void main(String[] args) throws Exception {
        boolean ok = true;
        ok &= queueMultiprocess();
        ok &= lockMultiprocess();
        System.out.println(ok ? "\nALL JAVA TESTS PASSED" : "\nSOME TESTS FAILED");
        if (!ok) System.exit(1);
    }

    static boolean queueMultiprocess() throws Exception {
        Path dir = Files.createTempDirectory("sqipc-queue");
        String db = dir.resolve("test.db").toString();
        String out = dir.resolve("out.txt").toString();
        try (Connection c = SqliteIpc.open(db)) { /* init schema */ }

        int nProducers = 4, nPer = 50;
        List<Process> kids = new ArrayList<>();
        for (int i = 0; i < nProducers; i++) {
            kids.add(spawnWorker("produce", db, Integer.toString(nPer)));
        }
        waitAll(kids);

        try (Connection c = SqliteIpc.open(db)) {
            SqliteIpc.Queue q = new SqliteIpc.Queue(c);
            long pending = q.pendingCount();
            if (pending != nProducers * nPer) {
                System.out.println("FAIL pending=" + pending);
                return false;
            }
        }

        List<Process> consumers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            consumers.add(spawnWorker("consume", db, out));
        }
        waitAll(consumers);

        List<String> items = Files.readAllLines(Path.of(out));
        items.removeIf(String::isBlank);
        int total = nProducers * nPer;
        if (items.size() != total) {
            System.out.println("FAIL consumed=" + items.size() + " want=" + total);
            return false;
        }
        Set<String> unique = new HashSet<>(items);
        if (unique.size() != total) {
            System.out.println("FAIL duplicate consumption");
            return false;
        }
        System.out.println("queue ok: " + total + " items, no duplicates");
        deleteTree(dir);
        return true;
    }

    static boolean lockMultiprocess() throws Exception {
        Path dir = Files.createTempDirectory("sqipc-lock");
        String db = dir.resolve("test.db").toString();
        String count = dir.resolve("count.txt").toString();
        Files.writeString(Path.of(count), "0");
        try (Connection c = SqliteIpc.open(db)) { /* init */ }

        int nProcs = 4, increments = 200;
        List<Process> kids = new ArrayList<>();
        for (int i = 0; i < nProcs; i++) {
            kids.add(spawnWorker("lock", db, count, Integer.toString(increments)));
        }
        waitAll(kids);

        int v = Integer.parseInt(Files.readString(Path.of(count)).trim());
        int expected = nProcs * increments;
        if (v != expected) {
            System.out.println("FAIL counter=" + v + " want=" + expected + " (lost updates)");
            return false;
        }
        System.out.println("lock ok: counter=" + v + " == " + expected);
        deleteTree(dir);
        return true;
    }

    static void deleteTree(Path root) throws IOException {
        if (root == null || !Files.exists(root)) return;
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            });
        }
    }

    static void waitAll(List<Process> kids) throws InterruptedException {
        for (Process p : kids) {
            int rc = p.waitFor();
            if (rc != 0) throw new IllegalStateException("worker exit=" + rc);
        }
    }

    static Process spawnWorker(String... args) throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(JAVA_HOME + "/bin/java");
        cmd.add("-cp");
        cmd.add(CLASS_PATH);
        cmd.add("sqliteipc.Worker");
        for (String a : args) cmd.add(a);
        return new ProcessBuilder(cmd)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    }
}
