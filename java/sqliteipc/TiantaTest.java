package sqliteipc;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Tests for the Java TIANTA layer + task objects.
 *
 * Build/run with sqlite-jdbc on the classpath:
 *   javac -cp /tmp/sqlite-jdbc.jar sqliteipc/*.java
 *   java -cp .:/tmp/sqlite-jdbc.jar sqliteipc.TiantaTest
 *
 * Spawns real OS processes for multi-process queue/lock verification.
 */
public final class TiantaTest {
    private static final String CP = System.getProperty("java.class.path");
    private static final String JH = System.getProperty("java.home");

    public static void main(String[] args) throws Exception {
        boolean ok = testQueueMultiprocess();
        ok &= testLockMultiprocess();
        ok &= testTasksAndExt();
        ok &= testLogging();
        System.out.println(ok ? "\nALL JAVA TIANTA TESTS PASSED" : "\nSOME TESTS FAILED");
        if (!ok) System.exit(1);
    }

    static boolean testQueueMultiprocess() throws Exception {
        Path dir = Files.createTempDirectory("tianta-j-queue");
        String db = dir.resolve("platform.dat").toString();
        String out = dir.resolve("out.txt").toString();
        try (TiHandle h = Tianta.TI_connect(db)) {}

        int nProducers = 4, nPer = 50;
        List<Process> kids = new ArrayList<>();
        for (int i = 0; i < nProducers; i++) kids.add(spawn("produce", db, String.valueOf(nPer)));
        waitAll(kids);

        try (TiHandle h = Tianta.TI_connect(db)) {
            long pending = Tianta.TI_queue_pending(h, "data");
            if (pending != nProducers * nPer) { System.out.println("FAIL pending=" + pending); return false; }
        }
        kids = new ArrayList<>();
        for (int i = 0; i < 3; i++) kids.add(spawn("consume", db, out, "data"));
        waitAll(kids);

        List<String> items = Files.readAllLines(Path.of(out));
        items.removeIf(String::isBlank);
        int total = nProducers * nPer;
        if (items.size() != total) { System.out.println("FAIL consumed=" + items.size()); return false; }
        Set<String> uniq = new HashSet<>(items);
        if (uniq.size() != total) { System.out.println("FAIL duplicates"); return false; }
        System.out.println("queue ok: " + total + " items, no dupes");
        deleteTree(dir);
        return true;
    }

    static boolean testLockMultiprocess() throws Exception {
        Path dir = Files.createTempDirectory("tianta-j-lock");
        String db = dir.resolve("platform.dat").toString();
        String cf = dir.resolve("count.txt").toString();
        Files.writeString(Path.of(cf), "0");
        try (TiHandle h = Tianta.TI_connect(db)) {}

        int nProcs = 4, inc = 200;
        List<Process> kids = new ArrayList<>();
        for (int i = 0; i < nProcs; i++) kids.add(spawn("lock", db, "test_lock", cf, String.valueOf(inc)));
        waitAll(kids);

        int v = Integer.parseInt(Files.readString(Path.of(cf)).trim());
        int expected = nProcs * inc;
        if (v != expected) { System.out.println("FAIL counter=" + v + " want " + expected); return false; }
        System.out.println("lock ok: counter=" + v + " == " + expected);
        deleteTree(dir);
        return true;
    }

    static boolean testTasksAndExt() throws Exception {
        Path dir = Files.createTempDirectory("tianta-j-task");
        String db = dir.resolve("platform.dat").toString();
        try (TiHandle h = Tianta.TI_connect(db)) {
            // post + wait with extension fields
            TiantaTasks.TmRecordDone.post(h, TiantaTasks.m(
                TiantaTasks.kv("file_path", "/data/tm.bin"),
                TiantaTasks.kv("size", 99L),
                TiantaTasks.kv("mission_id", "MISS-2026"),
                TiantaTasks.kv("freq", 145.8)));
            TiantaTasks.TaskEvent ev = TiantaTasks.TmRecordDone.wait(h, 2000);
            if (ev == null) { System.out.println("FAIL tm wait"); return false; }
            if (!"/data/tm.bin".equals(ev.getString("file_path")) || ev.getLong("size") != 99) {
                System.out.println("FAIL tm fields"); return false;
            }
            Map<String, Object> md = ev.metadata();
            if (!"MISS-2026".equals(md.get("mission_id"))) { System.out.println("FAIL metadata"); return false; }
            if (md.containsKey("file_path")) { System.out.println("FAIL predefined leaked"); return false; }

            // subscribe push
            CountDownLatch latch = new CountDownLatch(1);
            List<String> got = new ArrayList<>();
            long tag = TiantaTasks.DownloadReq.on(h, e -> { got.add(e.getString("req_id")); latch.countDown(); });
            Thread.sleep(400);
            TiantaTasks.DownloadReq.post(h, TiantaTasks.m(TiantaTasks.kv("req_id", "r1")));
            if (!latch.await(3, java.util.concurrent.TimeUnit.SECONDS)) {
                System.out.println("FAIL subscribe timeout"); return false;
            }
            TiantaTasks.DownloadReq.off(h, tag);
        }
        System.out.println("task post/wait/subscribe + extension fields OK");
        deleteTree(dir);
        return true;
    }

    static boolean testLogging() throws Exception {
        Path dir = Files.createTempDirectory("tianta-j-log");
        String p = Tianta.TI_log_setup("platform", dir.toString(), 300, 2);
        for (int i = 0; i < 80; i++) { Tianta.TI_log_info("row"); Tianta.TI_log_error("boom"); }
        if (!Files.exists(Path.of(p))) { System.out.println("FAIL log"); return false; }
        if (!Files.exists(Path.of(p + ".1"))) { System.out.println("FAIL no rotation"); return false; }
        System.out.println("logging rolling OK");
        deleteTree(dir);
        return true;
    }

    // --- worker ---
    static int runWorker(String[] args) throws Exception {
        String mode = args[0];
        String db = args[1];
        switch (mode) {
            case "produce": {
                int n = Integer.parseInt(args[2]);
                try (TiHandle h = Tianta.TI_connect(db)) {
                    long pid = ProcessHandle.current().pid();
                    for (int i = 0; i < n; i++) Tianta.TI_queue_enqueue(h, "data", "produce-" + pid + "-" + i);
                }
                return 0;
            }
            case "consume": {
                String out = args[2];
                String key = args.length > 3 ? args[3] : "";
                try (TiHandle h = Tianta.TI_connect(db)) {
                    try (FileOutputStream fos = new FileOutputStream(out, true)) {
                        Tianta.QueueItem item;
                        while ((item = Tianta.TI_queue_dequeue(h, key, 200)) != null) {
                            fos.write((item.payload + "\n").getBytes());
                        }
                    }
                }
                return 0;
            }
            case "lock": {
                String name = args[2];
                String cf = args[3];
                int n = Integer.parseInt(args[4]);
                try (TiHandle h = Tianta.TI_connect(db)) {
                    for (int i = 0; i < n; i++) {
                        Tianta.TI_lock_with(h, name, 10000, () -> {
                            int v = Files.exists(Path.of(cf)) ? Integer.parseInt(Files.readString(Path.of(cf)).trim()) : 0;
                            Files.writeString(Path.of(cf), String.valueOf(v + 1));
                        });
                    }
                }
                return 0;
            }
            default:
                return 2;
        }
    }

    static Process spawn(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(JH + "/bin/java");
        cmd.add("-cp");
        cmd.add(CP);
        cmd.add("sqliteipc.TiantaTestWorker");
        for (String a : args) cmd.add(a);
        return new ProcessBuilder(cmd)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .start();
    }

    static void waitAll(List<Process> kids) throws InterruptedException {
        for (Process p : kids) if (p.waitFor() != 0) throw new IllegalStateException("worker failed");
    }

    static void deleteTree(Path root) throws Exception {
        if (root == null || !Files.exists(root)) return;
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
        }
    }
}
