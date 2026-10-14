package sqliteipc;

/** Worker entrypoint spawned by TiantaTest. Delegates to TiantaTest.runWorker. */
public final class TiantaTestWorker {
    public static void main(String[] args) throws Exception {
        int rc = TiantaTest.runWorker(args);
        System.exit(rc);
    }
}
