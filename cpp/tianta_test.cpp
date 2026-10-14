// TIANTA layer C++ tests (single binary = worker + driver).
//
// Build & run:
//   clang++ -std=c++17 -pthread tianta_test.cpp -lsqlite3 -o /tmp/tianta_cpp
//   TIANTA_CPP=/tmp/tianta_cpp /tmp/tianta_cpp
//
// Worker modes (used via fork/exec by the driver):
//   produce <db> <count>
//   consume <db> <outfile> <key>
//   lock    <db> <name> <countfile> <n>
#include "tianta.hpp"
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

using namespace tianta;

static std::string BinSelf() {
    const char* p = std::getenv("TIANTA_CPP");
    static std::string v = p ? p : "/tmp/tianta_cpp";
    return v;
}

static Json make_fields(std::initializer_list<std::pair<std::string, std::string>> il) {
    Json j;
    for (auto& kv : il) j[kv.first] = kv.second;
    return j;
}

// --- worker modes -----------------------------------------------------------
static int WorkerProduce(const std::string& db, int n) {
    TiHandle* h = TI_connect(db);
    for (int i = 0; i < n; i++) {
        TI_queue_enqueue(h, "data", "produce-" + std::to_string(::getpid()) + "-" + std::to_string(i));
    }
    delete h;
    return 0;
}

static int WorkerConsume(const std::string& db, const std::string& out, const std::string& key) {
    TiHandle* h = TI_connect(db);
    std::ofstream f(out, std::ios::app);
    QueueItem item;
    while (TI_queue_dequeue(h, key, 200, item)) {
        f << item.payload << "\n";
    }
    delete h;
    return 0;
}

static int WorkerLock(const std::string& db, const std::string& name, const std::string& cf, int n) {
    TiHandle* h = TI_connect(db);
    for (int i = 0; i < n; i++) {
        TI_lock_with(h, name, 10000, [&]() {
            int v = 0;
            std::ifstream in(cf);
            if (in) in >> v;
            std::ofstream o(cf, std::ios::trunc);
            o << (v + 1);
        });
    }
    delete h;
    return 0;
}

static int RunWorker(int argc, char** argv) {
    if (argc < 3) return 2;
    std::string mode = argv[1];
    std::string db = argv[2];
    if (mode == "produce") return WorkerProduce(db, std::atoi(argv[3]));
    if (mode == "consume") return WorkerConsume(db, argv[3], argc > 4 ? argv[4] : "");
    if (mode == "lock") return WorkerLock(db, argv[3], argv[4], std::atoi(argv[5]));
    return 2;
}

// --- driver -----------------------------------------------------------------
static pid_t Spawn(const std::vector<std::string>& args) {
    pid_t pid = fork();
    if (pid == 0) {
        std::vector<char*> av;
        av.push_back(const_cast<char*>(BinSelf().c_str()));
        for (auto& a : args) av.push_back(const_cast<char*>(a.c_str()));
        av.push_back(nullptr);
        execv(av[0], av.data());
        _exit(127);
    }
    return pid;
}

static bool WaitAll(std::vector<pid_t>& kids) {
    bool ok = true;
    for (pid_t k : kids) {
        int st = 0;
        waitpid(k, &st, 0);
        if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) ok = false;
    }
    return ok;
}

static std::string TempDir(const char* tag, std::string& db) {
    std::string d = "/tmp/tiantacpp-" + std::string(tag) + "-" + std::to_string(::getpid());
    system(("rm -rf " + d + " && mkdir -p " + d).c_str());
    db = d + "/platform.dat";
    return d;
}

// subscribe callback accumulator
static std::vector<std::string> g_received;
static void OnDl(const Json& ev) { g_received.push_back(ev.at("req_id")); }

static void TestQueue() {
    std::string db, dir = TempDir("queue", db);
    { TiHandle* h = TI_connect(db); delete h; }
    std::string out = dir + "/out.txt";

    int nProducers = 4, nPer = 50;
    std::vector<pid_t> kids;
    for (int i = 0; i < nProducers; i++)
        kids.push_back(Spawn({"produce", db, std::to_string(nPer)}));
    if (!WaitAll(kids)) { printf("FAIL producer\n"); return; }

    { TiHandle* h = TI_connect(db);
      if (TI_queue_pending(h, "data") != nProducers * nPer) { printf("FAIL pending\n"); delete h; return; }
      delete h; }

    kids.clear();
    for (int i = 0; i < 3; i++) kids.push_back(Spawn({"consume", db, out, "data"}));
    if (!WaitAll(kids)) { printf("FAIL consumer\n"); return; }

    std::vector<std::string> items;
    std::ifstream f(out);
    std::string line;
    while (std::getline(f, line)) if (!line.empty()) items.push_back(line);
    std::sort(items.begin(), items.end());
    if ((int)items.size() != nProducers * nPer) { printf("FAIL consumed=%zu\n", items.size()); return; }
    for (size_t i = 1; i < items.size(); i++)
        if (items[i] == items[i-1]) { printf("FAIL duplicate\n"); return; }
    printf("queue ok: %d items no dupes\n", nProducers * nPer);
}

static void TestLock() {
    std::string db, dir = TempDir("lock", db);
    { TiHandle* h = TI_connect(db); delete h; }
    std::string cf = dir + "/count.txt";
    { std::ofstream f(cf); f << "0"; }

    int nProcs = 4, inc = 200;
    std::vector<pid_t> kids;
    for (int i = 0; i < nProcs; i++)
        kids.push_back(Spawn({"lock", db, "test_lock", cf, std::to_string(inc)}));
    if (!WaitAll(kids)) { printf("FAIL lock worker\n"); return; }
    int v = 0;
    { std::ifstream f(cf); f >> v; }
    if (v != nProcs * inc) { printf("FAIL counter=%d want %d\n", v, nProcs * inc); return; }
    printf("lock ok: counter=%d == %d\n", v, nProcs * inc);
}

static void TestTasksAndExt() {
    std::string db, dir = TempDir("task", db);
    TiHandle* h = TI_connect(db);
    // post + wait via task objects (JSON map)
    int64_t id = TI_task_post(h, TI_KEY_TM_DONE, TI_json_encode(make_fields({
        {"file_path", "/data/tm.bin"}, {"size", "99"}, {"mission_id", "MISS-2026"}
    })));
    (void)id;
    std::string raw;
    if (!TI_task_wait(h, TI_KEY_TM_DONE, 1000, raw)) { printf("FAIL wait tm\n"); delete h; return; }
    Json ev = TI_json_decode(raw);
    if (ev["file_path"] != "/data/tm.bin" || ev["mission_id"] != "MISS-2026") {
        printf("FAIL tm fields: %s\n", raw.c_str()); delete h; return;
    }
    // subscribe
    uint64_t tag = TI_task_subscribe(h, TI_KEY_DL_REQ, OnDl);
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    TI_task_post(h, TI_KEY_DL_REQ, TI_json_encode(make_fields({{"req_id", "r1"}})));
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    TI_task_unsubscribe(h, TI_KEY_DL_REQ, tag);
    bool found = false;
    for (auto& r : g_received) if (r == "r1") found = true;
    if (!found) { printf("FAIL subscribe dl\n"); delete h; return; }
    printf("task post/wait/subscribe + extension fields OK\n");
    delete h;
}

static void TestLog() {
    std::string d = "/tmp/tiantacpp-log-" + std::to_string(::getpid());
    system(("rm -rf " + d).c_str());
    std::string p = TI_log_setup("platform", d, 300, 2);
    for (int i = 0; i < 80; i++) { TI_log_info("row"); TI_log_error("boom critical"); }
    std::ifstream f(p);
    if (!f.good()) { printf("FAIL log\n"); return; }
    std::ifstream f1(p + ".1");
    if (!f1.good()) { printf("FAIL no rotation\n"); return; }
    printf("logging rolling OK\n");
}

int main(int argc, char** argv) {
    if (argc > 1) return RunWorker(argc, argv);
    TestQueue();
    TestLock();
    TestTasksAndExt();
    TestLog();
    printf("\nALL C++ TIANTA TESTS PASSED\n");
    return 0;
}
