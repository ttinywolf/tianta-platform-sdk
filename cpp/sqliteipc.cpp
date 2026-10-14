// Cross-process tests for the C++ SQLite queue + lock, plus the worker binary.
//
// Build (single file contains both the worker and the test driver):
//   clang++ -std=c++17 -pthread -DSQLITEIPC_TEST_MAIN sqliteipc.cpp -lsqlite3 -o /tmp/sqliteipc_test
//   SQLITEIPC_BIN=/tmp/sqliteipc_test /tmp/sqliteipc_test
//
// Worker modes (argv):
//   produce <db> <count>
//   consume <db> <outfile>
//   lock    <db> <countfile> <n>
#include "sqliteipc.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

using namespace sqliteipc;

static std::string BinSelf() {
    const char* p = std::getenv("SQLITEIPC_BIN");
    static std::string v = p ? p : "/tmp/sqliteipc_test";
    return v;
}

static void WorkerProduce(const std::string& db, int n) {
    sqlite3* c = Open(db);
    Queue q(c);
    for (int i = 0; i < n; i++) {
        q.Enqueue("produce-" + std::to_string(getpid()) + "-" + std::to_string(i));
    }
    sqlite3_close(c);
}

static void WorkerConsume(const std::string& db, const std::string& out) {
    sqlite3* c = Open(db);
    Queue q(c);
    std::ofstream f(out, std::ios::app);
    while (Queue::Item* item = q.Dequeue()) {
        f << item->payload << "\n";
        delete item;
    }
    sqlite3_close(c);
}

static void WorkerLock(const std::string& db, const std::string& countfile, int n) {
    sqlite3* c = Open(db);
    Lock lk(c, "test_lock");
    for (int i = 0; i < n; i++) {
        lk.WithLock(10000, [&]() {
            int v = 0;
            std::ifstream in(countfile);
            if (in) in >> v;
            std::ofstream out(countfile, std::ios::trunc);
            out << (v + 1);
        });
    }
    sqlite3_close(c);
}

// locknamed <db> <lockname> <countfile> <n>  -- used by cross-language test
static void WorkerLockNamed(const std::string& db, const std::string& lockname,
                            const std::string& countfile, int n) {
    sqlite3* c = Open(db);
    Lock lk(c, lockname);
    for (int i = 0; i < n; i++) {
        lk.WithLock(10000, [&]() {
            int v = 0;
            std::ifstream in(countfile);
            if (in) in >> v;
            std::ofstream out(countfile, std::ios::trunc);
            out << (v + 1);
        });
    }
    sqlite3_close(c);
}

static int RunWorker(int argc, char** argv) {
    if (argc < 3) return 2;
    std::string mode = argv[1];
    std::string db = argv[2];
    if (mode == "produce") WorkerProduce(db, std::atoi(argv[3]));
    else if (mode == "consume") WorkerConsume(db, argv[3]);
    else if (mode == "lock") WorkerLock(db, argv[3], std::atoi(argv[4]));
    else if (mode == "locknamed") WorkerLockNamed(db, argv[3], argv[4], std::atoi(argv[5]));
    else return 2;
    return 0;
}

// ---- Test driver ----

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
    std::string d = "/tmp/sqipc-" + std::string(tag) + "-" + std::to_string(getpid());
    std::string cmd = "rm -rf " + d + " && mkdir -p " + d;
    system(cmd.c_str());
    db = d + "/test.db";
    return d;
}

static bool TestQueue() {
    std::string db, dir = TempDir("queue", db);
    { sqlite3* c = Open(db); sqlite3_close(c); }
    std::string out = dir + "/out.txt";

    int nProducers = 4, nPer = 50;
    std::vector<pid_t> kids;
    for (int i = 0; i < nProducers; i++)
        kids.push_back(Spawn({"produce", db, std::to_string(nPer)}));
    if (!WaitAll(kids)) { printf("FAIL producer\n"); return false; }

    { sqlite3* c = Open(db); Queue q(c);
      int64_t pending = q.PendingCount();
      sqlite3_close(c);
      if (pending != nProducers * nPer) { printf("FAIL pending=%lld\n", (long long)pending); return false; }
    }

    kids.clear();
    for (int i = 0; i < 3; i++) kids.push_back(Spawn({"consume", db, out}));
    if (!WaitAll(kids)) { printf("FAIL consumer\n"); return false; }

    std::ifstream f(out);
    std::string line;
    std::vector<std::string> items;
    while (std::getline(f, line)) if (!line.empty()) items.push_back(line);
    int total = nProducers * nPer;
    if ((int)items.size() != total) { printf("FAIL consumed=%zu want=%d\n", items.size(), total); return false; }
    std::sort(items.begin(), items.end());
    for (size_t i = 1; i < items.size(); i++)
        if (items[i] == items[i-1]) { printf("FAIL duplicate\n"); return false; }
    printf("queue ok: %d items, no duplicates\n", total);
    return true;
}

static bool TestLock() {
    std::string db, dir = TempDir("lock", db);
    { sqlite3* c = Open(db); sqlite3_close(c); }
    std::string countfile = dir + "/count.txt";
    { std::ofstream f(countfile); f << "0"; }

    int nProcs = 4, increments = 200;
    std::vector<pid_t> kids;
    for (int i = 0; i < nProcs; i++)
        kids.push_back(Spawn({"lock", db, countfile, std::to_string(increments)}));
    if (!WaitAll(kids)) { printf("FAIL lock worker\n"); return false; }

    int v = 0;
    { std::ifstream f(countfile); f >> v; }
    int expected = nProcs * increments;
    if (v != expected) { printf("FAIL counter=%d want=%d (lost updates)\n", v, expected); return false; }
    printf("lock ok: counter=%d == %d\n", v, expected);
    return true;
}

#ifndef SQLITEIPC_TEST_MAIN
int main(int argc, char** argv) {
    if (argc > 1) return RunWorker(argc, argv);
    bool ok = TestQueue();
    ok = TestLock() && ok;
    printf(ok ? "\nALL C++ TESTS PASSED\n" : "\nSOME TESTS FAILED\n");
    return ok ? 0 : 1;
}
#endif
