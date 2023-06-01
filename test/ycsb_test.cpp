#include "Tree.h"
#include "Timer.h"
#include <city.h>

#include <stdlib.h>
#include <thread>
#include <time.h>
#include <vector>
#include <iostream>
#include <string>
#include <fstream>
#include <random>

#ifdef LONG_TEST_EPOCH
  #define TEST_EPOCH 40
  #define TIME_INTERVAL 1
#else
#ifdef SHORT_TEST_EPOCH
  #define TEST_EPOCH 5
  #define TIME_INTERVAL 0.2
#else
#ifdef MIDDLE_TEST_EPOCH
  #define TEST_EPOCH 10
  #define TIME_INTERVAL 1
#else
  #define TEST_EPOCH 10
  #define TIME_INTERVAL 0.5
#endif
#endif
#endif

#define MAX_THREAD_REQUEST 10000000
#define LOAD_HEARTBEAT 100000
#define USE_CORO
#define EPOCH_LAT_TEST
#define LOADER_NUM 8

extern double cache_miss[MAX_APP_THREAD];
extern double cache_hit[MAX_APP_THREAD];
extern uint64_t lock_fail[MAX_APP_THREAD];
// extern uint64_t try_lock[MAX_APP_THREAD];
extern uint64_t write_handover_num[MAX_APP_THREAD];
extern uint64_t try_write_op[MAX_APP_THREAD];
extern uint64_t read_handover_num[MAX_APP_THREAD];
extern uint64_t try_read_op[MAX_APP_THREAD];
extern uint64_t read_leaf_retry[MAX_APP_THREAD];
extern uint64_t leaf_cache_invalid[MAX_APP_THREAD];
extern uint64_t try_read_leaf[MAX_APP_THREAD];
extern uint64_t read_node_repair[MAX_APP_THREAD];
extern uint64_t try_read_node[MAX_APP_THREAD];
extern uint64_t read_node_type[MAX_APP_THREAD][MAX_NODE_TYPE_NUM];
extern uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];

int kThreadCount;
int kNodeCount;
int kCoroCnt = 8;
bool kIsStr;
bool kIsScan;
#ifdef USE_CORO
bool kUseCoro = true;
#else
bool kUseCoro = false;
#endif

std::string ycsb_load_path;
std::string ycsb_trans_path;
int fix_range_size = -1;
// turn it on if want to eliminate impact of write conflicts
bool rm_write_conflict = false;


std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][MAX_CORO_NUM];

extern volatile bool need_stop;
extern uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

std::default_random_engine e;
std::uniform_int_distribution<Value> randval(kValueMin, kValueMax);

Tree *tree;
DSM *dsm;


inline uint64_t key_hash(const Key &k) {
  return CityHash64((char *)&k, sizeof(k));
}


class RequsetGenBench : public RequstGen {
public:
  RequsetGenBench(DSM* dsm, Request* req, int req_num, int coro_id, int coro_cnt) :
                  dsm(dsm), req(req), req_num(req_num), coro_id(coro_id), coro_cnt(coro_cnt) {
    local_thread_id = dsm->getMyThreadID();
    cur = coro_id;
    epoch_id = 0;
    extra_k = MAX_KEY_SPACE_SIZE + kThreadCount * kCoroCnt * dsm->getMyNodeID() + local_thread_id * kCoroCnt + coro_id;
    flag = false;
  }

  Request next() override {
    cur = (cur + coro_cnt) % req_num;
    if (req[cur].is_insert) {
      if (cur + coro_cnt >= req_num) {
        // need_stop = true;
        ++ epoch_id;
        flag = true;
      }
      if (kIsStr) {
        req[cur].k = req[cur].k + epoch_id;   // For insert workloads, key should remain nonexist
      }
      else if (flag) {
        req[cur].k = int2key(extra_k);
        extra_k += kThreadCount * kCoroCnt * dsm->getClusterSize();
      }
    }
    tp[local_thread_id][coro_id]++;
    req[cur].v = randval(e);  // make value different per-epoch
    return req[cur];
  }

private:
  DSM *dsm;
  Request* req;
  int req_num;
  int coro_id;
  int coro_cnt;
  int local_thread_id;
  int cur;
  uint8_t epoch_id;
  uint64_t extra_k;
  bool flag;
};


RequstGen *gen_func(DSM* dsm, Request* req, int req_num, int coro_id, int coro_cnt) {
  return new RequsetGenBench(dsm, req, req_num, coro_id, coro_cnt);
}


void work_func(Tree *tree, const Request& r, CoroContext *ctx, int coro_id) {
  if (r.is_search) {
    Value v;
    tree->search(r.k, v, ctx, coro_id);
  }
  else if (r.is_update || r.is_insert) {
    tree->insert(r.k, r.v, ctx, coro_id, r.is_update);
  }
  else {
    std::map<Key, Value> ret;
    tree->range_query(r.k, r.k + r.range_size, ret);
  }
}


Timer bench_timer;
std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};


void thread_load(int id) {
  // use LOADER_NUM threads to load ycsb
  uint64_t loader_id = std::min(kThreadCount, LOADER_NUM) * dsm->getMyNodeID() + id;

  printf("I am loader %lu\n", loader_id);

  // 1. insert ycsb_load
  std::string op;
  std::ifstream load_in(ycsb_load_path + std::to_string(loader_id));
  if (!load_in.is_open()) {
    printf("Error opening load file\n");
    assert(false);
  }
  Key k;
  int cnt = 0;
  if (!kIsStr) {  // int workloads
    uint64_t int_k;
    while (load_in >> op >> int_k) {
      k = int2key(int_k);
      assert(op == "INSERT");
      tree->insert(k, randval(e), nullptr, 0, false, true);
      if (++ cnt % LOAD_HEARTBEAT == 0) {
        printf("thread %lu: %d load entries loaded.\n", loader_id, cnt);
      }
    }
  }
  else {  // string workloads
    std::string str_k;
    std::string line;
    while (std::getline(load_in, line)) {
      if (!line.size()) continue;
      std::istringstream tmp(line);
      tmp >> op >> str_k;
      k = str2key(str_k);
      assert(op == "INSERT");
      tree->insert(k, randval(e), nullptr, 0, false, true);
      if (++ cnt % LOAD_HEARTBEAT == 0) {
        printf("thread %lu: %d load entries loaded.\n", loader_id, cnt);
      }
    }
  }
  printf("loader %lu load finish\n", loader_id);
}


void thread_run(int id) {
  bindCore(id * 2 + 1);  // bind to CPUs in NUMA that close to mlx5_2

  dsm->registerThread();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;

  printf("I am %lu\n", my_id);

  if (id == 0) {
    bench_timer.begin();
  }

  // 1. insert ycsb_load
  if (id < std::min(kThreadCount, LOADER_NUM)) {
    thread_load(id);
  }

  // 2. load ycsb_trans
  Request* req = new Request[MAX_THREAD_REQUEST];
  int req_num = 0;
  std::ifstream trans_in(ycsb_trans_path + std::to_string(my_id));
  if (!trans_in.is_open()) {
    printf("Error opening trans file\n");
    assert(false);
  }
  std::string op;
  int cnt = 0;
  if (!kIsStr) {  // int workloads
    int range_size = 0;
    uint64_t int_k;
    while(trans_in >> op >> int_k) {
      if (op == "SCAN") trans_in >> range_size;
      else range_size = 0;
      Request r;
      r.is_search = (op == "READ");
      r.is_insert = (op == "INSERT");
      r.is_update = (op == "UPDATE");
      r.range_size = fix_range_size >= 0 ? fix_range_size : range_size;
      r.k = int2key(int_k);
      if (rm_write_conflict) {
        if (r.is_update || r.is_insert) {
          uint64_t all_thread_num = kThreadCount * dsm->getClusterSize();
          r.k = dsm->getNoComflictKey(key_hash(r.k), my_id, all_thread_num);
        }
      }
      req[req_num ++] = r;
      if (++ cnt % LOAD_HEARTBEAT == 0) {
        printf("thread %d: %d trans entries loaded.\n", id, cnt);
      }
    }
  }
  else {
    std::string str_k;
    std::string line;
    while (std::getline(trans_in, line)) {
      if (!line.size()) continue;
      std::istringstream tmp(line);
      tmp >> op >> str_k;
      assert(op != "SCAN");  // string workloads currently does not support SCAN
      Request r;
      r.is_search = (op == "READ");
      r.is_insert = (op == "INSERT");
      r.is_update = (op == "UPDATE");
      assert(r.is_search || r.is_insert || r.is_update);
      r.range_size = 0;
      r.k = str2key(str_k);
      if (rm_write_conflict) {
        if (r.is_update || r.is_insert) {
          uint64_t all_thread_num = kThreadCount * dsm->getClusterSize();
          r.k = dsm->getNoComflictKey(key_hash(r.k), my_id, all_thread_num);
        }
      }
      req[req_num ++] = r;
      if (++ cnt % LOAD_HEARTBEAT == 0) {
        printf("thread %d: %d trans entries loaded.\n", id, cnt);
      }
    }
  }

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d finish\n", dsm->getMyNodeID());
    dsm->barrier("warm_finish");

    uint64_t ns = bench_timer.end();
    printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

    ready = true;
    warmup_cnt.store(-1);
  }
  while (warmup_cnt.load() != -1)
    ;

  // 3. start ycsb test
  if (!kIsScan && kUseCoro) {
    tree->run_coroutine(gen_func, work_func, kCoroCnt, req, req_num);
  }
  else {
    /// without coro
    Timer timer;
    auto gen = new RequsetGenBench(dsm, req, req_num, 0, 0);
    auto thread_id = dsm->getMyThreadID();

    while (!need_stop) {
      auto r = gen->next();

      timer.begin();
      work_func(tree, r, nullptr, 0);
      auto us_10 = timer.end() / 100;

      if (us_10 >= LATENCY_WINDOWS) {
        us_10 = LATENCY_WINDOWS - 1;
      }
      latency[thread_id][0][us_10]++;
    }
  }
  printf("thread %d exit.\n", id);
}

void parse_args(int argc, char *argv[]) {
  if (argc != 6 && argc != 7) {
    printf("Usage: ./ycsb_test kNodeCount kThreadCount kCoroCnt workload_type[randint/email] workload_idx[a/b/c/d/e] [fix_range_size/rm_write_conflict]\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kThreadCount = atoi(argv[2]);
  kCoroCnt = atoi(argv[3]);
  kIsStr = (std::string(argv[4]) == "email");
  kIsScan = (std::string(argv[5]) == "e");
  ycsb_load_path = "../ycsb/workloads/load_" + std::string(argv[4]) + "_workload" + std::string(argv[5]);
  ycsb_trans_path = "../ycsb/workloads/txn_" + std::string(argv[4]) + "_workload" + std::string(argv[5]);
  if (argc == 7) {
    if(kIsScan) fix_range_size = atoi(argv[6]);
    else rm_write_conflict = (atoi(argv[6]) != 0);
  }

  printf("kNodeCount %d, kThreadCount %d, kCoroCnt %d\n", kNodeCount, kThreadCount, kCoroCnt);
  printf("ycsb_load: %s\n", ycsb_load_path.c_str());
  printf("ycsb_trans: %s\n", ycsb_trans_path.c_str());
  if (argc == 7) {
    if(kIsScan) printf("fix_range_size: %d\n", fix_range_size);
    else printf("rm_write_conflict: %s\n", rm_write_conflict ? "true" : "false");
  }
}

void save_latency(int epoch_id) {
  // sum up local latency cnt
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k)
      for (int j = 0; j < MAX_CORO_NUM; ++j) {
        latency_th_all[i] += latency[k][j][i];
    }
  }
  // store in file
  std::ofstream f_out("../us_lat/epoch_" + std::to_string(epoch_id) + ".lat");
  f_out << std::setiosflags(std::ios::fixed) << std::setprecision(1);
  if (f_out.is_open()) {
    for (int i = 0; i < LATENCY_WINDOWS; ++i) {
      f_out << i / 10.0 << "\t" << latency_th_all[i] << std::endl;
    }
    f_out.close();
  }
  else {
    printf("Fail to write file!\n");
    assert(false);
  }
  memset(latency, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_CORO_NUM * LATENCY_WINDOWS);
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  assert(kNodeCount >= MEMORY_NODE_NUM);
  config.machineNR = kNodeCount;
  config.threadNR = kThreadCount;
  dsm = DSM::getInstance(config);
  bindCore(kThreadCount * 2 + 1);
  if (rm_write_conflict) {
    dsm->loadKeySpace(ycsb_load_path, kIsStr);
  }
  dsm->registerThread();
  tree = new Tree(dsm);
  dsm->barrier("benchmark");

  for (int i = 0; i < kThreadCount; i ++) {
    th[i] = std::thread(thread_run, i);
  }

  while (!ready.load())
    ;
  timespec s, e;
  uint64_t pre_tp = 0;
  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  while(!need_stop) {

    sleep(TIME_INTERVAL);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      for (int j = 0; j < kCoroCnt; ++j)
        all_tp += tp[i][j];
    }
    clock_gettime(CLOCK_REALTIME, &s);

    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    double all = 0, hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i] + cache_miss[i]);
      hit += cache_hit[i];
    }

    uint64_t lock_fail_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      lock_fail_cnt += lock_fail[i];
      // try_lock_cnt += try_lock[i];
    }

    uint64_t try_write_op_cnt = 0, write_handover_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      write_handover_cnt += write_handover_num[i];
      try_write_op_cnt += try_write_op[i];
    }

    uint64_t try_read_op_cnt = 0, read_handover_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      read_handover_cnt += read_handover_num[i];
      try_read_op_cnt += try_read_op[i];
    }

    uint64_t try_read_leaf_cnt = 0, read_leaf_retry_cnt = 0, leaf_cache_invalid_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      try_read_leaf_cnt += try_read_leaf[i];
      read_leaf_retry_cnt += read_leaf_retry[i];
      leaf_cache_invalid_cnt += leaf_cache_invalid[i];
    }

    uint64_t try_read_node_cnt = 0, read_node_repair_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      try_read_node_cnt += try_read_node[i];
      read_node_repair_cnt += read_node_repair[i];
    }

    uint64_t read_node_type_cnt[MAX_NODE_TYPE_NUM];
    memset(read_node_type_cnt, 0, sizeof(uint64_t) * MAX_NODE_TYPE_NUM);
    for (int i = 0; i < MAX_NODE_TYPE_NUM; ++i) {
      for (int j = 0; j < MAX_APP_THREAD; ++j) {
        read_node_type_cnt[i] += read_node_type[j][i];
      }
    }

    uint64_t all_retry_cnt[MAX_FLAG_NUM];
    memset(all_retry_cnt, 0, sizeof(uint64_t) * MAX_FLAG_NUM);
    for (int i = 0; i < MAX_FLAG_NUM; ++i) {
      for (int j = 0; j < MAX_APP_THREAD; ++j) {
        all_retry_cnt[i] += retry_cnt[j][i];
      }
    }
    tree->clear_debug_info();

#ifdef EPOCH_LAT_TEST
    save_latency(++ count);
#else
    if (++ count == TEST_EPOCH / 2) {  // rm latency during warm up
      memset(latency, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_CORO_NUM * LATENCY_WINDOWS);
    }
#endif

    if (dsm->getMyNodeID() == 1) {
      printf("total %lu", all_retry_cnt[0]);
      for (int i = 1; i < MAX_FLAG_NUM; ++ i) {
        printf(",  retry%d %lu", i, all_retry_cnt[i]);
      }
      printf("\n");
    }

    double per_node_tp = cap * 1.0 / microseconds;
    uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));  // only node 0 return the sum

    printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

    if (dsm->getMyNodeID() == 0) {
      printf("epoch %d passed!\n", count);
      printf("cluster throughput %.3f Mops\n", cluster_tp / 1000.0);
      printf("cache hit rate: %lf\n", hit * 1.0 / all);
      printf("avg. lock/cas fail cnt: %lf\n", lock_fail_cnt * 1.0 / try_write_op_cnt);
      printf("write combining rate: %lf\n", write_handover_cnt * 1.0 / try_write_op_cnt);
      printf("read delegation rate: %lf\n", read_handover_cnt * 1.0 / try_read_op_cnt);
      printf("read leaf retry rate: %lf\n", read_leaf_retry_cnt * 1.0 / try_read_leaf_cnt);
      printf("read invalid leaf rate: %lf\n", leaf_cache_invalid_cnt * 1.0 / try_read_leaf_cnt);
      printf("read node repair rate: %lf\n", read_node_repair_cnt * 1.0 / try_read_node_cnt);
      printf("read invalid node rate: %lf\n", all_retry_cnt[INVALID_NODE] * 1.0 / try_read_node_cnt);
      for (int i = 1; i < MAX_NODE_TYPE_NUM; ++ i) {
        printf("node_type%d %lu   ", i, read_node_type_cnt[i]);
      }
      printf("\n\n");
    }
    if (count >= TEST_EPOCH) {
      need_stop = true;
    }
  }
#ifndef EPOCH_LAT_TEST
  save_latency(1);
#endif
  printf("[END]\n");
  for (int i = 0; i < kThreadCount; i++) {
    th[i].join();
    printf("Thread %d joined.\n", i);
  }
  tree->statistics();
  dsm->barrier("fin");

  return 0;
}
