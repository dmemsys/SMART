#include "DSM.h"
#include "GlobalAddress.h"
#include "Common.h"
#include "LocalLockTable.h"
#include "Timer.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>

int kReadRatio;
int kThreadCount;
int kNodeCount = 1;

uint64_t addressCnt = 1000000;
uint64_t msgSize = 1024;
double zipfan = 0.99;

std::thread th[MAX_APP_THREAD];

volatile bool need_stop = false;
volatile bool log_start = false;

DSM *dsm;
struct Record {
  timespec s, e;
  int cas_num;
  GlobalAddress addr;
  Record() {}
  Record(const timespec& s, const timespec& e, int cas_num, const GlobalAddress& addr) :
        s(s), e(e), cas_num(cas_num), addr(addr) {}
};
inline bool operator<(const timespec &a, const timespec &b) {
  return std::make_pair(a.tv_sec, a.tv_nsec) < std::make_pair(b.tv_sec, b.tv_nsec);
}
inline bool operator>=(const timespec &a, const timespec &b) {
  return !(a < b);
}
std::vector<Record> read_logs[MAX_APP_THREAD];
std::vector<Record> write_logs[MAX_APP_THREAD];

struct CalResult {
  double redundant_op;
  double redundant_cas;
  double avg_time_windows;
};

inline uint64_t to_offset(uint64_t k) {
  return ((CityHash64((char *)&k, sizeof(k))) % addressCnt) * (msgSize + 8);  // kv + lock
}


class AddressGenBench {
public:
  AddressGenBench(DSM *dsm): id(dsm->getMyThreadID()) {
    seed = rdtsc();
    mehcached_zipf_init(&state, addressCnt, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  GlobalAddress next(bool &is_read) {
    is_read = rand_r(&seed) % 100 < kReadRatio;
    uint64_t dis = mehcached_zipf_next(&state);
    return GlobalAddress{0, to_offset(dis)};
  }

private:
  int id;
  unsigned int seed;
  struct zipf_gen_state state;
};


void do_read(DSM *dsm, const GlobalAddress& addr, timespec& s, timespec& e) {
  Value v = 0;
  auto read_buffer = (dsm->get_rbuf(0)).get_page_buffer();
  clock_gettime(CLOCK_REALTIME, &s);
  dsm->read_sync(read_buffer, addr, msgSize);
  v = *(Value *)read_buffer;
  clock_gettime(CLOCK_REALTIME, &e);
  UNUSED(v);
  return;
}


int do_write(DSM *dsm, const GlobalAddress& addr, timespec& s, timespec& e) {
  unsigned int seed = rdtsc();
  Value v = rand_r(&seed);
  auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
  auto write_buffer = (dsm->get_rbuf(0)).get_page_buffer();
  bool res;
  int cas_cnt = 0;

  // unlock function
  auto unlock = [=](const GlobalAddress &addr){
    dsm->cas_sync(GADD(addr, msgSize), 1, 0, cas_buffer);
  };

  // write back function
  auto write_back = [=](const Value &v){
    *(Value *)write_buffer = v;
    dsm->write_sync(write_buffer, addr, msgSize);
  };

  // acquire lock
  clock_gettime(CLOCK_REALTIME, &s);
re_acquire:
  ++ cas_cnt;
  res = dsm->cas_sync(GADD(addr, msgSize), 0, 1, cas_buffer);
  if (!res) {
    goto re_acquire;
  }
  write_back(v);
  unlock(addr);
  clock_gettime(CLOCK_REALTIME, &e);
  ++ cas_cnt;
  return cas_cnt;
}


std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};


void thread_run(int id) {

  bindCore(id * 2 + 1);
  dsm->registerThread();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;
  printf("I am %lu\n", my_id);

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    dsm->barrier("warm_finish");
    ready = true;
    warmup_cnt.store(-1);
  }
  while (warmup_cnt.load() != -1)
    ;

  auto gen = new AddressGenBench(dsm);
  timespec s, e;

  while (!need_stop) {
    bool is_read;
    int cas_cnt = 0;
    auto addr = gen->next(is_read);
    // timer
    if (is_read) do_read(dsm, addr, s, e);
    else cas_cnt = do_write(dsm, addr, s,e);
    auto& logs = is_read ? read_logs[id] : write_logs[id];
    if(log_start) logs.push_back(Record(s, e, cas_cnt, addr));
  }
  printf("thread %d exit.\n", id);
}

void parse_args(int argc, char *argv[]) {
  if (argc != 4) {
    printf("Usage: ./redundant_test kReadRatio kThreadCount zipfan\n");
    exit(-1);
  }

  kReadRatio = atoi(argv[1]);
  kThreadCount = atoi(argv[2]);
  zipfan = atof(argv[3]);

  printf("kNodeCount %d, kReadRatio %d, kThreadCount %d, zipfan %lf\n", kNodeCount,
         kReadRatio, kThreadCount, zipfan);
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  assert(MEMORY_NODE_NUM == 1);
  config.machineNR = kNodeCount;
  config.threadNR = kThreadCount;
  dsm = DSM::getInstance(config);
  dsm->registerThread();
  bindCore(kThreadCount * 2 + 1);

  dsm->barrier("benchmark");

  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

  while (!ready.load())
    ;

  sleep(3);
  log_start = true;
  sleep(20);
  need_stop = true;


  for (int i = 0; i < kThreadCount; i++) {
    th[i].join();
    printf("Thread %d joined.\n", i);
  }

  // sort records
  printf("Start sorting...\n");
  auto sort_logs = [=](std::vector<Record> &total_log, std::vector<Record>* logs) {
    for (int i = 0; i < MAX_APP_THREAD; ++ i) {
      total_log.insert(total_log.end(), logs[i].begin(), logs[i].end());
    }
    std::sort(total_log.begin(), total_log.end(), [](const Record& a, const Record& b){
      return a.s < b.s;
    });
  };
  std::vector<Record> total_read_log, total_write_log;
  sort_logs(total_read_log, read_logs);
  sort_logs(total_write_log, write_logs);

  // calculate
  printf("Start calculating...\n");
  auto calculate_redundant = [=](const std::vector<Record> &total_log) {
    uint64_t redundant_op = 0, redundant_cas = 0;
    double time_windows = 0;
    int i = 0, windows_cnt = 0;
    while (i < (int)total_log.size()) {
      auto& r1 = total_log[i];
      while (++ i < (int)total_log.size()) {
        auto& r2 = total_log[i];
        if (r2.s >= r1.s && r2.s < r1.e) {
          if (r1.addr == r2.addr) {
            ++ redundant_op;
            redundant_cas += r2.cas_num;
          }
        }
        else {
          break;
        }
      }
      time_windows += (r1.e.tv_sec - r1.s.tv_sec) * 1000000 + (double)(r1.e.tv_nsec - r1.s.tv_nsec) / 1000;
      ++ windows_cnt;
    }
    return CalResult{(double)redundant_op / windows_cnt, (double)redundant_cas / windows_cnt, time_windows / windows_cnt};
  };
  auto read_results = calculate_redundant(total_read_log);
  auto write_results = calculate_redundant(total_write_log);
  printf("Calculation done!\n");
  printf("Avg. redundant rdma_read: %lf\n", read_results.redundant_op);
  printf("Avg. read time windows: %lf us\n", read_results.avg_time_windows);
  printf("Avg. redundant rdma_write: %lf\n", write_results.redundant_op);
  printf("Avg. redundant rdma_cas: %lf\n", write_results.redundant_cas);
  printf("Avg. write time windows: %lf us\n", write_results.avg_time_windows);

  printf("[END]\n");
  dsm->barrier("fin");

  return 0;
}
