#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <queue>
#include <bitset>
#include <limits>

#include "Debug.h"
#include "HugePageAlloc.h"
#include "Rdma.h"

#include "WRLock.h"

// Environment Config
#define MAX_MACHINE 20
#define MEMORY_NODE_NUM 2
#define CPU_PHYSICAL_CORE_NUM 72  // [CONFIG]
#define MAX_CORO_NUM 8

#define LATENCY_WINDOWS 100000
#define ALLOC_ALLIGN_BIT 8
#define MAX_KEY_SPACE_SIZE 60000000
// #define KEY_SPACE_LIMIT


// Auxiliary function
#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define UNUSED(x) (void)(x)

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define ROUND_UP(x, n) (((x) + (1<<(n)) - 1) & ~((1<<(n)) - 1))

#define ROUND_DOWN(x, n) ((x) & ~((1<<(n)) - 1))

#define MESSAGE_SIZE 96 // byte

#define RAW_RECV_CQ_COUNT 4096 // 128


// app thread
#define MAX_APP_THREAD 65    // one additional thread for data statistics(main thread)  [config]
#define APP_MESSAGE_NR 96
#define POLL_CQ_MAX_CNT_ONCE 8

// dir thread
#define NR_DIRECTORY 1
#define DIR_MESSAGE_NR 128


void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/coroutine/all.hpp>
#include <boost/crc.hpp>

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

using CheckFunc = std::function<bool ()>;
using CoroQueue = std::queue<std::pair<uint16_t, CheckFunc> >;
struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  CoroQueue *busy_waiting_queue;
  int coro_id;
};

using CRCProcessor = boost::crc_optimal<64, 0x42F0E1EBA9EA3693, 0xffffffffffffffff, 0xffffffffffffffff, false, false>;

namespace define {   // namespace define

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// Remote Allocation
constexpr uint64_t dsmSize           = 64;        // GB  [CONFIG]
constexpr uint64_t kChunkSize        = 16 * MB;   // B

// Rdma Buffer
constexpr uint64_t rdmaBufferSize    = 4;         // GB  [CONFIG]
constexpr int64_t kPerThreadRdmaBuf  = rdmaBufferSize * define::GB / MAX_APP_THREAD;
constexpr int64_t kPerCoroRdmaBuf    = kPerThreadRdmaBuf / MAX_CORO_NUM;

// Cache (MB)
constexpr int kIndexCacheSize = 600;

// KV
constexpr uint32_t keyLen = 8;
constexpr uint32_t simulatedValLen = 8;
constexpr uint32_t allocAlignLeafSize = ROUND_UP(keyLen + simulatedValLen + 8 + 2, ALLOC_ALLIGN_BIT);

// Tree
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0);

// Internal Node
constexpr uint32_t allocationPageSize = 8 + 8 + 256 * 8;
constexpr uint32_t allocAlignPageSize = ROUND_UP(allocationPageSize, ALLOC_ALLIGN_BIT);

// Internal Entry
constexpr uint32_t kvLenBit        = 7;
constexpr uint32_t nodeTypeNumBit  = 5;
constexpr uint32_t mnIdBit         = 8;
constexpr uint32_t offsetBit       = 48 - ALLOC_ALLIGN_BIT;
constexpr uint32_t hPartialLenMax  = 6;

// On-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = ON_CHIP_SIZE * 1024;
constexpr uint64_t kLocalLockNum = 4 * MB;  // tune to an appropriate value (as small as possible without affect the performance)
constexpr uint64_t kOnChipLockNum = kLockChipMemSize * 8;  // 1bit-lock
}


using Key = std::array<uint8_t, define::keyLen>;
using Value = uint64_t;
constexpr uint64_t kKeyMin = 1;
#ifdef KEY_SPACE_LIMIT
constexpr uint64_t kKeyMax = 60000000;  // only for int workloads
#endif
constexpr Value kValueNull = std::numeric_limits<Value>::min();
constexpr Value kValueMin = 1;
constexpr Value kValueMax = std::numeric_limits<Value>::max();

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

#endif /* __COMMON_H__ */
