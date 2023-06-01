#if !defined(_LOCAL_LOCK_TABLE_H_)
#define _LOCAL_LOCK_TABLE_H_

#include "Common.h"
#include "Key.h"
#include "Node.h"
#include "GlobalAddress.h"
#include "Hash.h"

#include <queue>
#include <set>
#include <atomic>
#include <mutex>


#define MAX_HANDOVER_TYPE_NUM 2
#define MAX_HOCL_HANDOVER 8
enum HandoverType {
  READ_HANDOVER,
  WRITE_HANDOVER,
};


struct LocalLockNode {
  // read waiting queue
  std::atomic<uint8_t> read_current;
  std::atomic<uint8_t> read_ticket;
  volatile bool read_handover;

  // write waiting queue
  std::atomic<uint8_t> write_current;
  std::atomic<uint8_t> write_ticket;
  volatile bool write_handover;

  /* ----- auxiliary variables for simplicity  TODO: dynamic allocate ----- */
  // identical time window start time
  std::atomic<bool> window_start;
  std::atomic<uint8_t> read_window;
  std::atomic<uint8_t> write_window;
  std::mutex r_lock;
  std::mutex w_lock;

  // hash conflict
  std::atomic<Key*> unique_read_key;
  std::atomic<Key*> unique_write_key;
  GlobalAddress unique_addr;

  // read delegation
  bool res;
  union {
    Value ret_value;
    InternalEntry ret_p;
  };

  // write combining
  std::mutex wc_lock;
  Value wc_buffer;

  // lock handover
  int handover_cnt;

  LocalLockNode() : read_current(0), read_ticket(0), read_handover(0), write_current(0), write_ticket(0), write_handover(0),
                    window_start(0), read_window(0), write_window(0),
                    unique_read_key(0), unique_write_key(0), unique_addr(0), handover_cnt(0) {}
};


class LocalLockTable {
public:
  LocalLockTable() {}

  // read-delegation
  std::pair<bool, bool> acquire_local_read_lock(const Key& k, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  void release_local_read_lock(const Key& k, std::pair<bool, bool> acquire_ret, bool& res, Value& ret_value);

  // write-combining
  std::pair<bool, bool> acquire_local_write_lock(const Key& k, const Value& v, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  bool get_combining_value(const Key& k, Value& v);
  void release_local_write_lock(const Key& k, std::pair<bool, bool> acquire_ret);

  /* ---- baseline ---- */
  // lock-handover
  bool acquire_local_lock(const GlobalAddress& addr, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  using RemoteFunc = std::function<void (const GlobalAddress &)>;
  void release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func);
  void release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func, RemoteFunc write_without_unlock, RemoteFunc write_and_unlock);

  // cas-handover
  bool acquire_local_lock(const Key& k, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  void release_local_lock(const Key& k, bool& res, InternalEntry& ret_p);

  // write_testing
  bool acquire_local_write_lock(const GlobalAddress& addr, const Value& v, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  using RemoteWriteBackFunc = std::function<void (const Value&)>;
  void release_local_write_lock(const GlobalAddress& addr, RemoteFunc unlock_func, const Value& v, RemoteWriteBackFunc write_func);

  // read-testing
  bool acquire_local_read_lock(const GlobalAddress& addr, CoroQueue *waiting_queue = nullptr, CoroContext *cxt = nullptr, int coro_id = 0);
  void release_local_read_lock(const GlobalAddress& addr, bool& res, Value& ret_value);

private:
  Hash hasher;
  LocalLockNode local_locks[define::kLocalLockNum];
};


// read-delegation
inline std::pair<bool, bool> LocalLockTable::acquire_local_read_lock(const Key& k, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  Key* unique_key = nullptr;
  Key* new_key = new Key(k);
  bool res = node.unique_read_key.compare_exchange_strong(unique_key, new_key);
  if (!res) {
    delete new_key;
    if (*unique_key != k) {  // conflict keys
      return std::make_pair(false, true);
    }
  }

  uint8_t ticket = node.read_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.read_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.read_current.load(std::memory_order_relaxed);
  }
  unique_key = node.unique_read_key.load();
  if (!unique_key || *unique_key != k) {  // conflict keys
    if (node.read_window) {
      -- node.read_window;
      if (!node.read_window && !node.write_window) {
        node.window_start = false;
      }
    }
    // node.read_handover = false;
    node.read_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.read_window) {
    node.read_handover = false;
  }
  return std::make_pair(node.read_handover, false);
}

// read-delegation
inline void LocalLockTable::release_local_read_lock(const Key& k, std::pair<bool, bool> acquire_ret, bool& res, Value& ret_value) {
  if (acquire_ret.second) return;

  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  if (!node.read_handover) {  // winner
    node.res = res;
    node.ret_value = ret_value;
  }
  else {  // losers accept the ret val from winner
    res = node.res;
    ret_value = node.ret_value;
  }

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.read_handover && node.window_start.compare_exchange_strong(start_window, true)) {
    // read time window start
    node.read_window = ((1UL << 8) + ticket - current) % (1UL << 8);

    node.w_lock.lock();
    auto w_current = node.write_current.load(std::memory_order_relaxed);
    node.write_window = ((1UL << 8) + node.write_ticket.load(std::memory_order_relaxed) - w_current) % (1UL << 8);
    node.w_lock.unlock();
  }

  node.read_handover = ticket != (uint8_t)(current + 1);

  if (!node.read_handover) {  // next epoch
    node.unique_read_key = nullptr;
  }

  node.r_lock.lock();
  if (node.read_window) {
    -- node.read_window;
    if (!node.read_window && !node.write_window) {
      node.window_start = false;
    }
  }
  node.read_current.fetch_add(1);
  node.r_lock.unlock();

  return;
}

// write-combining
inline std::pair<bool, bool> LocalLockTable::acquire_local_write_lock(const Key& k, const Value& v, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  Key* unique_key = nullptr;
  Key* new_key = new Key(k);
  bool res = node.unique_write_key.compare_exchange_strong(unique_key, new_key);
  if (!res) {
    delete new_key;
    if (*unique_key != k) {  // conflict keys
      return std::make_pair(false, true);
    }
  }

  node.wc_lock.lock();
  node.wc_buffer = v;     // local overwrite (combining)
  node.wc_lock.unlock();

  uint8_t ticket = node.write_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }
  unique_key = node.unique_write_key.load();
  if (!unique_key || *unique_key != k) {  // conflict keys
    if (node.write_window) {
      -- node.write_window;
      if (!node.read_window && !node.write_window) {
        node.window_start = false;
      }
    }
    // node.write_handover = false;
    node.write_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.write_window) {
    node.write_handover = false;
  }
  return std::make_pair(node.write_handover, false);
}

// write-combining
inline bool LocalLockTable::get_combining_value(const Key& k, Value& v) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];
  bool res = false;
  Key* unique_key = node.unique_write_key.load();
  if (unique_key && *unique_key == k) {  // wc
    node.wc_lock.lock();
    res = node.wc_buffer != v;
    v = node.wc_buffer;
    node.wc_lock.unlock();
  }
  return res;
}

// write-combining
inline void LocalLockTable::release_local_write_lock(const Key& k, std::pair<bool, bool> acquire_ret) {
  if (acquire_ret.second) return;

  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.write_handover && node.window_start.compare_exchange_strong(start_window, true)) {
    // write time window start
    node.r_lock.lock();
    auto r_current = node.read_current.load(std::memory_order_relaxed);
    node.read_window = ((1UL << 8) + node.read_ticket.load(std::memory_order_relaxed) - r_current) % (1UL << 8);
    node.r_lock.unlock();

    node.write_window = ((1UL << 8) + ticket - current) % (1UL << 8);
  }

  node.write_handover = ticket != (uint8_t)(current + 1);

  if (!node.write_handover) {  // next epoch
    node.unique_write_key = nullptr;
  }

  node.w_lock.lock();
  if (node.write_window) {
    -- node.write_window;
    if (!node.read_window && !node.write_window) {
      node.window_start = false;
    }
  }
  node.write_current.fetch_add(1);
  node.w_lock.unlock();

  return;
}

// lock-handover
inline bool LocalLockTable::acquire_local_lock(const GlobalAddress& addr, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    node.unique_addr = addr;
  }
  // if (node.unique_addr == addr) {
  //   node.handover_cnt ++;
  // }
  return node.write_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// lock-handover
inline void LocalLockTable::release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  if (node.unique_addr != addr) {
    unlock_func(addr);
  }
  if (!node.write_handover) {
    unlock_func(node.unique_addr);
  }

  node.write_current.fetch_add(1);
  return;
}

// lock-handover + embedding lock
inline void LocalLockTable::release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func, RemoteFunc write_without_unlock, RemoteFunc write_and_unlock) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  if (!node.write_handover) {
    if (node.unique_addr != addr) {
      unlock_func(node.unique_addr);
      write_and_unlock(addr);
    }
    else {
      write_and_unlock(addr);
    }
  }
  else {
    if (node.unique_addr != addr) {
      write_and_unlock(addr);
    }
    else {
      write_without_unlock(addr);
    }
  }

  node.write_current.fetch_add(1);
  return;
}

// cas-handover
inline bool LocalLockTable::acquire_local_lock(const Key& k, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    auto old_key = node.unique_write_key.load(std::memory_order_relaxed);
    node.unique_write_key = new Key(k);
    if(old_key) delete old_key;
  }
  // if (*node.unique_write_key == k) {
  //   node.handover_cnt ++;
  // }
  auto unique_key = node.unique_write_key.load(std::memory_order_relaxed);
  return node.write_handover && (unique_key && *unique_key == k);  // only if updating at the same k can this update handover
}

// cas-handover
inline void LocalLockTable::release_local_lock(const Key& k, bool& res, InternalEntry& ret_p) {
  auto &node = local_locks[hasher.get_hashed_lock_index(k)];

  auto unique_key = node.unique_write_key.load(std::memory_order_relaxed);
  if (unique_key && *unique_key == k) {
    if (!node.write_handover) {  // winner
      node.res = res;
      node.ret_p = ret_p;
    }
    else {
      res = node.res;
      ret_p = node.ret_p;
    }
  }

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  node.write_current.fetch_add(1);
  return;
}

// write-testing
inline bool LocalLockTable::acquire_local_write_lock(const GlobalAddress& addr, const Value& v, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  node.wc_lock.lock();
  node.wc_buffer = v;     // local overwrite (combining)
  node.wc_lock.unlock();

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.write_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    node.unique_addr = addr;
  }
  return node.write_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// write-testing
inline void LocalLockTable::release_local_write_lock(const GlobalAddress& addr, RemoteFunc unlock_func, const Value& v, RemoteWriteBackFunc write_func) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  if (!node.write_handover) {
    // unlock lock_node.unique_key
    node.wc_lock.lock();
    Value wc_v = node.wc_buffer;
    node.wc_lock.unlock();
    write_func(wc_v);
    unlock_func(node.unique_addr);
  }
  if (node.unique_addr != addr) {
    write_func(v);
    unlock_func(addr);
  }

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);

  node.write_current.fetch_add(1);
  return;
}

// read-testing
inline bool LocalLockTable::acquire_local_read_lock(const GlobalAddress& addr, CoroQueue *waiting_queue, CoroContext *cxt, int coro_id) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.read_ticket.fetch_add(1);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (cxt != nullptr) {
      waiting_queue->push(std::make_pair(coro_id, [=, &node](){
        return ticket == node.read_current.load(std::memory_order_relaxed);
      }));
      (*cxt->yield)(*cxt->master);
    }
    current = node.read_current.load(std::memory_order_relaxed);
  }

  if (!node.read_handover) {  // winner
    node.unique_addr = addr;
  }
  return node.read_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// read-testing
inline void LocalLockTable::release_local_read_lock(const GlobalAddress& addr, bool& res, Value& ret_value) {
  auto &node = local_locks[hasher.get_hashed_lock_index(addr)];

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  if (node.unique_addr == addr) {  // hash conflict clients is not involved in ret value handover
    if (!node.read_handover) {  // winner
      node.res = res;
      node.ret_value = ret_value;
    }
    else {  // losers accept the ret val from winner
      res = node.res;
      ret_value = node.ret_value;
    }
  }
  node.read_handover = ticket != (uint8_t)(current + 1);
  node.read_current.fetch_add(1);
  return;
}

#endif // _LOCAL_LOCK_TABLE_H_
