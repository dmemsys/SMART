#if !defined(_NORMAL_CACHE_H_)
#define _NORMAL_CACHE_H_

#include "Common.h"
#include "Node.h"
#include "DSM.h"

#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <atomic>

/*
  For node granularity cache, realize by concurrent_unordered_map. [key prefix -> cache node]
*/
struct CacheEntry {
  // fixed
  uint8_t depth;
  GlobalAddress addr;
  std::vector<InternalEntry> records;
  // faa
  // volatile mutable uint64_t counter;

  CacheEntry() {}
  CacheEntry(const InternalPage* p_node, const GlobalAddress& addr) :
             depth(p_node->hdr.depth + p_node->hdr.partial_len), addr(addr) {
    for (int i = 0; i < node_type_to_num(p_node->hdr.type()); ++ i) {
      const auto& e = p_node->records[i];
      records.push_back(e);
    }
  }

  uint64_t content_size() const {
    return sizeof(uint8_t) + sizeof(GlobalAddress) + sizeof(InternalEntry) * records.size();  // + sizeof(uint64_t)
  }
};


using CacheKey = std::vector<uint8_t>;
class cache_key_hash {
public:
  cache_key_hash() {}
  uint64_t operator() (const CacheKey& key) const {
    uint64_t hash_key = 0;
    for (auto partial : key) hash_key = (hash_key << 8) + partial;
    return hash_key;
  }
};


class NormalCache {
public:
  NormalCache(int cache_size, DSM *dsm);

  void add_to_cache(const Key& k, const InternalPage* p_node, const GlobalAddress &node_addr);

  bool search_from_cache(const Key& k, volatile CacheEntry**& entry_ptr_ptr, CacheEntry*& entry_ptr, int& entry_idx);
  void search_range_from_cache(const Key &from, const Key &to, std::vector<RangeCache> &result);
  void invalidate(volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr);
  void statistics();

private:
  void _insert(const CacheKey& byte_array, CacheEntry* new_entry);

  bool _search(CacheKey& byte_prefix, uint8_t last_byte, volatile CacheEntry**& entry_ptr_ptr, CacheEntry*& entry_ptr, int& entry_idx);

  void _evict();
  // void _evict_one();
  // void _get_a_random_entry(volatile CacheEntry** &entry_ptr_ptr, CacheEntry* &entry_ptr);
  void _safely_delete(CacheEntry* cache_entry);

private:
  // Cache
  uint64_t cache_size; // MB
  std::atomic<int64_t> free_size;
  // std::atomic<uint64_t> map_size;
  tbb::concurrent_queue<CacheKey> keys;

  using CacheMap = tbb::concurrent_unordered_map<CacheKey, volatile CacheEntry*, cache_key_hash>;
  CacheMap cache_map;

  // GC
  tbb::concurrent_queue<CacheEntry*> cache_entry_gc;
  static const int safely_free_epoch = 2 * MAX_APP_THREAD * MAX_CORO_NUM;

  // Eviction
  DSM *dsm;
  tbb::concurrent_queue<std::pair<volatile CacheEntry**, CacheEntry*> > eviction_list;
};


#endif // _NORMAL_CACHE_H_
