#if !defined(_RADIX_CACHE_H_)
#define _RADIX_CACHE_H_

#include "Common.h"
#include "Node.h"
#include "NormalCache.h"

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
// #include <tbb/concurrent_vector.h>
#include <vector>
#include <queue>
#include <atomic>
#include <stack>


struct CacheNodeValue {
  volatile CacheEntry* cache_entry;
  volatile void * next;

  CacheNodeValue() :  cache_entry(nullptr), next(nullptr) {}
  CacheNodeValue(CacheEntry* cache_entry, void *next) :
                 cache_entry(cache_entry), next(next) {}
};


class CacheHeader {
public:
  uint8_t depth;
  std::vector<uint8_t> partial;

  CacheHeader() : depth(0) {}

  CacheHeader(const std::vector<uint8_t>& byte_array, int depth, int partial_len) : depth(depth) {
    for (int i = 0; i < partial_len; ++ i) {
      partial.push_back(byte_array[depth + i]);
    }
  }

  static CacheHeader* split_header(const CacheHeader* old_hdr, int diff_idx) {
    auto new_hdr = new CacheHeader();
    for (int i = diff_idx + 1; i < (int)old_hdr->partial.size(); ++ i) new_hdr->partial.push_back(old_hdr->partial[i]);
    new_hdr->depth = old_hdr->depth + diff_idx + 1;
    return new_hdr;
  }

  uint64_t content_size() const {
    return sizeof(uint8_t) + sizeof(uint8_t) * partial.size();
  }
};


class no_hash {
public:
  no_hash() {}
  uint8_t operator() (const uint8_t& key) const { return key; }
};
using CacheMap = tbb::concurrent_unordered_map<uint8_t, CacheNodeValue, no_hash>;


/*
  node: [header, records]
*/
class CacheNode {
public:
  volatile CacheHeader* header;
  CacheMap records;  // value is const

  CacheNode() {
    header = new CacheHeader();
  }

  // insert leaf node
  CacheNode(const std::vector<uint8_t>& byte_array, int start, CacheEntry* new_entry) {
    header = new CacheHeader(byte_array, start, byte_array.size() - start - 1);
    records[byte_array.back()] = CacheNodeValue(new_entry, nullptr);
  }

  // split internal node
  CacheNode(const std::vector<uint8_t>& byte_array, int start, int partial_len,
            uint8_t partial_1, CacheNode* next_node, uint8_t partial_2, CacheEntry* new_entry, CacheNode* &nested_node) {
    header = new CacheHeader(byte_array, start, partial_len);
    if (partial_1 == partial_2) {  // split for insert new_entry at old header
      records[partial_1] = CacheNodeValue(new_entry, next_node);
    }
    else {
      records[partial_1] = CacheNodeValue(nullptr, next_node);
      if (start + partial_len >= (int)byte_array.size() - 1) {  // insert entry directly
        nested_node = nullptr;
        records[partial_2] = CacheNodeValue(new_entry, nullptr);
      }
      else {  // insert leaf node
        nested_node = new CacheNode(byte_array, start + partial_len + 1, new_entry);
        records[partial_2] = CacheNodeValue(nullptr, nested_node);
      }
    }
  }

  uint64_t content_size() const {
    return ((CacheHeader *)header)->content_size() + (sizeof(uint8_t) + sizeof(CacheEntry *) + sizeof(CacheNode *)) * records.size();
  }

  ~CacheNode() { delete header; }
};


/*
  This class is used to calculate the cache memory consumption
  so as to trigger eviction.
*/
class FreeMemManager {
public:
  FreeMemManager(int64_t free_size) : free_size(free_size) {}

  void consume(int _size) {
    // free_size -= _size;
    free_size.fetch_add(-_size);
  }

  using NodeSizeMap = tbb::concurrent_hash_map<CacheNode*, uint64_t>;
  void consume_by_node(CacheNode* node) {
#ifdef CACHE_ENABLE_ART
    auto new_size = node->content_size();
    NodeSizeMap::accessor w_entry;
    auto new_inserted = node_mem_size.insert(w_entry, node);
    auto old_size = new_inserted ? 0 : w_entry->second;
    if (new_size == old_size) {
      return;
    }
    w_entry->second = new_size;
    // free_size -= new_size - old_size;
    free_size.fetch_add(-new_size + old_size);
#else
    return;  // emulate normal cache
#endif
  }

  void free(int _size) {
    free_size.fetch_add(_size);
  }

  int64_t remain_size() const {
    return free_size.load();
  }

private:
  std::atomic<int64_t> free_size;
  NodeSizeMap node_mem_size;
};


struct SearchRet {
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  int next_idx;
  // uint64_t counter;
  SearchRet() {}
  SearchRet(volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr, int next_idx) :
    entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr), next_idx(next_idx) {}
};


class RadixCache {

public:
  RadixCache(int cache_size, DSM *dsm);

  void add_to_cache(const Key& k, const InternalPage* p_node, const GlobalAddress &node_addr);

  bool search_from_cache(const Key& k, volatile CacheEntry**& entry_ptr_ptr, CacheEntry*& entry_ptr, int& entry_idx);
  void search_range_from_cache(const Key &from, const Key &to, std::vector<RangeCache> &result);
  void invalidate(volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr);
  void statistics();

private:
  void _insert(const CacheKey& byte_array, CacheEntry* new_entry);

  using SearchRetStk = std::stack<SearchRet>;
  bool _search(const CacheKey& byte_array, SearchRetStk& ret);
  // bool _random_search(SearchRetStk& ret);

  void _evict();
  // void _evict_one();
  void _safely_delete(CacheEntry* cache_entry);
  void _safely_delete(CacheHeader* cache_hdr);

private:
  // Cache
  uint64_t cache_size; // MB
  FreeMemManager* free_manager;
  CacheNode* cache_root;
  tbb::concurrent_queue<CacheNode*>* node_queue;

  // GC
  tbb::concurrent_queue<CacheEntry*> cache_entry_gc;
  tbb::concurrent_queue<CacheHeader*> cache_hdr_gc;
  static const int safely_free_epoch = 2 * MAX_APP_THREAD * MAX_CORO_NUM;

  // FIFIO Eviction
  DSM *dsm;
  tbb::concurrent_queue<std::pair<volatile CacheEntry**, CacheEntry*> > eviction_list;
};

#endif // _RADIX_CACHE_H_
