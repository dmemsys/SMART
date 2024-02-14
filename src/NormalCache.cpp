#include "NormalCache.h"

#include <random>
#include <queue>
#include <vector>
#include <set>


/*
  Normal kv entry cache, realize by concurrent_unordered_map. [key prefix -> cache entry]
*/
NormalCache::NormalCache(int cache_size, DSM *dsm) : cache_size(cache_size), dsm(dsm) {
  free_size = define::MB * cache_size;
  // map_size = 0;
}

void NormalCache::add_to_cache(const Key& k, const InternalPage* p_node, const GlobalAddress &node_addr) {
  auto depth = p_node->hdr.depth - 1;

  std::vector<uint8_t> byte_array(k.begin(), k.begin() + depth);
  for (int i = 0; i < (int)p_node->hdr.partial_len; ++ i) byte_array.push_back(p_node->hdr.partial[i]);

  auto new_entry = new CacheEntry(p_node, node_addr);
  _insert(byte_array, new_entry);
  if (free_size < 0) {
    _evict();
  }
  return;
}

void NormalCache::_insert(const CacheKey& byte_array, CacheEntry* new_entry) {
  if (cache_map.find(byte_array) == cache_map.end()) {
    keys.push(byte_array);
  }
  auto old_entry = (CacheEntry *)cache_map[byte_array];

  if (__sync_bool_compare_and_swap(&(cache_map[byte_array]), old_entry, new_entry)) {
    free_size.fetch_add(-sizeof(CacheEntry*) - new_entry->content_size());
    if (old_entry) {
      free_size.fetch_add(old_entry->content_size());
      _safely_delete(old_entry);
    }
    else {
      free_size.fetch_add(-sizeof(Key));
    }
    eviction_list.push(std::make_pair(&(cache_map[byte_array]), new_entry));
// retry:
//     // calculate free_size consume by key
//     auto old_map_size = map_size.load();
//     auto new_map_size = cache_map.size() * (sizeof(Key));
//     auto cur_free_size = free_size.load();
//     if (new_map_size != old_map_size) {
//       if (free_size.compare_exchange_weak(cur_free_size, cur_free_size - (new_map_size - old_map_size))) {
//         map_size = new_map_size;
//         return;
//       }
//       goto retry;
//     }
  }
  else {
    delete new_entry;
  }
}


bool NormalCache::search_from_cache(const Key& k, volatile CacheEntry**& entry_ptr_ptr, CacheEntry*& entry_ptr, int& entry_idx) {
  CacheKey byte_array(k.begin(), k.begin() + define::keyLen - 1);

  return _search(byte_array, k.back(), entry_ptr_ptr, entry_ptr, entry_idx);
}

bool NormalCache::_search(CacheKey& byte_prefix, uint8_t last_byte, volatile CacheEntry**& entry_ptr_ptr, CacheEntry*& entry_ptr, int& entry_idx) {
try_upper:
  auto r_entry = cache_map.find(byte_prefix);
  if (r_entry != cache_map.end() && (entry_ptr = (CacheEntry *)r_entry->second)) {
    for (int i = 0; i < (int)entry_ptr->records.size(); ++ i) {
      const auto& e = entry_ptr->records[i];
      if (e != InternalEntry::Null() && e.partial == last_byte) {
        // __sync_fetch_and_add(&(entry_ptr->counter), 1UL);
        entry_ptr_ptr = &(r_entry->second);
        entry_idx = i;
        return true;
      }
    }
  }
  if (!byte_prefix.empty()) {
    last_byte = byte_prefix.back();
    byte_prefix.pop_back();
    goto try_upper;
  }
  return false;
}


void NormalCache::search_range_from_cache(const Key &from, const Key &to, std::vector<RangeCache> &result) {
  GlobalAddress p_ptr;
  InternalEntry p;
  int depth;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;

  for (auto k = from; k < to; k = k + 1) {
    auto e = search_from_cache(k, entry_ptr_ptr, entry_ptr, entry_idx);
    if (e) {
      assert(entry_idx >= 0);
      p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
      p = entry_ptr->records[entry_idx];
      depth = entry_ptr->depth;

      auto leftmost = p.is_leaf ? k : get_leftmost(k, depth);
      auto rightmost = p.is_leaf ? k : get_rightmost(k, depth);
      result.push_back(RangeCache(leftmost, rightmost, p_ptr, p, depth, entry_ptr_ptr, entry_ptr));
    }
  }
  return;
}

void NormalCache::invalidate(volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr) {
  if (entry_ptr_ptr && entry_ptr && __sync_bool_compare_and_swap(entry_ptr_ptr, entry_ptr, 0UL)) {
    free_size.fetch_add(sizeof(CacheEntry*) + entry_ptr->content_size() + sizeof(Key));
    _safely_delete(entry_ptr);
  }
}

void NormalCache::_evict() {
  do {
    // _evict_one();
    std::pair<volatile CacheEntry**, CacheEntry*> next;
    if(eviction_list.try_pop(next) && *next.first == next.second) {
      invalidate(next.first, next.second);
    }
  } while (free_size.load() < 0);
  // do {
  //   _evict_one();
  // } while (free_size.load() < 0);
}

// void NormalCache::_evict_one() {
//   volatile CacheEntry** entry_ptr_ptr_1;
//   volatile CacheEntry** entry_ptr_ptr_2;
//   CacheEntry* entry_ptr_1;
//   CacheEntry* entry_ptr_2;

//   _get_a_random_entry(entry_ptr_ptr_1, entry_ptr_1);
//   _get_a_random_entry(entry_ptr_ptr_2, entry_ptr_2);

//   if (entry_ptr_1->counter < entry_ptr_2->counter) {
//     invalidate(entry_ptr_ptr_1, entry_ptr_1);
//   } else {
//     invalidate(entry_ptr_ptr_2, entry_ptr_2);
//   }
// }

// void NormalCache::_get_a_random_entry(volatile CacheEntry** &entry_ptr_ptr, CacheEntry* &entry_ptr) {
// retry:
//   auto k = dsm->getRandomKey();
//   CacheKey byte_array(k.begin(), k.begin() + define::keyLen - 1);

// try_upper:
//   auto r_entry = cache_map.find(byte_array);
//   if (r_entry != cache_map.end() && (entry_ptr = (CacheEntry *)r_entry->second)) {
//     entry_ptr_ptr = &(r_entry->second);
//     return;
//   }
//   if (!byte_array.empty()) {
//     byte_array.pop_back();
//     goto try_upper;
//   }

//   goto retry;
// }

void NormalCache::_safely_delete(CacheEntry* cache_entry) {
  cache_entry_gc.push(cache_entry);
  while (cache_entry_gc.unsafe_size() > safely_free_epoch) {
    CacheEntry* next;
    if (cache_entry_gc.try_pop(next)) {
      delete next;
    }
  }
}

void NormalCache::statistics() {
  std::cout << " ----- [IndexCache]: " << " cache size=" << cache_size << " MB"
                                       << " free_size=" << free_size / define::MB << " MB" 
                                       <<  " ----- " << std::endl;
  std::map<int, int64_t> cnt;
  uint64_t kp_cnt = 0;
  for (auto entry_iter = cache_map.begin(); entry_iter != cache_map.end(); ++ entry_iter) {
    auto cache_entry = (CacheEntry *)entry_iter->second;
    if (cache_entry) {
      int depth = cache_entry->depth;
      if (cnt.find(depth) == cnt.end()) cnt[depth] = 0;
      cnt[depth] ++;
      kp_cnt += cache_entry->records.size();
    }
  }
  for (const auto& e : cnt) {
    std::cout << "depth=" << e.first << " cnt=" << e.second << std::endl;
  }
  printf("consumed cache size = %.3lf MB\n", (double)cache_size - (double)free_size / define::MB);
}
