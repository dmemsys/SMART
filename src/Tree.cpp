#include "Tree.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "Node.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <atomic>
#include <mutex>


double cache_miss[MAX_APP_THREAD];
double cache_hit[MAX_APP_THREAD];
uint64_t lock_fail[MAX_APP_THREAD];
// uint64_t try_lock[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_cache_invalid[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
uint64_t read_node_repair[MAX_APP_THREAD];
uint64_t try_read_node[MAX_APP_THREAD];
uint64_t read_node_type[MAX_APP_THREAD][MAX_NODE_TYPE_NUM];
uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
volatile bool need_stop = false;
uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];

thread_local CoroCall Tree::worker[MAX_CORO_NUM];
thread_local CoroCall Tree::master;
thread_local CoroQueue Tree::busy_waiting_queue;


Tree::Tree(DSM *dsm, uint16_t tree_id) : dsm(dsm), tree_id(tree_id) {
  assert(dsm->is_register());

#ifdef TREE_ENABLE_CACHE
  // init local cache
// #ifdef CACHE_ENABLE_ART
  index_cache = new RadixCache(define::kIndexCacheSize, dsm);
// #else
//   index_cache = new NormalCache(define::kIndexCacheSize, dsm);
// #endif
#endif

  local_lock_table = new LocalLockTable();

  root_ptr_ptr = get_root_ptr_ptr();

  // init root entry to Null
  auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
  dsm->read_sync((char *)entry_buffer, root_ptr_ptr, sizeof(InternalEntry));
  auto root_ptr = *(InternalEntry *)entry_buffer;
  if (dsm->getMyNodeID() == 0 && root_ptr != InternalEntry::Null()) {
    auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
retry:
    bool res = dsm->cas_sync(root_ptr_ptr, (uint64_t)root_ptr, (uint64_t)InternalEntry::Null(), cas_buffer);
    if (!res && (root_ptr = *(InternalEntry *)cas_buffer) != InternalEntry::Null()) {
      goto retry;
    }
  }
}


GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset = define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;
  return addr;
}


InternalEntry Tree::get_root_ptr(CoroContext *cxt, int coro_id) {
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  dsm->read_sync((char *)entry_buffer, root_ptr_ptr, sizeof(InternalEntry), cxt);
  return *(InternalEntry *)entry_buffer;
}


void Tree::insert(const Key &k, Value v, CoroContext *cxt, int coro_id, bool is_update, bool is_load) {
  assert(dsm->is_register());

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

  // traversal
  GlobalAddress p_ptr;
  InternalEntry p;
  GlobalAddress node_ptr;  // node address(excluding header)
  int depth;
  int retry_flag = FIRST_TRY;

  // cache
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;
  int cache_depth = 0;

  // temp
  GlobalAddress leaf_addr = GlobalAddress::Null();
  char* page_buffer;
  bool is_valid, type_correct;
  InternalPage* p_node = nullptr;
  Header hdr;
  int max_num;
  uint64_t* cas_buffer;
  int debug_cnt = 0;

#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, cxt, coro_id);
  write_handover = (lock_res.first && !lock_res.second);
#endif
  try_write_op[dsm->getMyThreadID()]++;
  if (write_handover) {
    write_handover_num[dsm->getMyThreadID()]++;
    goto insert_finish;
  }

  // search local cache
#ifdef TREE_ENABLE_CACHE
  from_cache = index_cache->search_from_cache(k, entry_ptr_ptr, entry_ptr, entry_idx);
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    node_ptr = entry_ptr->addr;
    depth = entry_ptr->depth;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }
#else
  p_ptr = root_ptr_ptr;
  p = get_root_ptr(cxt, coro_id);
  node_ptr = root_ptr_ptr;
  depth = 0;
#endif
  depth ++;  // partial key in entry is matched
  cache_depth = depth;

#ifdef TREE_TEST_ROWEX_ART
  if (!is_update) lock_node(node_ptr, cxt, coro_id);
#else
  UNUSED(is_update);  // is_update is only used in ROWEX_ART baseline
#endif

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;

  // 1. If we are at a NULL node, inject a leaf
  if (p == InternalEntry::Null()) {
    assert(from_cache == false);
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    bool res = out_of_place_write_leaf(k, v, depth, leaf_addr, get_partial(k, depth-1), p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);
    // cas fail, retry
    if (!res) {
      p = *(InternalEntry*) cas_buffer;
      retry_flag = CAS_NULL;
      goto next;
    }
    goto insert_finish;
  }

  // 2. If we are at a leaf, we need to update it / replace it with a node
  if (p.is_leaf) {
    // 2.1 read the leaf
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
    is_valid = read_leaf(p.addr(), leaf_buffer, std::max((unsigned long)p.kv_len, sizeof(Leaf)), p_ptr, from_cache, cxt, coro_id);

    if (!is_valid) {
#ifdef TREE_ENABLE_CACHE
      // invalidate the old leaf entry cache
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
#endif
      // re-read leaf entry
      auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
      dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
      p = *(InternalEntry *)entry_buffer;
      from_cache = false;
      retry_flag = INVALID_LEAF;
      goto next;
    }

    auto leaf = (Leaf *)leaf_buffer;
    auto _k = leaf->get_key();

    // 2.2 Check if we are updating an existing key
    if (_k == k) {
      if (is_load) {
        goto insert_finish;
      }
      // Check if the key no need to update
#ifdef TREE_ENABLE_WRITE_COMBINING
      local_lock_table->get_combining_value(k, v);
#endif
      if (leaf->get_value() == v) {
        goto insert_finish;
      }
#ifdef TREE_ENABLE_IN_PLACE_UPDATE
      // in place update leaf
      in_place_update_leaf(k, v, p.addr(), leaf, cxt, coro_id);
#else
      // out of place update leaf
      bool res = out_of_place_update_leaf(k, v, depth, leaf_addr, p_ptr, p, node_ptr, cxt, coro_id, !is_update);
#ifdef TREE_ENABLE_CACHE
      // invalidate the old leaf entry cache
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
#endif
      if (!res) {
        lock_fail[dsm->getMyThreadID()] ++;
        if (++ debug_cnt > 50) {
          // TODO retry too many times, restart...
          p_ptr = root_ptr_ptr;
          p = get_root_ptr(cxt, coro_id);
          node_ptr = root_ptr_ptr;
          cache_depth = depth = 1;
          // debug_cnt = 0;
        }
        from_cache = false;
        retry_flag = CAS_LEAF;
        goto next;
      }
#endif
      goto insert_finish;
    }

    // 2.3 New key, we must merge the two leaves into a node (leaf split)
    int partial_len = longest_common_prefix(_k, k, depth);
    uint8_t diff_partial = get_partial(_k, depth + partial_len);
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    bool res = out_of_place_write_node(k, v, depth, leaf_addr, partial_len, diff_partial, p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);
    // cas fail, retry
    if (!res) {
      p = *(InternalEntry*) cas_buffer;
      retry_flag = CAS_LEAF;
      goto next;
    }
    goto insert_finish;
  }

  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
#ifdef TREE_ENABLE_CACHE
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
#endif
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_NODE;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
#ifdef TREE_ENABLE_CACHE
  if (from_cache && !type_correct) {  // invalidate the out dated node type
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
  if (depth == hdr.depth) {
    index_cache->add_to_cache(k, p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }
#else
  UNUSED(type_correct);
#endif

  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      // need split
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      int partial_len = hdr.depth + i - depth;  // hdr.depth may be outdated, so use partial_len wrt. depth
      bool res = out_of_place_write_node(k, v, depth, leaf_addr, partial_len, hdr.partial[i], p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);
      // cas fail, retry
      if (!res) {
        p = *(InternalEntry*) cas_buffer;
        retry_flag = SPLIT_HEADER;
        goto next;
      }
#ifdef TREE_ENABLE_CACHE
      // invalidate cache node due to outdated cache entry in cache node
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
#endif
      // udpate cas header. Optimization: no need to snyc; mask node_type
      auto header_buffer = (dsm->get_rbuf(coro_id)).get_header_buffer();
      auto new_hdr = Header::split_header(hdr, i);
      dsm->cas_mask(GADD(p.addr(), sizeof(GlobalAddress)), (uint64_t)hdr, (uint64_t)new_hdr, header_buffer, ~Header::node_type_mask, false, cxt);
      goto insert_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;
#ifdef TREE_TEST_ROWEX_ART
  if (!is_update) unlock_node(node_ptr, cxt, coro_id);
  node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
  if (!is_update) lock_node(node_ptr, cxt, coro_id);
#else
  node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
#endif

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type());
  // search a exists slot first
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, depth)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      retry_flag = FIND_NEXT;
      goto next;  // search next level
    }
  }
  // if no match slot, then find an empty slot to insert leaf directly
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e == InternalEntry::Null()) {
      auto e_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      bool res = out_of_place_write_leaf(k, v, depth + 1, leaf_addr, get_partial(k, depth), e_ptr, old_e, node_ptr, cas_buffer, cxt, coro_id);
      // cas success, return
      if (res) {
        goto insert_finish;
      }
      // cas fail, check
      else {
        auto e = *(InternalEntry*) cas_buffer;
        if (e.partial == get_partial(k, depth)) {  // same partial keys insert to the same empty slot
          p_ptr = e_ptr;
          p = e;
          from_cache = false;
          depth ++;
          retry_flag = CAS_EMPTY;
          goto next;  // search next level
        }
      }
    }
  }

#ifdef TREE_ENABLE_ART
  // 3.4 node is full, switch node type
  int slot_id;
  cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  if (insert_behind(k, v, depth + 1, leaf_addr, get_partial(k, depth), p.type(), node_ptr, cas_buffer, slot_id, cxt, coro_id)){  // insert success
    auto next_type = num_to_node_type(slot_id);
    cas_node_type(next_type, p_ptr, p, hdr, cxt, coro_id);
#ifdef TREE_ENABLE_CACHE
    if (from_cache) {  // cache is outdated since node type is changed
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
#endif
    goto insert_finish;
  }
  else {  // same partial keys insert to the same empty slot
    p_ptr = GADD(node_ptr, slot_id * sizeof(InternalEntry));
    p = *(InternalEntry*) cas_buffer;
    from_cache = false;
    depth ++;
    retry_flag = INSERT_BEHIND_EMPTY;
    goto next;
  }
#else
  assert(false);
#endif

insert_finish:
#ifdef TREE_TEST_ROWEX_ART
  if (!is_update) unlock_node(node_ptr, cxt, coro_id);
#endif
#ifdef TREE_ENABLE_CACHE
  if (!write_handover) {
    auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
    cache_hit[dsm->getMyThreadID()] += hit;
    cache_miss[dsm->getMyThreadID()] += (1 - hit);
  }
#endif
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->release_local_write_lock(k, lock_res);
#endif
  return;
}


bool Tree::read_leaf(const GlobalAddress &leaf_addr, char *leaf_buffer, int leaf_size, const GlobalAddress &p_ptr, bool from_cache, CoroContext *cxt, int coro_id) {
  try_read_leaf[dsm->getMyThreadID()] ++;
re_read:
  dsm->read_sync(leaf_buffer, leaf_addr, leaf_size, cxt);
  auto leaf = (Leaf *)leaf_buffer;
  // udpate reverse pointer if needed
  if (!from_cache && leaf->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(leaf_addr, leaf->rev_ptr, p_ptr, cas_buffer, false, cxt);
    // dsm->cas_sync(leaf_addr, leaf->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  // invalidation
  if (!leaf->is_valid(p_ptr, from_cache)) {
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
  if (!leaf->is_consistent()) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
  return true;
}


void Tree::in_place_update_leaf(const Key &k, Value &v, const GlobalAddress &leaf_addr, Leaf* leaf,
                               CoroContext *cxt, int coro_id) {
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  static const uint64_t lock_cas_offset = ROUND_DOWN(STRUCT_OFFSET(Leaf, lock_byte), 3);
  static const uint64_t lock_mask       = 1UL << ((STRUCT_OFFSET(Leaf, lock_byte) - lock_cas_offset) * 8);
#endif

  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &unique_leaf_addr) {
#ifdef TREE_ENABLE_EMBEDDING_LOCK
    return dsm->cas_mask_sync(GADD(unique_leaf_addr, lock_cas_offset), 0UL, ~0UL, cas_buffer, lock_mask, cxt);
#else
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_leaf_addr, lock_addr, mask);
    return dsm->cas_dm_mask_sync(lock_addr, 0UL, ~0UL, cas_buffer, mask, cxt);
#endif
  };

  // unlock function
  auto unlock = [=](const GlobalAddress &unique_leaf_addr){
#ifdef TREE_ENABLE_EMBEDDING_LOCK
    dsm->cas_mask_sync(GADD(unique_leaf_addr, lock_cas_offset), ~0UL, 0UL, cas_buffer, lock_mask, cxt);
#else
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_leaf_addr, lock_addr, mask);
    dsm->cas_dm_mask_sync(lock_addr, ~0UL, 0UL, cas_buffer, mask, cxt);
#endif
  };

  // start lock & write & unlock
  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write w/o unlock
  auto write_without_unlock = [=](const GlobalAddress &unique_leaf_addr){
    dsm->write_sync((const char*)leaf, unique_leaf_addr, sizeof(Leaf), cxt);
  };
  // write and unlock
  auto write_and_unlock = [=](const GlobalAddress &unique_leaf_addr){
    leaf->unlock();
    dsm->write_sync((const char*)leaf, unique_leaf_addr, sizeof(Leaf), cxt);
  };
#endif

  lock_handover = local_lock_table->acquire_local_lock(leaf_addr, &busy_waiting_queue, cxt, coro_id);
#endif
  if (lock_handover) {
    goto write_leaf;
  }
  // try_lock[dsm->getMyThreadID()] ++;

re_acquire:
  if (!acquire_lock(leaf_addr)){
    if (cxt != nullptr) {
      busy_waiting_queue.push(std::make_pair(coro_id, [](){ return true; }));
      (*cxt->yield)(*cxt->master);
    }
    lock_fail[dsm->getMyThreadID()] ++;
    goto re_acquire;
  }

write_leaf:
#ifdef TREE_TEST_HOCL_HANDOVER
  // in-place write leaf & unlock
  assert(leaf->get_key() == k);
  leaf->set_value(v);
  leaf->set_consistent();
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write back the lock at the same time
  local_lock_table->release_local_lock(leaf_addr, unlock, write_without_unlock, write_and_unlock);
#else
  dsm->write_sync((const char*)leaf, leaf_addr, sizeof(Leaf), cxt);
  local_lock_table->release_local_lock(leaf_addr, unlock);
#endif

#else
  UNUSED(unlock);
  // in-place write leaf & unlock
  assert(leaf->get_key() == k);
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->get_combining_value(k, v);
#endif
  leaf->set_value(v);
  leaf->set_consistent();
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write back the lock at the same time
  leaf->unlock();
  dsm->write_sync((const char*)leaf, leaf_addr, sizeof(Leaf), cxt);
#else
  // batch write updated leaf and on-chip lock
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)leaf;
  rs[0].dest = leaf_addr;
  rs[0].size = sizeof(Leaf);
  rs[0].is_on_chip = false;
  GlobalAddress lock_addr;
  uint64_t mask;
  get_on_chip_lock_addr(leaf_addr, lock_addr, mask);
  rs[1].source = (uint64_t)cas_buffer;  // unlock
  rs[1].dest = lock_addr;
  rs[1].is_on_chip = true;
  dsm->write_cas_mask_sync(rs[0], rs[1], ~0UL, 0UL, mask, cxt);
#endif
#endif
  return;
}


bool Tree::out_of_place_update_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, const GlobalAddress &e_ptr, InternalEntry &old_e, const GlobalAddress& node_addr,
                                    CoroContext *cxt, int coro_id, bool disable_handover) {
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  bool res = false;

  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
  if (!disable_handover) {
    lock_handover = local_lock_table->acquire_local_lock(k, &busy_waiting_queue, cxt, coro_id);
  }
#endif
  if (lock_handover) {
    goto update_finish;
  }
  // try_lock[dsm->getMyThreadID()] ++;
  res = out_of_place_write_leaf(k, v, depth, leaf_addr, old_e.partial, e_ptr, old_e, node_addr, cas_buffer, cxt, coro_id);
  if (res) {
    // invalid the old leaf
    auto zero_byte = (dsm->get_rbuf(coro_id)).get_zero_byte();
    dsm->write(zero_byte, GADD(old_e.addr(), STRUCT_OFFSET(Leaf, valid_byte)), sizeof(uint8_t), false, cxt);
  }
  else {
    old_e = *(InternalEntry*) cas_buffer;
  }
update_finish:
#ifdef TREE_TEST_HOCL_HANDOVER
  if (!disable_handover) {
    local_lock_table->release_local_lock(k, res, old_e);
  }
#endif
  return res;
}


void Tree::get_on_chip_lock_addr(const GlobalAddress &leaf_addr, GlobalAddress &lock_addr, uint64_t &mask) {
  auto leaf_offset = leaf_addr.offset;
  auto lock_index = CityHash64((char *)&leaf_offset, sizeof(leaf_offset)) % define::kOnChipLockNum;
  lock_addr.nodeID = leaf_addr.nodeID;
  lock_addr.offset = lock_index / 64 * sizeof(uint64_t);
  mask = 1UL << (lock_index % 64);
}

#ifdef TREE_TEST_ROWEX_ART
void Tree::lock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id) {
  // HOCL
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &unique_node_addr) {
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_node_addr, lock_addr, mask);
    return dsm->cas_dm_mask_sync(lock_addr, 0UL, ~0UL, cas_buffer, mask, cxt);
  };

  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
  lock_handover = local_lock_table->acquire_local_lock(node_addr, &busy_waiting_queue, cxt, coro_id);
#endif
  if (lock_handover) {
    return;
  }
  // try_lock[dsm->getMyThreadID()] ++;
re_acquire:
  if (!acquire_lock(node_addr)){
    if (cxt != nullptr) {
      busy_waiting_queue.push(std::make_pair(coro_id, [](){ return true; }));
      (*cxt->yield)(*cxt->master);
    }
    lock_fail[dsm->getMyThreadID()] ++;
    goto re_acquire;
  }
  return;
}

void Tree::unlock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id) {
  // HOCL
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // unlock function
  auto unlock = [=](const GlobalAddress &unique_node_addr){
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_node_addr, lock_addr, mask);
    dsm->cas_dm_mask_sync(lock_addr, ~0UL, 0UL, cas_buffer, mask, cxt);
  };

#ifdef TREE_TEST_HOCL_HANDOVER
  local_lock_table->release_local_lock(node_addr, unlock);
#else
  unlock(node_addr);
#endif
  return;
}
#endif

bool Tree::out_of_place_write_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, uint8_t partial_key,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr, uint64_t *ret_buffer,
                                   CoroContext *cxt, int coro_id) {
  bool unwrite = leaf_addr == GlobalAddress::Null();
#ifdef TREE_ENABLE_WRITE_COMBINING
  if (local_lock_table->get_combining_value(k, v)) unwrite = true;
#endif
  // allocate & write
  if (unwrite) {  // !ONLY allocate once
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
    new (leaf_buffer) Leaf(k, v, e_ptr);
    leaf_addr = dsm->alloc(sizeof(Leaf));
    dsm->write_sync(leaf_buffer, leaf_addr, sizeof(Leaf), cxt);
  }
  else {  // write the changed e_ptr inside leaf
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }

  // cas entry
  auto new_e = InternalEntry(partial_key, sizeof(Leaf) < 128 ? sizeof(Leaf) : 0, leaf_addr);
  auto remote_cas = [=](){
    return dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
  };

// #ifndef TREE_TEST_ROWEX_ART
  return remote_cas();
// #else
//   return lock_and_cas_in_node(node_addr, remote_cas, cxt, coro_id);
// #endif
}


bool Tree::read_node(InternalEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                     CoroContext *cxt, int coro_id) {
  auto read_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(p.type()) * sizeof(InternalEntry);
  dsm->read_sync(node_buffer, p.addr(), read_size, cxt);
  auto p_node = (InternalPage *)node_buffer;
  auto& hdr = p_node->hdr;

  read_node_type[dsm->getMyThreadID()][hdr.type()] ++;
  try_read_node[dsm->getMyThreadID()] ++;

  if (hdr.node_type != p.node_type) {
    if (hdr.node_type > p.node_type) {  // need to read the rest part
      read_node_repair[dsm->getMyThreadID()] ++;
      auto remain_size = (node_type_to_num(hdr.type()) - node_type_to_num(p.type())) * sizeof(InternalEntry);
      dsm->read_sync(node_buffer + read_size, GADD(p.addr(), read_size), remain_size, cxt);
    }
    p.node_type = hdr.node_type;
    type_correct = false;
  }
  else type_correct = true;
  // udpate reverse pointer if needed
  if (!from_cache && p_node->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, false, cxt);
    // dsm->cas_sync(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  return p_node->is_valid(p_ptr, depth, from_cache);
}


bool Tree::out_of_place_write_node(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int partial_len, uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;
  auto leaf_unwrite = (leaf_addr == GlobalAddress::Null());

  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  dsm->alloc_nodes(new_node_num, node_addrs);

  // allocate & write new leaf
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
  auto leaf_e_ptr = GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header) + sizeof(InternalEntry) * 1);
#ifdef TREE_ENABLE_WRITE_COMBINING
  if (local_lock_table->get_combining_value(k, v)) leaf_unwrite = true;
#endif
  if (leaf_unwrite) {  // !ONLY allocate once
    new (leaf_buffer) Leaf(k, v, leaf_e_ptr);
    leaf_addr = dsm->alloc(sizeof(Leaf));
  }
  else {  // write the changed e_ptr inside new leaf  TODO: batch
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = leaf_e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }

  // init inner nodes
  NodeType nodes_type = num_to_node_type(2);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num - 1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }

  // insert the two leaf into the last node
  auto node_buffer  = (dsm->get_rbuf(coro_id)).get_page_buffer();
  node_pages[new_node_num - 1] = new (node_buffer) InternalPage(k, partial_len, depth, nodes_type, rev_ptr);
  node_pages[new_node_num - 1]->records[0] = InternalEntry(diff_partial, old_e);
  node_pages[new_node_num - 1]->records[1] = InternalEntry(get_partial(k, depth + partial_len),
                                                           sizeof(Leaf) < 128 ? sizeof(Leaf) : 0, leaf_addr);

  // init the parent entry
  auto new_e = InternalEntry(old_e.partial, nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 1];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  if (leaf_unwrite) {
    rs[new_node_num].source     = (uint64_t)leaf_buffer;
    rs[new_node_num].dest       = leaf_addr;
    rs[new_node_num].size       = sizeof(Leaf);
    rs[new_node_num].is_on_chip = false;
  }
  dsm->write_batches_sync(rs, (leaf_unwrite ? new_node_num + 1 : new_node_num), cxt, coro_id);

  // cas
  auto remote_cas = [=](){
    return dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
  };
// #ifndef TREE_TEST_ROWEX_ART
  bool res = remote_cas();
// #else
//   bool res = lock_and_cas_in_node(node_addr, remote_cas, cxt, coro_id);
// #endif
  if (!res) reclaim_memory();

  // cas the updated rev_ptr inside old leaf / old node
  if (res) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(old_e.addr(), e_ptr, GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header)), cas_buffer, false, cxt);
  }


#ifdef TREE_ENABLE_CACHE
  if (res) {
    for (int i = 0; i < new_node_num; ++ i) {
      index_cache->add_to_cache(k, node_pages[i], GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header)));
    }
  }
#endif
  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}


void Tree::cas_node_type(NodeType next_type, GlobalAddress p_ptr, InternalEntry p, Header hdr,
                         CoroContext *cxt, int coro_id) {
  auto node_addr = p.addr();
  auto header_addr = GADD(node_addr, sizeof(GlobalAddress));
  auto cas_buffer_1 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto cas_buffer_2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  std::pair<bool, bool> res = std::make_pair(false, false);

  // batch cas old_entry & node header to change node type
  auto remote_cas_both = [=, &p_ptr, &p, &hdr](){
    auto new_e = InternalEntry(next_type, p);
    RdmaOpRegion rs[2];
    rs[0].source     = (uint64_t)cas_buffer_1;
    rs[0].dest       = p_ptr;
    rs[0].is_on_chip = false;
    rs[1].source     = (uint64_t)cas_buffer_2;
    rs[1].dest       = header_addr;
    rs[1].is_on_chip = false;
    return dsm->two_cas_mask_sync(rs[0], (uint64_t)p, (uint64_t)new_e, ~0UL,
                                  rs[1], hdr, Header(next_type), Header::node_type_mask, cxt);
  };

  // only cas old_entry
  auto remote_cas_entry = [=, &p_ptr, &p](){
    auto new_e = InternalEntry(next_type, p);
    return dsm->cas_sync(p_ptr, (uint64_t)p, (uint64_t)new_e, cas_buffer_1, cxt);
  };

  // only cas node_header
  auto remote_cas_header = [=, &hdr](){
    return dsm->cas_mask_sync(header_addr, hdr, Header(next_type), cas_buffer_2, Header::node_type_mask, cxt);
  };

  // read down to find target entry when split
  auto read_first_entry = [=, &p_ptr, &p](){
    p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
  };

re_switch:
  auto old_res = res;
  if (!old_res.first && !old_res.second) {
    res = remote_cas_both();
  }
  else {
    if (!old_res.first)  res.first  = remote_cas_entry();
    if (!old_res.second) res.second = remote_cas_header();
  }
  if (!res.first) {
    p = *(InternalEntry *)cas_buffer_1;
    // handle the conflict when switch & split/delete happen at the same time
    while (p != InternalEntry::Null() && !p.is_leaf && p.addr() != node_addr) {
      read_first_entry();
      retry_cnt[dsm->getMyThreadID()][SWITCH_FIND_TARGET] ++;
    }
    if (p.addr() != node_addr || p.type() >= next_type) res.first = true;  // no need to retry
  }
  if (!res.second) {
    hdr = *(Header *)cas_buffer_2;
    if (hdr.type() >= next_type) res.second = true;  // no need to retry
  }
  if (!res.first || !res.second) {
    retry_cnt[dsm->getMyThreadID()][SWITCH_RETRY] ++;
    goto re_switch;
  }
}


bool Tree::insert_behind(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, uint8_t partial_key, NodeType node_type,
                         const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                         CoroContext *cxt, int coro_id) {
  int max_num, i;
  assert(node_type != NODE_256);
  max_num = node_type_to_num(node_type);
  // try cas an empty slot
  for (i = 0; i < 256 - max_num; ++ i) {
    auto slot_id = max_num + i;
    GlobalAddress e_ptr = GADD(node_addr, slot_id * sizeof(InternalEntry));
    bool res = out_of_place_write_leaf(k, v, depth, leaf_addr, partial_key, e_ptr, InternalEntry::Null(), node_addr, ret_buffer, cxt, coro_id);
    // cas success, return to switch node type
    if (res) {
      inserted_idx = slot_id;
      return true;
    }
    // cas fail, check
    else {
      auto e = *(InternalEntry*) ret_buffer;
      if (e.partial == partial_key) {  // same partial keys insert to the same empty slot
        inserted_idx = slot_id;
        return false;  // search next level
      }
    }
    retry_cnt[dsm->getMyThreadID()][INSERT_BEHIND_TRY_NEXT] ++;
  }
  assert(false);
}


bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  // handover
  bool search_res = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  bool read_handover = false;

  // traversal
  GlobalAddress p_ptr;
  InternalEntry p;
  int depth;
  int retry_flag = FIRST_TRY;

  // cache
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;
  int cache_depth = 0;

  // temp
  char* page_buffer;
  bool is_valid, type_correct;
  InternalPage* p_node = nullptr;
  Header hdr;
  int max_num;

#ifdef TREE_ENABLE_READ_DELEGATION
  lock_res = local_lock_table->acquire_local_read_lock(k, &busy_waiting_queue, cxt, coro_id);
  read_handover = (lock_res.first && !lock_res.second);
#endif
  try_read_op[dsm->getMyThreadID()]++;
  if (read_handover) {
    read_handover_num[dsm->getMyThreadID()]++;
    goto search_finish;
  }

  // search local cache
#ifdef TREE_ENABLE_CACHE
  from_cache = index_cache->search_from_cache(k, entry_ptr_ptr, entry_ptr, entry_idx);
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    depth = entry_ptr->depth;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }
#else
  p_ptr = root_ptr_ptr;
  p = get_root_ptr(cxt, coro_id);
  depth = 0;
#endif
  depth ++;
  cache_depth = depth;
  assert(p != InternalEntry::Null());

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;

  // 1. If we are at a NULL node, inject a leaf
  if (p == InternalEntry::Null()) {
    assert(from_cache == false);
    search_res = false;
    goto search_finish;
  }

  // 2. If we are at a leaf, read the leaf
  if (p.is_leaf) {
    // 2.1 read the leaf
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
    is_valid = read_leaf(p.addr(), leaf_buffer, std::max((unsigned long)p.kv_len, sizeof(Leaf)), p_ptr, from_cache, cxt, coro_id);

    if (!is_valid) {
#ifdef TREE_ENABLE_CACHE
      // invalidate the old leaf entry cache
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
#endif
      // re-read leaf entry
      auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
      dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
      p = *(InternalEntry *)entry_buffer;
      from_cache = false;
      retry_flag = INVALID_LEAF;
      goto next;
    }
    auto leaf = (Leaf *)leaf_buffer;
    auto _k = leaf->get_key();

    // 2.2 Check if it is the key we search
    if (_k == k) {
      v = leaf->get_value();
      search_res = true;
    }
    else {
      search_res = false;
    }
    goto search_finish;
  }

  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
#ifdef TREE_ENABLE_CACHE
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
#endif
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_NODE;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
#ifdef TREE_ENABLE_CACHE
  if (from_cache && !type_correct) {  // invalidate the out dated node type
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
  if (depth == hdr.depth) {
    index_cache->add_to_cache(k, p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }
#else
  UNUSED(type_correct);
#endif

  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      search_res = false;
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type());
  // find from the exist slot
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      retry_flag = FIND_NEXT;
      goto next;  // search next level
    }
  }

search_finish:
#ifdef TREE_ENABLE_CACHE
  if (!read_handover) {
    auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
    cache_hit[dsm->getMyThreadID()] += hit;
    cache_miss[dsm->getMyThreadID()] += (1 - hit);
  }
#endif
#ifdef TREE_ENABLE_READ_DELEGATION
  local_lock_table->release_local_read_lock(k, lock_res, search_res, v);  // handover the ret leaf addr
#endif

  return search_res;
}


void Tree::search_entries(const Key &from, const Key &to, int target_depth, std::vector<ScanContext> &res, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  GlobalAddress p_ptr;
  InternalEntry p;
  int depth;
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;
  int cache_depth = 0;

  bool type_correct;
  char* page_buffer;
  bool is_valid;
  InternalPage* p_node;
  Header hdr;
  int max_num;

  // search local cache
#ifdef TREE_ENABLE_CACHE
  from_cache = index_cache->search_from_cache(from, entry_ptr_ptr, entry_ptr, entry_idx);
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    depth = entry_ptr->depth;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }
#else
  p_ptr = root_ptr_ptr;
  p = get_root_ptr(cxt, coro_id);
  depth = 0;
#endif
  depth ++;
  cache_depth = depth;

next:
  // 1. If we are at a NULL node
  if (p == InternalEntry::Null()) {
    goto search_finish;
  }

  // 2. Check if it is the target depth
  if (depth == target_depth) {
    res.push_back(ScanContext(p, p_ptr, depth-1, from_cache, entry_ptr_ptr, entry_ptr, from, to, BORDER, BORDER));
    goto search_finish;
  }
  if (p.is_leaf) {
    goto search_finish;
  }

  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
#ifdef TREE_ENABLE_CACHE
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
#endif
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
#ifdef TREE_ENABLE_CACHE
  if (from_cache && !type_correct) {
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);  // invalidate the out dated node type
  }
#else
  UNUSED(type_correct);
#endif
  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(from, hdr.depth + i) != hdr.partial[i]) {
      goto search_finish;
    }
    if (hdr.depth + i + 1 == target_depth) {
      range_query_on_page(p_node, from_cache, depth-1,
                          p_ptr, p,
                          from, to, BORDER, BORDER, res);
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  // find from the exist slot
  max_num = node_type_to_num(p.type());
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(from, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      goto next;  // search next level
    }
  }
search_finish:
#ifdef TREE_ENABLE_CACHE
  auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
  cache_hit[dsm->getMyThreadID()] += hit;
  cache_miss[dsm->getMyThreadID()] += (1 - hit);
#endif
  return;
}

/*
  range query, DO NOT support corotine currently
*/
// [from, to)
void Tree::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {
  thread_local std::vector<ScanContext> survivors;
  thread_local std::vector<RdmaOpRegion> rs;
  thread_local std::vector<ScanContext> si;
  thread_local std::vector<RangeCache> range_cache;
  thread_local std::set<uint64_t> tokens;

  assert(dsm->is_register());
  if (to <= from) return;

  range_cache.clear();
  tokens.clear();

  auto range_buffer = (dsm->get_rbuf(0)).get_range_buffer();
  int cnt;

  // search local cache
#ifdef TREE_ENABLE_CACHE
  index_cache->search_range_from_cache(from, to, range_cache);
  // entries in cache
  for (auto & rc : range_cache) {
    survivors.push_back(ScanContext(rc.e, rc.e_ptr, rc.depth, true, rc.entry_ptr_ptr, rc.entry_ptr,
                                    std::max(rc.from, from),
                                    std::min(rc.to, to - 1),
                                    rc.from <= from   ? BORDER : INSIDE,   // TODO: outside?
                                    rc.to   >= to - 1 ? BORDER : INSIDE));
  }
  if (range_cache.empty()) {
    int partial_len = longest_common_prefix(from, to - 1, 0);
    search_entries(from, to - 1, partial_len, survivors, nullptr, 0);
  }
#else
  int partial_len = longest_common_prefix(from, to - 1, 0);
  search_entries(from, to - 1, partial_len, survivors, nullptr, 0);
#endif

  int idx = 0;
next_level:
  idx  ++;
  if (survivors.empty()) {  // exit
    return;
  }
  rs.clear();
  si.clear();

  // 1. batch read the current level of nodes / leaves
  cnt = 0;
  for(auto & s : survivors) {
    auto& p = s.e;
    auto token = (uint64_t)p.addr();
    if (tokens.find(token) == tokens.end()) {
      RdmaOpRegion r;
      r.source     = (uint64_t)range_buffer + cnt * define::allocationPageSize;
      r.dest       = p.addr();
      r.size       = p.is_leaf ? std::max((unsigned long)p.kv_len, sizeof(Leaf)) : (
                              s.from_cache ?  // TODO: art
                              (sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(NODE_256) * sizeof(InternalEntry)) :
                              (sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(p.type()) * sizeof(InternalEntry))
                          );
      r.is_on_chip = false;
      rs.push_back(r);
      si.push_back(s);
      cnt ++;
      tokens.insert(token);
    }
  }
  survivors.clear();
  // printf("cnt=%d\n", cnt);

  // 2. separate requests with its target node, and read them using doorbell batching for each batch
  dsm->read_batches_sync(rs);

  // 3. process the read nodes and leaves
  for (int i = 0; i < cnt; ++ i) {
    // 3.1 if it is leaf, check & save result
    if (si[i].e.is_leaf) {
      Leaf *leaf = (Leaf *)(range_buffer + i * define::allocationPageSize);
      auto k = leaf->get_key();

      if (!leaf->is_valid(si[i].e_ptr, si[i].from_cache)) {
        // invalidate the old leaf entry cache
#ifdef TREE_ENABLE_CACHE
        if (si[i].from_cache) {
          index_cache->invalidate(si[i].entry_ptr_ptr, si[i].entry_ptr);
        }
#endif
        // re-read leaf entry
        auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
        dsm->read_sync((char *)entry_buffer, si[i].e_ptr, sizeof(InternalEntry));
        si[i].e = *(InternalEntry *)entry_buffer;
        si[i].from_cache = false;
        survivors.push_back(si[i]);
        continue;
      }
      if (!leaf->is_consistent()) {  // re-read leaf is unconsistent
        survivors.push_back(si[i]);
      }

      if (k >= from && k < to) {  // [from, to)
        ret[k] = leaf->get_value();
        // TODO: cache hit ratio
      }
    }
    // 3.2 if it is node, check & choose in-range entry in it
    else {
      InternalPage* node = (InternalPage *)(range_buffer + i * define::allocationPageSize);
      if (!node->is_valid(si[i].e_ptr, si[i].depth + 1, si[i].from_cache)) {  // node deleted || outdated cache entry in cached node
#ifdef TREE_ENABLE_CACHE
        // invalidate the old node cache
        if (si[i].from_cache) {
          index_cache->invalidate(si[i].entry_ptr_ptr, si[i].entry_ptr);
        }
#endif
        // re-read node entry
        auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
        dsm->read_sync((char *)entry_buffer, si[i].e_ptr, sizeof(InternalEntry));
        si[i].e = *(InternalEntry *)entry_buffer;
        si[i].from_cache = false;
        survivors.push_back(si[i]);
        continue;
      }
      range_query_on_page(node, si[i].from_cache, si[i].depth,
                          si[i].e_ptr, si[i].e,
                          si[i].from, si[i].to, si[i].l_state, si[i].r_state, survivors);
    }
  }
  goto next_level;
}


void Tree::range_query_on_page(InternalPage* page, bool from_cache, int depth,
                               GlobalAddress p_ptr, InternalEntry p,
                               const Key &from, const Key &to, State l_state, State r_state,
                               std::vector<ScanContext>& res) {
  // check header
  auto& hdr = page->hdr;
  // assert(ei.depth + 1 == hdr.depth);  // only in condition of no concurrent insert
#ifdef TREE_ENABLE_CACHE
  if (depth == hdr.depth - 1) {
    index_cache->add_to_cache(from, page, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }
#endif

  if (l_state == BORDER) { // left state: BORDER --> other state
    int j;
    for (j = 0; j < hdr.partial_len; ++ j) if (hdr.partial[j] != get_partial(from, hdr.depth + j)) break;
    if (j == hdr.partial_len) l_state = BORDER;
    else if (hdr.partial[j] > get_partial(from, hdr.depth + j)) l_state = INSIDE;
    else l_state = OUTSIDE;
  }
  if (r_state == BORDER) {  // right state: BORDER --> other state
    int j;
    for (j = 0; j < hdr.partial_len; ++ j) if (hdr.partial[j] != get_partial(to, hdr.depth + j)) break;
    if (j == hdr.partial_len) r_state = BORDER;
    else if (hdr.partial[j] < get_partial(to, hdr.depth + j)) r_state = INSIDE;
    else r_state = OUTSIDE;
  }
  if (l_state == OUTSIDE || r_state == OUTSIDE) return;

  // check partial & choose entry from records
  const uint8_t from_partial = get_partial(from, hdr.depth + hdr.partial_len);
  const uint8_t to_partial   = get_partial(to  , hdr.depth + hdr.partial_len);
  int max_num = node_type_to_num(hdr.type());
  for(int j = 0; j < max_num; ++ j) {
    const auto& e = page->records[j];
    if (e == InternalEntry::Null()) continue;

    auto e_l_state = l_state;
    auto e_r_state = r_state;

    if (e_l_state == BORDER)  {  // left state: BORDER --> other state
      if (e.partial == from_partial) e_l_state = BORDER;
      else if (e.partial > from_partial) e_l_state = INSIDE;
      else e_l_state = OUTSIDE;
    }
    if (e_r_state == BORDER){    // right state: BORDER --> other state
      if (e.partial == to_partial) e_r_state = BORDER;
      else if (e.partial < to_partial) e_r_state = INSIDE;
      else e_r_state = OUTSIDE;
    }
    if (e_l_state != OUTSIDE && e_r_state != OUTSIDE) {
      auto next_from = from;
      auto next_to = to;
      // calculate [from, to) for this survivor entry
      if (e_l_state == INSIDE) {
        for (int i = 0; i < hdr.partial_len; ++ i) next_from = remake_prefix(next_from, hdr.depth + i, hdr.partial[i]);
        next_from = remake_prefix(next_from, hdr.depth + hdr.partial_len, e.partial);
      }
      if (e_r_state == INSIDE) {
        for (int i = 0; i < hdr.partial_len; ++ i) next_to   = remake_prefix(next_to  , hdr.depth + i, hdr.partial[i]);
        next_to   = remake_prefix(next_to  , hdr.depth + hdr.partial_len, e.partial);
      }
      res.push_back(ScanContext(e, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + j * sizeof(InternalEntry)),
                                hdr.depth + hdr.partial_len, false, nullptr, nullptr, next_from, next_to, e_l_state, e_r_state));
    }
  }
}


void Tree::run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req, int req_num) {
  using namespace std::placeholders;

  assert(coro_cnt <= MAX_CORO_NUM);
  for (int i = 0; i < coro_cnt; ++i) {
    RequstGen *gen = gen_func(dsm, req, req_num, i, coro_cnt);
    worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, work_func, i));
  }

  master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

  master();
}


void Tree::coro_worker(CoroYield &yield, RequstGen *gen, WorkFunc work_func, int coro_id) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (!need_stop) {
    auto r = gen->next();

    coro_timer.begin();
    work_func(this, r, &ctx, coro_id);
    auto us_10 = coro_timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][coro_id][us_10]++;
  }
}


void Tree::coro_master(CoroYield &yield, int coro_cnt) {
  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }
  while (!need_stop) {
    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      yield(worker[next_coro_id]);
    }
    // uint64_t wr_ids[POLL_CQ_MAX_CNT_ONCE];
    // int cnt = dsm->poll_rdma_cq_batch_once(wr_ids, POLL_CQ_MAX_CNT_ONCE);
    // for (int i = 0; i < cnt; ++ i) {
    //   yield(worker[wr_ids[i]]);
    // }

    if (!busy_waiting_queue.empty()) {
    // int cnt = busy_waiting_queue.size();
    // while (cnt --) {
      auto next = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      next_coro_id = next.first;
      if (next.second()) {
        yield(worker[next_coro_id]);
      }
      else {
        busy_waiting_queue.push(next);
      }
    }
  }
}


void Tree::statistics() {
#ifdef TREE_ENABLE_CACHE
  index_cache->statistics();
#endif
}

void Tree::clear_debug_info() {
  memset(cache_miss, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(cache_hit, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  // memset(try_lock, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_leaf_retry, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_cache_invalid, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_leaf, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_node_repair, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_node, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_node_type, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_NODE_TYPE_NUM);
  memset(retry_cnt, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_FLAG_NUM);
}
