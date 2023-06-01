#if !defined(_NODE_H_)
#define _NODE_H_

#include "Common.h"
#include "GlobalAddress.h"
#include "Key.h"


struct PackedGAddr {  // 48-bit, used by node addr/leaf addr (not entry addr)
  uint64_t mn_id     : define::mnIdBit;
  uint64_t offset    : define::offsetBit;

  operator uint64_t() { return (offset << define::mnIdBit) | mn_id; }
} __attribute__((packed));


static_assert(sizeof(PackedGAddr) == 6);

static CRCProcessor crc_processor;

/*
  Leaf Node
*/
class Leaf {
public:
  // for invalidation
  GlobalAddress rev_ptr;
  // TODO: add key len & value len for out-of-place updates

  union {
  struct {
  uint8_t f_padding   : 7;
  uint8_t valid       : 1;
  };
  uint8_t valid_byte;
  };

  uint64_t checksum;  // checksum(kv)

  // kv
  Key key;
  union {
  Value value;
  uint8_t _padding[define::simulatedValLen];
  };

  union {
  struct {
    uint8_t w_lock    : 1;
    uint8_t r_padding : 7;
  };
  uint8_t lock_byte;
  };

public:
  Leaf() {}
  Leaf(const Key& key, const Value& value, const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), f_padding(0), valid(1), key(key), value(value), lock_byte(0) { set_consistent(); }

  const Key& get_key() const { return key; }
  Value get_value() const { return value; }
  bool is_valid(const GlobalAddress& p_ptr, bool from_cache) const { return valid && (!from_cache || p_ptr == rev_ptr); }
  bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key) + sizeof(uint8_t) * define::simulatedValLen);
    return crc_processor.checksum() == checksum;
  }

  void set_value(const Value& val) { value = val; }
  void set_consistent() {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key) + sizeof(uint8_t) * define::simulatedValLen);
    checksum = crc_processor.checksum();
  }
  void unlock() { w_lock = 0; };
  void lock() { w_lock = 1; };

  static uint8_t get_partial(const Key& key, int depth);
  static Key get_leftmost(const Key& key, int depth);
  static Key get_rightmost(const Key& key, int depth);
  static Key remake_prefix(const Key& key, int depth, uint8_t diff_partial);
  static int longest_common_prefix(const Key &k1, const Key &k2, int depth);

} __attribute__((packed));


/*
  Header
*/
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
#define MAX_NODE_TYPE_NUM 8
enum NodeType : uint8_t {
  NODE_DELETED,
  NODE_4,
  NODE_8,
  NODE_16,
  NODE_32,
  NODE_64,
  NODE_128,
  NODE_256
};
#else
#define MAX_NODE_TYPE_NUM 5
enum NodeType : uint8_t {
  NODE_DELETED,
  NODE_4,
  NODE_16,
  NODE_48,
  NODE_256
};
#endif


inline int node_type_to_num(NodeType type) {
  if (type == NODE_DELETED) return 0;
#ifndef TREE_ENABLE_ART
  type = NODE_256;
#endif
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
  return 1 << (static_cast<int>(type) + 1);
#else
  switch (type) {
    case NODE_4  : return 4;
    case NODE_16 : return 16;
    case NODE_48 : return 48;
    case NODE_256: return 256;
    default:  assert(false);
  }
#endif
}


inline NodeType num_to_node_type(int num) {
  if (num == 0) return NODE_DELETED;
#ifndef TREE_ENABLE_ART
  return NODE_256;
#endif
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
  for (int i = 1; i < MAX_NODE_TYPE_NUM; ++ i) {
    if (num < (1 << (i + 1))) return static_cast<NodeType>(i);
  }
#else
  if (num < 4) return NODE_4;
  if (num < 16) return NODE_16;
  if (num < 48) return NODE_48;
  if (num < 256) return NODE_256;
#endif
  assert(false);
}


class Header {
public:
  union {
  struct {
    uint8_t depth;
    uint8_t node_type   : define::nodeTypeNumBit;
    uint8_t partial_len : 8 - define::nodeTypeNumBit;
    uint8_t partial[define::hPartialLenMax];
  };

  uint64_t val;
  };

public:
  Header() : depth(0), node_type(0), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(int depth) : depth(depth), node_type(0), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(NodeType node_type) : depth(0), node_type(node_type), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(const Key &k, int partial_len, int depth, NodeType node_type) : depth(depth), node_type(node_type), partial_len(partial_len) {
    assert((uint32_t)partial_len <= define::hPartialLenMax);
    for (int i = 0; i < partial_len; ++ i) partial[i] = get_partial(k, depth + i);
  }

  operator uint64_t() { return val; }

  bool is_match(const Key& k) {
    for (int i = 0; i < partial_len; ++ i) {
      if (get_partial(k, depth + i) != partial[i]) return false;
    }
    return true;
  }

  static Header split_header(const Header& old_hdr, int diff_idx) {
    auto new_hdr = Header();
    for (int i = diff_idx + 1; i < old_hdr.partial_len; ++ i) new_hdr.partial[i - diff_idx - 1] = old_hdr.partial[i];
    new_hdr.partial_len = old_hdr.partial_len - diff_idx - 1;
    new_hdr.depth = old_hdr.depth + diff_idx + 1;
    return new_hdr;
  }

  NodeType type() const {
    return static_cast<NodeType>(node_type);
  }
  static const uint64_t node_type_mask = (((1UL << define::nodeTypeNumBit) - 1) << 8);
} __attribute__((packed));


static_assert(sizeof(Header) == 8);
static_assert(1UL << (8 - define::nodeTypeNumBit) >= define::hPartialLenMax);


/*
  Internal Nodes
*/
class InternalEntry {
public:
  union {
  union {
    // is_leaf = 0
    struct {
      uint8_t  partial;

      uint8_t  empty     : define::kvLenBit - define::nodeTypeNumBit;
      uint8_t  node_type : define::nodeTypeNumBit;

      uint8_t  is_leaf   : 1;
      PackedGAddr packed_addr;
    }__attribute__((packed));

    // is_leaf = 1
    struct {
      uint8_t  _partial;
      uint8_t  kv_len     : define::kvLenBit;
      uint8_t  _is_leaf   : 1;
      PackedGAddr _packed_addr;
    }__attribute__((packed));
  };

  uint64_t val;
  };

public:
  InternalEntry() : val(0) {}
  InternalEntry(uint8_t partial, uint8_t kv_len, const GlobalAddress &addr) :
                _partial(partial), kv_len(kv_len), _is_leaf(1), _packed_addr{addr.nodeID, addr.offset >> ALLOC_ALLIGN_BIT} {}
  InternalEntry(uint8_t partial, NodeType node_type, const GlobalAddress &addr) :
                partial(partial), empty(0), node_type(static_cast<uint8_t>(node_type)), is_leaf(0), packed_addr{addr.nodeID, addr.offset >> ALLOC_ALLIGN_BIT} {}
  InternalEntry(uint8_t partial, const InternalEntry& e) :
                _partial(partial), kv_len(e.kv_len), _is_leaf(e._is_leaf), _packed_addr(e._packed_addr) {}
  InternalEntry(NodeType node_type, const InternalEntry& e) :
                partial(e.partial), empty(0), node_type(static_cast<uint8_t>(node_type)), is_leaf(e.is_leaf), packed_addr(e.packed_addr) {}

  operator uint64_t() const { return val; }

  static InternalEntry Null() {
    static InternalEntry zero;
    return zero;
  }

  NodeType type() const {
    return static_cast<NodeType>(node_type);
  }

  GlobalAddress addr() const {
    return GlobalAddress{packed_addr.mn_id, packed_addr.offset << ALLOC_ALLIGN_BIT};
  }
} __attribute__((packed));

inline bool operator==(const InternalEntry &lhs, const InternalEntry &rhs) { return lhs.val == rhs.val; }
inline bool operator!=(const InternalEntry &lhs, const InternalEntry &rhs) { return lhs.val != rhs.val; }

static_assert(sizeof(InternalEntry) == 8);


class InternalPage {
public:
  // for invalidation
  GlobalAddress rev_ptr;

  Header hdr;
  InternalEntry records[256];

public:
  InternalPage() { std::fill(records, records + 256, InternalEntry::Null()); }
  InternalPage(const Key &k, int partial_len, int depth, NodeType node_type, const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), hdr(k, partial_len, depth, node_type) {
    std::fill(records, records + 256, InternalEntry::Null());
  }

  bool is_valid(const GlobalAddress& p_ptr, int depth, bool from_cache) const { return hdr.type() != NODE_DELETED && hdr.depth <= depth && (!from_cache || p_ptr == rev_ptr); }
} __attribute__((packed));


static_assert(sizeof(InternalPage) == 8 + 8 + 256 * 8);


/*
  Range Query
*/
enum State : uint8_t {
  INSIDE,
  BORDER,
  OUTSIDE
};

class CacheEntry;

struct RangeCache {
  Key from;
  Key to;  // include
  GlobalAddress e_ptr;
  InternalEntry e;
  int depth;
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  RangeCache() {}
  RangeCache(const Key& from, const Key& to, const GlobalAddress& e_ptr, const InternalEntry& e, int depth, volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr) :
             from(from), to(to), e_ptr(e_ptr), e(e), depth(depth), entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr) {}
};


struct ScanContext {
  InternalEntry e;
  GlobalAddress e_ptr;
  int depth;
  bool from_cache;
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  Key from;
  Key to;  // include
  State l_state;
  State r_state;
  ScanContext() {}
  ScanContext(const InternalEntry& e, const GlobalAddress& e_ptr, int depth, bool from_cache, volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr,
              const Key& from, const Key& to, State l_state, State r_state) :
              e(e), e_ptr(e_ptr), depth(depth), from_cache(from_cache), entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr),
              from(from), to(to), l_state(l_state), r_state(r_state) {}
};

#endif // _NODE_H_
