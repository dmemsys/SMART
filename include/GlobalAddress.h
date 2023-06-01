#ifndef __GLOBALADDRESS_H__
#define __GLOBALADDRESS_H__

#include "Common.h"

#include <iostream>


class GlobalAddress {
public:

  union {
    struct {
    uint64_t nodeID : 16;
    uint64_t offset : 48;
    };
    uint64_t val;
  };

  GlobalAddress() : val(0) {}
  GlobalAddress(uint64_t nodeID, uint64_t offset) : nodeID(nodeID), offset(offset) {}
  GlobalAddress(uint64_t val) : val(val) {}

  operator uint64_t() const { return val; }

  static GlobalAddress Null() {
    static GlobalAddress _zero;
    return _zero;
  };

  static GlobalAddress Max() {
    static GlobalAddress _max(MEMORY_NODE_NUM, 0);
    return _max;
  };
} __attribute__((packed));

static_assert(sizeof(GlobalAddress) == sizeof(uint64_t));

inline GlobalAddress GADD(const GlobalAddress &addr, int off) {
  auto ret = addr;
  ret.offset += off;
  return ret;
}

inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator<(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return std::make_pair(lhs.nodeID, lhs.offset) < std::make_pair(rhs.nodeID, rhs.offset);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const GlobalAddress &obj) {
  os << "[" << (int)obj.nodeID << ", 0x" << std::hex << obj.offset << std::dec << "]";
  return os;
}

#endif /* __GLOBALADDRESS_H__ */
