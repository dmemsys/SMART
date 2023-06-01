#if !defined(_LOCAL_ALLOC_H_)
#define _LOCAL_ALLOC_H_

#include "Common.h"
#include "GlobalAddress.h"

#include <vector>
#include <list>

// for fine-grained shared memory alloc
// not thread safe
// now it is a simple log-structure alloctor
class LocalAllocator {

public:
  LocalAllocator() {
    head = GlobalAddress::Null();
    cur = GlobalAddress::Null();
  }

  using FreeList = std::list<std::pair<GlobalAddress, size_t> >;
  GlobalAddress malloc(size_t size, bool &need_chunck, bool align = true) {
    GlobalAddress res;

    // search from prefetch memory first
    if (align) {
      cur.offset = ROUND_UP(cur.offset, ALLOC_ALLIGN_BIT);
    }
    res = cur;
    if (head == GlobalAddress::Null() || (cur.offset + size > head.offset + define::kChunkSize)) {
      need_chunck = true;
    } else {
      need_chunck = false;
      cur.offset += size;
    }

    // search from the free_list then
    if (need_chunck) {
      for (auto iter = free_list.begin(); iter != free_list.end(); ++ iter) {
        if (iter->second >= size) {
          res = iter->first;
          free_list.erase(iter);
          need_chunck = false;
          break;
        }
      }
    }

    return res;
  }

  void set_chunck(GlobalAddress &addr) {
    head = cur = addr;
  }

  void free(const GlobalAddress &addr, size_t size) {
    free_list.push_back(std::make_pair(addr, size));
  }

private:
  GlobalAddress head;
  GlobalAddress cur;
  FreeList free_list;
};

#endif // _LOCAL_ALLOC_H_
