#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"

// abstract rdma registered buffer
class RdmaBuffer {

private:
  // async, buffer safty
  static const int kCasBufferCnt    = 256;
  static const int kPageBufferCnt   = 256;  // big enough to hold batch internal node write in out_of_place_write_node
  static const int kLeafBufferCnt   = 32;
  static const int kHeaderBufferCnt = 32;
  static const int kEntryBufferCnt  = 32;

  char *buffer;

  uint64_t *cas_buffer;
  char *page_buffer;
  char *leaf_buffer;
  uint64_t * header_buffer;
  uint64_t * entry_buffer;
  char   *range_buffer;
  char   *zero_byte;

  int cas_buffer_cur;
  int page_buffer_cur;
  int leaf_buffer_cur;
  int header_buffer_cur;
  int entry_buffer_cur;

public:
  RdmaBuffer(char *buffer) {
    set_buffer(buffer);

    cas_buffer_cur    = 0;
    page_buffer_cur   = 0;
    leaf_buffer_cur   = 0;
    header_buffer_cur = 0;
    entry_buffer_cur  = 0;
  }

  RdmaBuffer() = default;

  void set_buffer(char *buffer) {
    // printf("set buffer %p\n", buffer);
    this->buffer  = buffer;
    cas_buffer    = (uint64_t *)buffer;
    page_buffer   = (char     *)((char *)cas_buffer    + sizeof(uint64_t)   * kCasBufferCnt);
    leaf_buffer   = (char     *)((char *)page_buffer   + define::allocationPageSize * kPageBufferCnt);
    header_buffer = (uint64_t *)((char *)leaf_buffer   + define::allocAlignLeafSize * kLeafBufferCnt);
    entry_buffer  = (uint64_t *)((char *)header_buffer + sizeof(uint64_t)   * kHeaderBufferCnt);
    zero_byte     = (char     *)((char *)entry_buffer  + sizeof(uint64_t)   * kEntryBufferCnt);
    range_buffer  = (char     *)((char *)zero_byte     + sizeof(char));
    *zero_byte    = '\0';

    assert(range_buffer - buffer < define::kPerCoroRdmaBuf);
  }

  uint64_t *get_cas_buffer() {
    cas_buffer_cur = (cas_buffer_cur + 1) % kCasBufferCnt;
    return cas_buffer + cas_buffer_cur;
  }

  char *get_page_buffer() {
    page_buffer_cur = (page_buffer_cur + 1) % kPageBufferCnt;
    return page_buffer + page_buffer_cur * define::allocationPageSize;
  }

  char *get_leaf_buffer() {
    leaf_buffer_cur = (leaf_buffer_cur + 1)  % kLeafBufferCnt;
    return leaf_buffer + leaf_buffer_cur * define::allocAlignLeafSize;
  }

  uint64_t *get_header_buffer() {
    header_buffer_cur = (header_buffer_cur + 1) % kHeaderBufferCnt;
    return header_buffer + header_buffer_cur;
  }

  uint64_t *get_entry_buffer() {
    entry_buffer_cur = (entry_buffer_cur + 1) % kEntryBufferCnt;
    return entry_buffer + entry_buffer_cur;
  }

  char *get_range_buffer() {
    return range_buffer;
  }

  char *get_zero_byte() {
    return zero_byte;
  }
};

#endif // _RDMA_BUFFER_H_
