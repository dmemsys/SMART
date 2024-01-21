
#include "DSM.h"
#include "Directory.h"
#include "HugePageAlloc.h"

#include "DSMKeeper.h"
#include "Key.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <map>

thread_local int DSM::thread_id = -1;
thread_local ThreadConnection *DSM::iCon = nullptr;
thread_local char *DSM::rdma_buffer = nullptr;
thread_local LocalAllocator DSM::local_allocators[MEMORY_NODE_NUM][NR_DIRECTORY];
thread_local RdmaBuffer DSM::rbuf[MAX_CORO_NUM];
thread_local uint64_t DSM::thread_tag = 0;


DSM *DSM::getInstance(const DSMConfig &conf) {
  static DSM *dsm = nullptr;
  static WRLock lock;

  lock.wLock();
  if (!dsm) {
    dsm = new DSM(conf);
  } else {
  }
  lock.wUnlock();

  return dsm;
}

DSM::DSM(const DSMConfig &conf)
    : conf(conf), appID(0), cache(conf.cacheConfig) {

  baseAddr = (uint64_t)hugePageAlloc(conf.dsmSize * define::GB);

  Debug::notifyInfo("shared memory size: %dGB, 0x%lx", conf.dsmSize, baseAddr);
  Debug::notifyInfo("rdma cache size: %dGB", conf.cacheConfig.cacheSize);

  // warmup
  memset((char *)baseAddr, 0, conf.dsmSize * define::GB);
  memset((char *)cache.data, 0, cache.size * define::GB);

  initRDMAConnection();
  if (myNodeID < MEMORY_NODE_NUM) {  // start memory server
    for (int i = 0; i < NR_DIRECTORY; ++i) {
      dirAgent[i] =
          new Directory(dirCon[i], remoteInfo, MEMORY_NODE_NUM, i, myNodeID);
    }
    Debug::notifyInfo("Memory server %d start up", myNodeID);
  }
  keeper->barrier("DSM-init");
}

DSM::~DSM() { hugePageFree((void *)baseAddr, conf.dsmSize * define::GB); }

void DSM::registerThread() {

  if (thread_id != -1)
    return;

  thread_id = appID.fetch_add(1);
  thread_tag = thread_id + (((uint64_t)this->getMyNodeID()) << 32) + 1;

  iCon = thCon[thread_id];

  iCon->message->initRecv();
  iCon->message->initSend();
  rdma_buffer = (char *)cache.data + thread_id * define::kPerThreadRdmaBuf;

  for (int i = 0; i < MAX_CORO_NUM; ++i) {
    rbuf[i].set_buffer(rdma_buffer + i * define::kPerCoroRdmaBuf);
  }
}

void DSM::loadKeySpace(const std::string& load_workloads_path, bool is_str) {
  keySpaceSize = 0;
  std::string op, line, str_k;
  int int_k;
  std::ifstream load_in(load_workloads_path);
  Debug::notifyInfo("Loading key space...");
  while (std::getline(load_in, line)) {
    if (!line.size()) continue;
    std::istringstream tmp(line);
    tmp >> op;
    assert(op == "INSERT");
    if(is_str) {
      tmp >> str_k;
      keyBuffer[keySpaceSize ++] = str2key(str_k);
    }
    else {
      tmp >> int_k;
      keyBuffer[keySpaceSize ++] = int2key(int_k);
    }
  }
  Debug::notifyInfo("Key space load done: keySpaceSize=%d", keySpaceSize);
}

Key DSM::getRandomKey() {
  uint32_t seed = asm_rdtsc();
  return keyBuffer[rand_r(&seed) % keySpaceSize];
}

Key DSM::getNoComflictKey(uint64_t key_hash, uint64_t global_thread_id, uint64_t global_thread_num) {
  auto start = keySpaceSize / global_thread_num * global_thread_id;
  return keyBuffer[start + key_hash % (keySpaceSize / global_thread_num)];
}

void DSM::initRDMAConnection() {

  Debug::notifyInfo("Machine NR: %d", conf.machineNR);

  remoteInfo = new RemoteConnection[conf.machineNR];

  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    thCon[i] =
        new ThreadConnection(i, (void *)cache.data, cache.size * define::GB,
                             conf.machineNR, remoteInfo);
  }

  for (int i = 0; i < NR_DIRECTORY; ++i) {
    dirCon[i] =
        new DirectoryConnection(i, (void *)baseAddr, conf.dsmSize * define::GB,
                                conf.machineNR, remoteInfo);
  }

  keeper = new DSMKeeper(thCon, dirCon, remoteInfo, conf.machineNR);
  myNodeID = keeper->getMyNodeID();
}

void DSM::read(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
               CoroPull *sink) {
  if (sink == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], true,
             sink->get());
    (*sink)();
  }
}

void DSM::read_sync(char *buffer, GlobalAddress gaddr, size_t size,
                    CoroPull *sink) {
  read(buffer, gaddr, size, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write(const char *buffer, GlobalAddress gaddr, size_t size,
                bool signal, CoroPull *sink) {

  if (sink == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].dsmRKey[0], -1, true,
              sink->get());
    (*sink)();
  }
}

void DSM::write_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                     CoroPull *sink) {
  write(buffer, gaddr, size, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::fill_keys_dest(RdmaOpRegion &ror, GlobalAddress gaddr, bool is_chip) {
  ror.lkey = iCon->cacheLKey;
  if (is_chip) {
    ror.dest = remoteInfo[gaddr.nodeID].lockBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].lockRKey[0];
  } else {
    ror.dest = remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset;
    ror.remoteRKey = remoteInfo[gaddr.nodeID].dsmRKey[0];
  }
}

void DSM::read_batch(RdmaOpRegion *rs, int k, bool signal, CoroPull *sink) {

  int node_id = -1;
  for (int i = 0; i < k; ++i) {
    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (sink == nullptr) {
    rdmaReadBatch(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaReadBatch(iCon->data[0][node_id], rs, k, true, sink->get());
    (*sink)();
  }
}

void DSM::read_batch_sync(RdmaOpRegion *rs, int k, CoroPull *sink) {
  read_batch(rs, k, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::read_batches_sync(const std::vector<RdmaOpRegion>& rs, CoroPull *sink) {
  std::map<uint64_t, std::vector<RdmaOpRegion> > each_rs;

  for (const auto& r : rs) {
    int node_id = GlobalAddress{r.dest}.nodeID;
    each_rs[node_id].emplace_back(r);
  }
  for (auto& p : each_rs) {
    auto& _rs = p.second;
    read_batch(&_rs[0], (int)_rs.size(), true, sink);
  }

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, (int)each_rs.size(), &wc);
  }
}

void DSM::write_batch(RdmaOpRegion *rs, int k, bool signal, CoroPull *sink) {

  int node_id = -1;
  for (int i = 0; i < k; ++i) {
    GlobalAddress gaddr;
    gaddr.val = rs[i].dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(rs[i], gaddr, rs[i].is_on_chip);
  }

  if (sink == nullptr) {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, signal);
  } else {
    rdmaWriteBatch(iCon->data[0][node_id], rs, k, true, sink->get());
    (*sink)();
  }
}

void DSM::write_batch_sync(RdmaOpRegion *rs, int k, CoroPull *sink) {
  write_batch(rs, k, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_batches_sync(RdmaOpRegion *rs, int k, CoroPull *sink) {
  std::map<uint64_t, std::vector<RdmaOpRegion> > each_rs;

  for (int i = 0; i < k; ++ i) {
    int node_id = GlobalAddress{rs[i].dest}.nodeID;
    each_rs[node_id].emplace_back(rs[i]);
  }
  for (auto& p : each_rs) {
    auto& rs = p.second;
    write_batch(&rs[0], (int)rs.size(), true, sink);
  }

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, (int)each_rs.size(), &wc);
  }
}

void DSM::write_faa(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                    uint64_t add_val, bool signal, CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = faa_ror.dest;

    fill_keys_dest(faa_ror, gaddr, faa_ror.is_on_chip);
  }
  if (sink == nullptr) {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, signal);
  } else {
    rdmaWriteFaa(iCon->data[0][node_id], write_ror, faa_ror, add_val, true,
                 sink->get());
    (*sink)();
  }
}
void DSM::write_faa_sync(RdmaOpRegion &write_ror, RdmaOpRegion &faa_ror,
                         uint64_t add_val, CoroPull *sink) {
  write_faa(write_ror, faa_ror, add_val, true, sink);
  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_cas(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                    uint64_t equal, uint64_t val, bool signal,
                    CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;

    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  if (sink == nullptr) {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val, signal);
  } else {
    rdmaWriteCas(iCon->data[0][node_id], write_ror, cas_ror, equal, val, true, sink->get());
    (*sink)();
  }
}
void DSM::write_cas_sync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                         uint64_t equal, uint64_t val, CoroPull *sink) {
  write_cas(write_ror, cas_ror, equal, val, true, sink);
  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_cas_mask(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                         uint64_t equal, uint64_t val, uint64_t mask, bool signal,
                         CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    node_id = gaddr.nodeID;

    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;

    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  if (sink == nullptr) {
    rdmaWriteCasMask(iCon->data[0][node_id], write_ror, cas_ror, equal, val, mask, signal);
  } else {
    rdmaWriteCasMask(iCon->data[0][node_id], write_ror, cas_ror, equal, val, mask, true, sink->get());
    (*sink)();
  }
}
void DSM::write_cas_mask_sync(RdmaOpRegion &write_ror, RdmaOpRegion &cas_ror,
                              uint64_t equal, uint64_t val, uint64_t mask, CoroPull *sink) {
  write_cas_mask(write_ror, cas_ror, equal, val, mask, true, sink);
  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_read(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                   uint64_t equal, uint64_t val, bool signal,
                   CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    fill_keys_dest(read_ror, gaddr, read_ror.is_on_chip);
  }

  if (sink == nullptr) {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, signal);
  } else {
    rdmaCasRead(iCon->data[0][node_id], cas_ror, read_ror, equal, val, true,
                sink->get());
    (*sink)();
  }
}

void DSM::read_cas(RdmaOpRegion &read_ror, RdmaOpRegion &cas_ror,
                   uint64_t equal, uint64_t val, bool signal,
                   CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = read_ror.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(read_ror, gaddr, read_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }

  if (sink == nullptr) {
    rdmaReadCas(iCon->data[0][node_id], read_ror, cas_ror, equal, val, signal);
  } else {
    rdmaReadCas(iCon->data[0][node_id], read_ror, cas_ror, equal, val, true,
                sink->get());
    (*sink)();
  }
}

bool DSM::cas_read_sync(RdmaOpRegion &cas_ror, RdmaOpRegion &read_ror,
                        uint64_t equal, uint64_t val, CoroPull *sink) {
  cas_read(cas_ror, read_ror, equal, val, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *(uint64_t *)cas_ror.source;
}

bool DSM::read_cas_sync(RdmaOpRegion &read_ror, RdmaOpRegion &cas_ror,
                        uint64_t equal, uint64_t val, CoroPull *sink) {
  read_cas(read_ror, cas_ror, equal, val, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *(uint64_t *)cas_ror.source;
}

void DSM::cas_write(RdmaOpRegion &cas_ror, RdmaOpRegion &write_ror,
                    uint64_t equal, uint64_t val, bool signal,
                    CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(cas_ror, gaddr, cas_ror.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = write_ror.dest;
    fill_keys_dest(write_ror, gaddr, write_ror.is_on_chip);
  }

  if (sink == nullptr) {
    rdmaCasWrite(iCon->data[0][node_id], cas_ror, write_ror, equal, val, signal);
  } else {
    rdmaCasWrite(iCon->data[0][node_id], cas_ror, write_ror, equal, val, true,
                sink->get());
    (*sink)();
  }
}

bool DSM::cas_write_sync(RdmaOpRegion &cas_ror, RdmaOpRegion &write_ror,
                         uint64_t equal, uint64_t val, CoroPull *sink) {
  if (GlobalAddress{cas_ror.dest}.nodeID == GlobalAddress{write_ror.dest}.nodeID) {
    cas_write(cas_ror, write_ror, equal, val, true, sink);

    if (sink == nullptr) {
      ibv_wc wc;
      pollWithCQ(iCon->cq, 1, &wc);
    }
  }
  else {
    if(!cas_ror.is_on_chip) cas(GlobalAddress{cas_ror.dest}, equal, val, (uint64_t *)cas_ror.source, true, sink);
    else cas_dm(GlobalAddress{cas_ror.dest}, equal, val, (uint64_t *)cas_ror.source, true, sink);
    if(!write_ror.is_on_chip) write((const char *)write_ror.source, GlobalAddress{write_ror.dest}, write_ror.size, true, sink);
    else write_dm((const char *)write_ror.source, GlobalAddress{write_ror.dest}, write_ror.size, true, sink);

    if (sink == nullptr) {
      ibv_wc wc;
      pollWithCQ(iCon->cq, 2, &wc);
    }
  }

  return equal == *(uint64_t *)cas_ror.source;
}

void DSM::two_cas_mask(RdmaOpRegion &cas_ror_1, uint64_t equal_1, uint64_t val_1, uint64_t mask_1,
                       RdmaOpRegion &cas_ror_2, uint64_t equal_2, uint64_t val_2, uint64_t mask_2,
                       bool signal, CoroPull *sink) {
  int node_id;
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror_1.dest;
    node_id = gaddr.nodeID;
    fill_keys_dest(cas_ror_1, gaddr, cas_ror_1.is_on_chip);
  }
  {
    GlobalAddress gaddr;
    gaddr.val = cas_ror_2.dest;
    fill_keys_dest(cas_ror_2, gaddr, cas_ror_2.is_on_chip);
  }

  if (sink == nullptr) {
    rdmaTwoCasMask(iCon->data[0][node_id], cas_ror_1, equal_1, val_1, mask_1,
                   cas_ror_2, equal_2, val_2, mask_2, signal);
  } else {
    rdmaTwoCasMask(iCon->data[0][node_id], cas_ror_1, equal_1, val_1, mask_1,
                   cas_ror_2, equal_2, val_2, mask_2, true, sink->get());
    (*sink)();
  }
}
std::pair<bool, bool> DSM::two_cas_mask_sync(RdmaOpRegion &cas_ror_1, uint64_t equal_1, uint64_t val_1, uint64_t mask_1,
                                             RdmaOpRegion &cas_ror_2, uint64_t equal_2, uint64_t val_2, uint64_t mask_2,
                                             CoroPull *sink) {
  if (GlobalAddress{cas_ror_1.dest}.nodeID == GlobalAddress{cas_ror_2.dest}.nodeID) {
    two_cas_mask(cas_ror_1, equal_1, val_1, mask_1, cas_ror_2, equal_2, val_2, mask_2, true, sink);

    if (sink == nullptr) {
      ibv_wc wc;
      pollWithCQ(iCon->cq, 1, &wc);
    }
  }
  else {
    if(!cas_ror_1.is_on_chip) cas_mask(GlobalAddress{cas_ror_1.dest}, equal_1, val_1, (uint64_t *)cas_ror_1.source, mask_1, true, sink);
    else cas_dm_mask(GlobalAddress{cas_ror_1.dest}, equal_1, val_1, (uint64_t *)cas_ror_1.source, mask_1, true, sink);
    if(!cas_ror_2.is_on_chip) cas_mask(GlobalAddress{cas_ror_2.dest}, equal_2, val_2, (uint64_t *)cas_ror_2.source, mask_2, true, sink);
    else cas_dm_mask(GlobalAddress{cas_ror_2.dest}, equal_2, val_2, (uint64_t *)cas_ror_2.source, mask_2, true, sink);

    if (sink == nullptr) {
      ibv_wc wc;
      pollWithCQ(iCon->cq, 2, &wc);
    }
  }

  return std::make_pair(equal_1 == *(uint64_t *)cas_ror_1.source, equal_2 == *(uint64_t *)cas_ror_2.source);
}

void DSM::cas(GlobalAddress gaddr, uint64_t equal, uint64_t val,
              uint64_t *rdma_buffer, bool signal, CoroPull *sink) {

  if (sink == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].dsmRKey[0], true, sink->get());
    (*sink)();
  }
}

bool DSM::cas_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, CoroPull *sink) {
  cas(gaddr, equal, val, rdma_buffer, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

void DSM::cas_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                   uint64_t *rdma_buffer, uint64_t mask, bool signal, CoroPull *sink) {
  if (sink == nullptr) {
    rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                          remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                          val, iCon->cacheLKey,
                          remoteInfo[gaddr.nodeID].dsmRKey[0], mask, signal);
  }
  else {
    rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                          remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset, equal,
                          val, iCon->cacheLKey,
                          remoteInfo[gaddr.nodeID].dsmRKey[0], mask, true, sink->get());
    (*sink)();
  }
}

bool DSM::cas_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                        uint64_t *rdma_buffer, uint64_t mask, CoroPull *sink) {
  cas_mask(gaddr, equal, val, rdma_buffer, mask, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return (equal & mask) == (*rdma_buffer & mask);
}

void DSM::faa_boundary(GlobalAddress gaddr, uint64_t add_val,
                       uint64_t *rdma_buffer, uint64_t mask, bool signal,
                       CoroPull *sink) {
  if (sink == nullptr) {
    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                            remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                            add_val, iCon->cacheLKey,
                            remoteInfo[gaddr.nodeID].dsmRKey[0], mask, signal);
  } else {
    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                            remoteInfo[gaddr.nodeID].dsmBase + gaddr.offset,
                            add_val, iCon->cacheLKey,
                            remoteInfo[gaddr.nodeID].dsmRKey[0], mask, true,
                            sink->get());
    (*sink)();
  }
}

void DSM::faa_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
                            uint64_t *rdma_buffer, uint64_t mask,
                            CoroPull *sink) {
  faa_boundary(gaddr, add_val, rdma_buffer, mask, true, sink);
  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::read_dm(char *buffer, GlobalAddress gaddr, size_t size, bool signal,
                  CoroPull *sink) {

  if (sink == nullptr) {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaRead(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
             remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
             iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], true,
             sink->get());
    (*sink)();
  }
}

void DSM::read_dm_sync(char *buffer, GlobalAddress gaddr, size_t size,
                       CoroPull *sink) {
  read_dm(buffer, gaddr, size, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::write_dm(const char *buffer, GlobalAddress gaddr, size_t size,
                   bool signal, CoroPull *sink) {
  if (sink == nullptr) {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1,
              signal);
  } else {
    rdmaWrite(iCon->data[0][gaddr.nodeID], (uint64_t)buffer,
              remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, size,
              iCon->cacheLKey, remoteInfo[gaddr.nodeID].lockRKey[0], -1, true,
              sink->get());
    (*sink)();
  }
}

void DSM::write_dm_sync(const char *buffer, GlobalAddress gaddr, size_t size,
                        CoroPull *sink) {
  write_dm(buffer, gaddr, size, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

void DSM::cas_dm(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                 uint64_t *rdma_buffer, bool signal, CoroPull *sink) {

  if (sink == nullptr) {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], signal);
  } else {
    rdmaCompareAndSwap(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                       remoteInfo[gaddr.nodeID].lockBase + gaddr.offset, equal,
                       val, iCon->cacheLKey,
                       remoteInfo[gaddr.nodeID].lockRKey[0], true,
                       sink->get());
    (*sink)();
  }
}

bool DSM::cas_dm_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                      uint64_t *rdma_buffer, CoroPull *sink) {
  cas_dm(gaddr, equal, val, rdma_buffer, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return equal == *rdma_buffer;
}

void DSM::cas_dm_mask(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                      uint64_t *rdma_buffer, uint64_t mask, bool signal, CoroPull *sink) {
  if (sink == nullptr) {
    rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                          remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
                          equal, val, iCon->cacheLKey,
                          remoteInfo[gaddr.nodeID].lockRKey[0], mask, signal);
  }
  else {
    rdmaCompareAndSwapMask(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                          remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
                          equal, val, iCon->cacheLKey,
                          remoteInfo[gaddr.nodeID].lockRKey[0], mask, true, sink->get());
    (*sink)();
  }
}

bool DSM::cas_dm_mask_sync(GlobalAddress gaddr, uint64_t equal, uint64_t val,
                           uint64_t *rdma_buffer, uint64_t mask, CoroPull *sink) {
  cas_dm_mask(gaddr, equal, val, rdma_buffer, mask, true, sink);

  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }

  return (equal & mask) == (*rdma_buffer & mask);
}

void DSM::faa_dm_boundary(GlobalAddress gaddr, uint64_t add_val,
                          uint64_t *rdma_buffer, uint64_t mask, bool signal,
                          CoroPull *sink) {
  if (sink == nullptr) {

    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                            remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
                            add_val, iCon->cacheLKey,
                            remoteInfo[gaddr.nodeID].lockRKey[0], mask, signal);
  } else {
    rdmaFetchAndAddBoundary(iCon->data[0][gaddr.nodeID], (uint64_t)rdma_buffer,
                            remoteInfo[gaddr.nodeID].lockBase + gaddr.offset,
                            add_val, iCon->cacheLKey,
                            remoteInfo[gaddr.nodeID].lockRKey[0], mask, true,
                            sink->get());
    (*sink)();
  }
}

void DSM::faa_dm_boundary_sync(GlobalAddress gaddr, uint64_t add_val,
                               uint64_t *rdma_buffer, uint64_t mask,
                               CoroPull *sink) {
  faa_dm_boundary(gaddr, add_val, rdma_buffer, mask, true, sink);
  if (sink == nullptr) {
    ibv_wc wc;
    pollWithCQ(iCon->cq, 1, &wc);
  }
}

uint64_t DSM::poll_rdma_cq(int count) {
  ibv_wc wc;
  pollWithCQ(iCon->cq, count, &wc);

  return wc.wr_id;
}

bool DSM::poll_rdma_cq_once(uint64_t &wr_id) {
  ibv_wc wc;
  int res = pollOnce(iCon->cq, 1, &wc);

  wr_id = wc.wr_id;

  return res == 1;
}

int DSM::poll_rdma_cq_batch_once(uint64_t *wr_ids, int count) {
  assert(count <= POLL_CQ_MAX_CNT_ONCE);
  ibv_wc wc_array[POLL_CQ_MAX_CNT_ONCE];
  int res = pollOnce(iCon->cq, count, wc_array);

  for (int i = 0; i < res; ++ i) wr_ids[i] = wc_array[i].wr_id;
  return res;
}
