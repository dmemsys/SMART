#include "RdmaCache.h"

Cache::Cache(const CacheConfig &cache_config) {
    size = cache_config.cacheSize;
    data = (uint64_t)hugePageAlloc(size * define::GB);
}

Cache::~Cache() { hugePageFree((void *)data, size * define::GB); }