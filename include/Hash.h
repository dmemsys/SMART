#if !defined(_HASH_H_)
#define _HASH_H_

#include "Common.h"
#include "Key.h"
#include "GlobalAddress.h"
#include "city.h"


class Hash {
public:
  Hash() {}

  uint64_t get_hashed_lock_index(const Key& k);
  uint64_t get_hashed_lock_index(const GlobalAddress& addr);
};


inline uint64_t Hash::get_hashed_lock_index(const Key& k) {
  return CityHash64((char *)&k, sizeof(k)) % define::kLocalLockNum;
}


inline uint64_t Hash::get_hashed_lock_index(const GlobalAddress& addr) {
  return CityHash64((char *)&addr, sizeof(addr)) % define::kLocalLockNum;
}


#endif // _HASH_H_
