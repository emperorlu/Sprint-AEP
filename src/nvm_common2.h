#pragma once 

#include <cstring>
#include <string>
#include <libpmem.h>
#include <sys/time.h>
#include <assert.h>
#include <atomic>

#include "nvm_allocator.h"
#include "statistic.h"
#include "debug.h"

namespace rocksdb {
    const int NVM_NodeSize = 256;
    const int NVM_KeySize = 8;
    const int NVM_ValueSize = 1024;//1024;
    const int NVM_PointSize = 8;
    const int NVM_SignSize = 3;
    const int NVM_KeyBuf = NVM_KeySize + NVM_PointSize + NVM_SignSize;
    // Statistic stats;
    static inline int KeyCompare(const void *key1, const void *key2) {
        return memcmp(key1, key2, NVM_KeySize);
    }

    static inline uint64_t char8toint64(const char *key) {
        uint64_t value = ((((uint64_t)key[0]) & 0xff) << 56) | ((((uint64_t)key[1]) & 0xff) << 48) |
               ((((uint64_t)key[2]) & 0xff) << 40) | ((((uint64_t)key[3]) & 0xff) << 32) |
               ((((uint64_t)key[4]) & 0xff) << 24) | ((((uint64_t)key[5]) & 0xff) << 16) |
               ((((uint64_t)key[6]) & 0xff) << 8)  | (((uint64_t)key[7]) & 0xff);
        return value;

    }

    static inline void fillchar8wirhint64(char *key, uint64_t value) {
        key[0] = ((char)(value >> 56)) & 0xff;
        key[1] = ((char)(value >> 48)) & 0xff;
        key[2] = ((char)(value >> 40) )& 0xff;
        key[3] = ((char)(value >> 32)) & 0xff;
        key[4] = ((char)(value >> 24)) & 0xff;
        key[5] = ((char)(value >> 16)) & 0xff;
        key[6] = ((char)(value >> 8)) & 0xff;
        key[7] = ((char)value) & 0xff;
    }
}


// static inline uint64_t get_now_micros(){
//     struct timeval tv;
//     gettimeofday(&tv, NULL);
//     return (tv.tv_sec) * 1000000 + tv.tv_usec;
// }

extern atomic<uint64_t> perist_data;

static inline void show_persist_data() {
    print_log(LV_INFO, "Persit data is %ld %lf GB.", perist_data.load(std::memory_order_relaxed), (1.0 * perist_data) / 1000 / 1000/ 1000);
}

static inline void nvm_persist(const void *addr, size_t len) {
    perist_data += len;
    // print_log(LV_DEBUG, "perist_data is %ld, len is %ld", perist_data.load(std::memory_order_relaxed), len);
    // show_persist_data();
    pmem_persist(addr, len);
}

static inline void nvm_memcpy_persist(void *pmemdest, const void *src, size_t len, bool statistic = true) {
    if(statistic) {
        perist_data += len;
    }
    pmem_memcpy_persist(pmemdest, src, len);
}

// #define SBH_DEBUG
#ifdef SBH_DEBUG
#define SBH_PRINT(format, a...) printf("DEBUG: %-30s %3d:"#format"\n", __FUNCTION__, __LINE__, ##a)
#else 
#define SBH_PRINT(format, a...)
#endif