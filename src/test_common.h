#pragma once

#include <unistd.h>
#include <stdint.h>
#include <sys/time.h>

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

#ifdef CZL_DEBUG
#define CZL_PRINT(format, a...) printf("DEBUG: %-30s %3d:"#format"\n", __FUNCTION__, __LINE__, ##a)
#else 
#define CZL_PRINT(format, a...)
#endif

static inline int file_exists(char const *file)
{
    return access(file, F_OK);
}

/*
 * find_last_set_64 -- returns last set bit position or -1 if set bit not found
 */
static inline int
find_last_set_64(uint64_t val)
{
    return 64 - __builtin_clzll(val) - 1;
}

inline uint64_t get_now_micros(){
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (tv.tv_sec) * 1000000 + tv.tv_usec;
}