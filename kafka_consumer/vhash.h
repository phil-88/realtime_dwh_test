
#ifndef _VHASH_H
#define _VHASH_H

#include <cstdint>

typedef std::int64_t    int64;
typedef std::uint64_t  uint64;

int64 vhash_impl(const char *bytes, int len, uint64 seed = 200LL);

#endif
