
#ifndef _VHASH_H
#define _VHASH_H

typedef unsigned long long    uint64;
typedef long long             int64;

int64 vhash_impl(const char *bytes, int len, uint64 seed = 200LL);

#endif
