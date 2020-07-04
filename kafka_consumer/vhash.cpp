
#include "vhash.h"
#include <string.h>

void mix(uint64 &a, uint64 &b, uint64 &c)
{
    a -= b;
    a -= c;
    a ^= (c >> 43);
    b -= c;
    b -= a;
    b ^= (a << 9);
    c -= a;
    c -= b;
    c ^= (b >> 8);
    a -= b;
    a -= c;
    a ^= (c >> 38);
    b -= c;
    b -= a;
    b ^= (a << 23);
    c -= a;
    c -= b;
    c ^= (b >> 5);
    a -= b;
    a -= c;
    a ^= (c >> 35);
    b -= c;
    b -= a;
    b ^= (a << 49);
    c -= a;
    c -= b;
    c ^= (b >> 11);
    a -= b;
    a -= c;
    a ^= (c >> 12);
    b -= c;
    b -= a;
    b ^= (a << 18);
    c -= a;
    c -= b;
    c ^= (b >> 22);
}


int64 vhash_impl(const char *bytes, int len, uint64 seed)
{
    int size = len;

    uint64 a = seed;
    uint64 b = a;
    uint64 c = -7046029254386353133LL;

    uint64 *intBuffer = (uint64 *)bytes;

    if (len == 8)
    {
        c += size;
        a += intBuffer[0];
        mix(a, b, c);
        return c >> 1;
    }

    while (len >= 24)
    {
        a += intBuffer[0];
        b += intBuffer[1];
        c += intBuffer[2];
        mix(a, b, c);
        intBuffer += 3;
        len -= 24;
    }
    c += size;

    char charBuffer[24];
    memset(charBuffer, 0, 24);
    memcpy(charBuffer, (char*)intBuffer, len);

    intBuffer = (uint64 *)charBuffer;
    a += intBuffer[0];
    b += intBuffer[1];
    c += intBuffer[2] << 8;
    mix(a, b, c);

    return c >> 1;
}
