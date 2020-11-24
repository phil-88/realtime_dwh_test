
#include "utils.h"


std::vector<std::string> splitString(std::string s, char delim)
{
    std::vector<std::string> res;
    std::istringstream f(s);
    std::string p;
    while (getline(f, p, delim))
    {
        res.push_back(p);
    }
    return res;
}

int64 toInt(std::string s, int64 defultValue)
{
    std::size_t const sign = s.find_first_of("-");
    std::size_t const b = s.find_first_of("0123456789");
    if (b != std::string::npos)
    {
        std::size_t const e = s.find_first_not_of("0123456789", b);
        return (sign != std::string::npos ? -1 : 1) * stoll(s.substr(b, e != std::string::npos ? e - b : e));
    }
    return defultValue;
}

void intToHex(uint32 x, char *s, int size)
{
    static const char digits[513] =
        "000102030405060708090a0b0c0d0e0f"
        "101112131415161718191a1b1c1d1e1f"
        "202122232425262728292a2b2c2d2e2f"
        "303132333435363738393a3b3c3d3e3f"
        "404142434445464748494a4b4c4d4e4f"
        "505152535455565758595a5b5c5d5e5f"
        "606162636465666768696a6b6c6d6e6f"
        "707172737475767778797a7b7c7d7e7f"
        "808182838485868788898a8b8c8d8e8f"
        "909192939495969798999a9b9c9d9e9f"
        "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
        "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
        "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
        "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
        "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
        "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";
    int i = size - 1;
    char *lut = (char *)(digits);
    while (i >= 0)
    {
        int pos = (x & 0xFF) * 2;

        s[i * 2] = lut[pos];
        s[i * 2 + 1] = lut[pos + 1];

        x >>= 8;
        i -= 1;
    }
}

void write_int(char *buf, int v, int &offset)
{
    *(int*)(buf + offset) = v;
    offset += 4;
}

void write_short(char *buf, short v, int &offset)
{
    *(short*)(buf + offset) = v;
    offset += 2;
}

void write_float(char *buf, float v, int &offset)
{
    *(float*)(buf + offset) = v;
    offset += 4;
}

void write_int64(char *buf, int64 v, int &offset)
{
    *(int64*)(buf + offset) = v;
    offset += 8;
}

char hex2byte(char *h)
{
    char a = (h[0] <= '9') ? h[0] - '0' : (h[0] & 0x7) + 9;
    char b = (h[1] <= '9') ? h[1] - '0' : (h[1] & 0x7) + 9;
    return (a << 4) + b;
}

int unescape(char *buf, int len)
{
    int in, out;
    for (in = 0, out = 0; in < len; ++in && ++out)
    {
        if (buf[in] == '\\' && in + 1 < len)
        {
            ++in;
            switch (buf[in])
            {
            case 't':
                buf[out] = '\t';
                break;
            case 'b':
                buf[out] = '\b';
                break;
            case 'f':
                buf[out] = '\f';
                break;
            case 'n':
                buf[out] = '\n';
                break;
            case 'r':
                buf[out] = '\r';
                break;
            case '\\':
                buf[out] = '\\';
                break;
            case '"':
                buf[out] = '"';
                break;
            case 'u':
                if (in + 4 < len
                    && buf[in + 1] == '0'
                    && buf[in + 2] == '0'
                    && buf[in + 3] >= '0' && buf[in + 3] < '8'
                    && ((buf[in + 4] >= '0' && buf[in + 4] <= '9') ||
                        (buf[in + 4] >= 'a' && buf[in + 4] <= 'f') ||
                        (buf[in + 4] >= 'A' && buf[in + 4] <= 'F')))
                {
                    buf[out] = hex2byte(buf + in + 3);
                    in += 4;
                    break;
                }
            default:
                buf[out++] = '\\';
                buf[out] = buf[in];
            }
        }
        else if (out < in)
        {
            buf[out] = buf[in];
        }
    }
    return out;
}

