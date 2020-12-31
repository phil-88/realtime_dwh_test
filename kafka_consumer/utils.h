
#ifndef _UTILS_H
#define _UTILS_H

#include <string>
#include <vector>
#include <limits.h>

#define JSMN_STATIC
#define JSMN_PARENT_LINKS
#include "jsmn.h"

#include <climits>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <ctime>
#include <regex>
#include <iomanip>
#include <fstream>
#include <cstdint>

#define NOTINT INT_MAX

typedef std::int8_t     int8;
typedef std::uint8_t   uint8;
typedef std::int32_t    int32;
typedef std::uint32_t  uint32;
typedef std::int64_t    int64;
typedef std::uint64_t  uint64;


std::vector<std::string> splitString(std::string s, char delim);

int64 toInt(std::string s, int64 defultValue = NOTINT);

void intToHex(uint32 x, char *s, int size = 4);
char hex2byte(char *h);

void write_int(char *buf, int v, int &offset);
void write_short(char *buf, short v, int &offset);
void write_float(char *buf, float v, int &offset);
void write_int64(char *buf, int64 v, int &offset);

int unescape(char *buf, int len);

bool fileExists(const std::string& filename);

#define vt_report_error(errcode, args...) \
    do { \
        fprintf(stderr, args); \
        abort(); \
    } while(0)


#endif
