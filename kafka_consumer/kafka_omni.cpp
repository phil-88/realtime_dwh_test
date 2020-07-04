
//#include "Vertica.h"
#include "json.h"
#include "vhash.h"

#define JSMN_STATIC
#define JSMN_PARENT_LINKS
#include "jsmn.h"

#include <iostream>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <ctime>
#include <regex>
#include <iomanip>
#include <fstream>
#include "tsl/hopscotch_map.h"
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"


#define MAX_INDEX_SIZE 2024
#define MAX_FIELD_COUNT 256
#define MAX_GROUP_COUNT 64
#define MAX_VALUES_SIZE 16000

#define ALL_PARTITIONS -1
#define REBALANCE_PARTITIONS -2
#define REBALANCE_AUTOCOMMIT true

#define POLL_TIMEOUT 30000


//using namespace Vertica;
using namespace std;
using namespace rapidjson;
using namespace cppkafka;

typedef unsigned char  uint8;
typedef unsigned long uint32;


#define vt_report_error(errcode, args...) \
    do { \
        fprintf(stderr, args); \
        abort(); \
    } while(0)


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

void intToHex(uint32 x, char *s, int size = 4)
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


class Sink
{
public:
    virtual void put(Message &doc) = 0;
    virtual void flush() = 0;
};


// ARES

enum AresType
{
    ARES_BOOL   = 0x00000001,
    ARES_INT32  = 0x00050020,
    ARES_UINT32 = 0x00060020,
    ARES_FLOAT  = 0x00070020,
    ARES_UUID   = 0x000a0080,
    ARES_INT64  = 0x000d0040,
    ARES_TYPE_SIZE = 0x0000ffff,
    ARES_ARRAY_MASK = 0x01000000,
    ARES_ARRAY_TYPE = 0x00ffffff
};


struct ColumnData {
    int type;
    int pos;
    bool fromString;
    std::vector<int> offsets;
    std::vector<bool> dataBool;
    std::vector<int32> dataInt32;
    std::vector<int64> dataInt64;
    std::vector<float> dataFloat;
};


class ARESSink : public Sink
{
    std::vector<std::string> columns;

    tsl::hopscotch_map<std::string, ColumnData,
        std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, ColumnData> >,
        30, true, tsl::power_of_two_growth_policy> columnData;

    int rowCount;

public:
    ARESSink(std::string format)
    {
        ColumnData d;

        columns.push_back("_timestamp");
        d.type = ARES_UINT32;
        d.pos = columns.size() - 1;
        d.fromString = false;
        columnData["_timestamp"] = d;

        columns.push_back("_key");
        d.type = ARES_INT64;
        d.pos = columns.size() - 1;
        d.fromString = true;
        columnData["_key"] = d;

        parseTypes(format);
        rowCount = 0;
    }

    void parseTypes(const std::string &format)
    {
        std::unordered_map<std::string, int> binaryTypes;
        binaryTypes["timestamp"]   = ARES_UINT32;
        binaryTypes["integer"]     = ARES_INT32;
        binaryTypes["boolean"]     = ARES_BOOL;
        binaryTypes["float"]       = ARES_FLOAT;
        binaryTypes["number"]      = ARES_FLOAT;
        binaryTypes["timestamp[]"] = ARES_UINT32 | ARES_ARRAY_MASK;
        binaryTypes["integer[]"]   = ARES_INT32 | ARES_ARRAY_MASK;
        binaryTypes["boolean[]"]   = ARES_BOOL  | ARES_ARRAY_MASK;
        binaryTypes["float[]"]     = ARES_FLOAT | ARES_ARRAY_MASK;
        binaryTypes["number[]"]    = ARES_FLOAT | ARES_ARRAY_MASK;

        std::vector<std::string> keys = splitString(format, ',');
        for (size_t j = 0; j < keys.size(); ++j)
        {
            std::vector<std::string> keyParts = splitString(keys[j], ':');
            std::string key = keyParts[0];
            columns.push_back(key);

            std::string typeName = keyParts[1];
            auto it = binaryTypes.find(typeName);
            if (it == binaryTypes.end())
            {
                ColumnData d;
                d.type = (typeName.find("[]") != string::npos ? ARES_ARRAY_MASK : 0) | ARES_INT64;
                d.pos = columns.size() - 1;
                d.fromString = true;
                columnData[key] = d;
            }
            else
            {
                ColumnData d;
                d.type = it->second;
                d.pos = columns.size() - 1;
                d.fromString = false;
                columnData[key] = d;
            }
        }
    }

    void put(Message &doc)
    {
        ColumnData &column_ts = columnData["_timestamp"];
        int ts = rd_kafka_message_timestamp(doc.get_handle(), NULL) / 1000;
        column_ts.dataInt32.push_back(ts);
        column_ts.offsets.push_back(rowCount);

        ColumnData &column_key = columnData["_key"];
        std::string key(doc.get_key());
        column_key.dataInt64.push_back(vhash_impl(key.data(), key.length()));
        column_key.offsets.push_back(rowCount);

        std::string j(doc.get_payload());
        StringStream ss(j.c_str());

        SAXJsonParser parser;
        parser.init();

        Reader reader;
        reader.Parse<kParseNumbersAsStringsFlag>(ss, parser);

        for (auto i = parser.data.begin(); i != parser.data.end(); ++i)
        {
            auto found = columnData.find(std::string(i->first));
            if (found != columnData.end())
            {
                ColumnData &column = (ColumnData&)found->second;
                int column_type = column.type & ARES_ARRAY_TYPE;

                if (column.fromString)
                {
                    std::string value = toString(i->second);
                    if (!value.empty())
                    {
                        column.dataInt64.push_back(vhash_impl(value.data(), value.length()));
                        column.offsets.push_back(rowCount);
                    }
                }
                else if (column_type == ARES_INT32 || column_type == ARES_UINT32)
                {
                    column.dataInt32.push_back(toInt32(i->second));
                    column.offsets.push_back(rowCount);
                }
                else if (column_type == ARES_INT64)
                {
                    column.dataInt64.push_back(toInt64(i->second));
                    column.offsets.push_back(rowCount);
                }
                else if (column_type == ARES_BOOL)
                {
                    column.dataBool.push_back(toBool(i->second));
                    column.offsets.push_back(rowCount);
                }
                else if (column_type == ARES_FLOAT)
                {
                    column.dataFloat.push_back(toDouble(i->second));
                    column.offsets.push_back(rowCount);
                }
            }
        }
        rowCount += 1;
    }

    void flush()
    {
        vector<string> column_list;
        int columnCount = columns.size();
        int filledColumns = 0;
        int arrayColumns = 0;
        int valueCount = 0;

        for (int i = 0; i < columns.size(); ++i)
        {
            ColumnData &d = columnData.at(columns[i]);
            if (d.offsets.size() > 0)
            {
                column_list.push_back(columns[i]);
                filledColumns += 1;
                arrayColumns += (d.type & ARES_ARRAY_MASK) > 0 ? 1 : 0;
                valueCount += (d.type & ARES_ARRAY_MASK) > 0 ? d.offsets.size() : rowCount;
            }
        }
        columnCount = filledColumns;

        int sizeMax = 36 + (columnCount * 19 + 4); // + end col
        sizeMax += filledColumns * (rowCount + 7) / 8; //nulls
        sizeMax += arrayColumns * (rowCount + 2) * 4; //offsets + align + end row
        sizeMax += valueCount * 8 + columnCount * 8; //values + align
        char *buf = new char[sizeMax];
        memset(buf, 0, sizeMax);

        int offset = 0;
        write_int(buf, 0xFEED0001, offset);

        write_int(buf, rowCount, offset);
        write_short(buf, columnCount, offset);
        offset += 14;
        write_int(buf, (int)time(0), offset);

        int header_offset = offset;
        int start_offsets = header_offset;
        int start_types = start_offsets + (columnCount + 1) * 4 + columnCount * 8;
        int start_ids = start_types + columnCount * 4;
        int start_mod = start_ids + columnCount * 2;
        int data_offset = start_mod + columnCount;

        for (int i = 0; i < column_list.size(); ++i)
        {
            ColumnData &col = columnData.at(column_list[i]);
            int val_count = col.offsets.size();
            int val_type = col.type & ARES_ARRAY_TYPE;
            int val_size = col.type & ARES_TYPE_SIZE;
            int has_value_offsets = col.type & ARES_ARRAY_MASK;

            offset = start_offsets + i * 4;
            write_int(buf, data_offset, offset);

            offset = start_types + i * 4;
            write_int(buf, col.type, offset);

            offset = start_ids + i * 2;
            write_short(buf, col.pos, offset);

            offset = start_mod + i;
            buf[offset] = val_count == 0 ? 0 : (val_count == rowCount ? 1 : 2);

            if (val_count == 0)
            {
                continue;
            }

            offset = data_offset;
            if (val_count > 0 && val_count < rowCount)
            {
                for (int j = 0; j < col.offsets.size(); ++j)
                {
                    int val_offset = col.offsets[j];
                    buf[offset + (val_offset + 7) / 8] |= 1 << (7 - (val_offset % 8));
                }
                offset += (rowCount + 7) / 8;
            }

            if (has_value_offsets)
            {
                offset += (4 - (offset % 4)) % 4;
                int valueNo = 0;
                for (int r = 0; r < rowCount; ++r)
                {
                    int valueRow = valueNo < val_count ? col.offsets[valueNo] : rowCount;
                    write_int(buf, valueNo * (val_size / 8), offset);
                    if (r == valueRow)
                    {
                        valueNo += 1;
                    }
                }
                write_int(buf, valueNo * (val_size / 8), offset);
            }

            offset += (8 - (offset % 8)) % 8;
            int data_start_offset = offset;
            for (int j = 0; j < col.offsets.size(); ++j)
            {
                int value_offset = (has_value_offsets ? j : col.offsets[j]);
                if (val_type == ARES_INT32 || val_type == ARES_UINT32)
                {
                    offset = data_start_offset + value_offset * 4;
                    write_int(buf, col.dataInt32[j], offset);
                }
                else if (val_type == ARES_INT64)
                {
                    offset = data_start_offset + value_offset * 8;
                    write_int64(buf, col.dataInt64[j], offset);
                }
                else if (val_type == ARES_FLOAT)
                {
                    offset = data_start_offset + value_offset * 4;
                    write_float(buf, col.dataFloat[j], offset);
                }
                else if (val_type == ARES_BOOL)
                {
                    offset = data_start_offset + (value_offset + 7) / 8;
                    buf[offset] |= 1 << (7 - (value_offset % 8));
                    offset += 1;
                }
            }
            int enc_value_count = (has_value_offsets ? col.offsets.size() : rowCount);
            data_offset = data_start_offset + (val_size + 7) / 8 * enc_value_count;
        }
        int total_size = data_offset;
        total_size += (8 - (total_size % 8)) % 8;

        offset = start_offsets + columnCount * 4;
        write_int(buf, data_offset, offset); // mark end

        std::ofstream output("ares_batch.dat", std::ios::out | std::ios::binary);
        output.write(buf, total_size);
        output.close();
    }
};


// CSV

int align256(int v, int s, uint8 base)
{
    return (v + s) <= base ? v : ((v + base - 1) / base) * base;
}

int encode256(int v, uint8 base)
{
    if (v < base)
    {
        return v + 1;
    }
    else if (v <= 127 * base)
    {
        return 128 | (v / base);
    }
    return 0;
}

enum OutputFormat
{
    OUTPUT_JSON,
    OUTPUT_EAV,
    OUTPUT_ARRAY,
    OUTPUT_COLUMNS
};


enum HeaderFields
{
    HEADER_PARTITION,
    HEADER_OFFSET,
    HEADER_KEY,
    HEADER_TIMESTAMP
};


enum ScalarTransform
{
    IDENTICAL = 0,
    HASH      = 1
};


class CSVSink : public Sink
{
    const std::string delimiter, terminator;
    std::vector<int> headerFields;
    int formatType;

    std::vector<std::string> jsonFields;
    tsl::hopscotch_map<std::string, int, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, int> >, 30, true, tsl::power_of_two_growth_policy> jsonFieldIndex;

    int fieldGroupCount;
    std::vector<int> fieldGroup;
    std::vector<int> groupSize;

    std::vector<int> fieldFunction;

public:

    CSVSink(std::string format, std::string delimiter, std::string terminator)
        : delimiter(delimiter), terminator(terminator)
    {
        formatType = OUTPUT_JSON;

        for (std::string header : splitString(format, ';'))
        {
            // kafka headers
            if (header == std::string("partition"))
            {
                headerFields.push_back(HEADER_PARTITION);
            }
            else if (header == std::string("offset"))
            {
                headerFields.push_back(HEADER_OFFSET);
            }
            else if (header == std::string("key"))
            {
                headerFields.push_back(HEADER_KEY);
            }
            else if (header == std::string("timestamp"))
            {
                headerFields.push_back(HEADER_TIMESTAMP);
            }
            // value format
            else if (header == std::string("json"))
            {
                formatType = OUTPUT_JSON;
            }
            else if (header == std::string("eav"))
            {
                formatType = OUTPUT_EAV;
            }
            else if (header.substr(0, 5) == std::string("array"))
            {
                formatType = OUTPUT_ARRAY;
                parseFields(header.substr(6), true);
            }
            else if (header.substr(0, 7) == std::string("columns"))
            {
                formatType = OUTPUT_COLUMNS;
                parseFields(header.substr(8), false);
            }
        }
    }

    void parseFields(const std::string &format, bool defaultGroupping)
    {
        std::vector<std::string> groups;
        std::unordered_map<std::string, std::vector<std::pair<std::string, int> > > groupFields;

        std::vector<std::string> keys = splitString(format, ',');
        for (size_t j = 0; j < keys.size(); ++j)
        {
            std::vector<std::string> keyParts = splitString(keys[j], '=');
            std::string expr = keyParts[0];

            std::string key;
            int transform;
            if (expr.find('(') != std::string::npos)
            {
                std::vector<std::string> exprTok = splitString(expr, '(');
                if (exprTok[0] == std::string("hash"))
                {
                    transform = HASH;
                }
                else
                {
                    vt_report_error(0, "unkown funtion");
                }
                key = splitString(exprTok[1], ')')[0];
            }
            else
            {
                transform = IDENTICAL;
                key = expr;
            }
//            jsonFields.push_back(key);
//            fieldFunction.push_back(transform);

            std::string group = keyParts.size() > 1 ? keyParts[1] : (defaultGroupping ? "" : key);
            auto it = groupFields.find(group);
            if (it == groupFields.end())
            {
                it = groupFields.insert(std::make_pair(group, std::vector<std::pair<std::string,int> >())).first;
                groups.push_back(group);
            }
            it->second.push_back(std::make_pair(key, transform));
//            jsonFieldIndex[key] = j;
//            fieldGroup.push_back(it->second);
        }

        for (size_t k = 0; k < groups.size(); ++k)
        {
            std::vector<std::pair<std::string,int> > fields = groupFields[groups[k]];
            for (size_t f = 0; f < fields.size(); ++f)
            {
                jsonFieldIndex[fields[f].first] = jsonFields.size();
                jsonFields.push_back(fields[f].first);
                fieldFunction.push_back(fields[f].second);
                fieldGroup.push_back(k);
            }
            groupSize.push_back(fields.size());
        }
        fieldGroupCount = groups.size();
    }

    void put(Message &doc)
    {
        std::cout << toRecord(doc);
    }

    void flush()
    {
    }

    std::string toRecord(Message &doc)
    {
        if (formatType == OUTPUT_JSON)
        {
            return toJSONRecord(doc);
        }
        else if (formatType == OUTPUT_EAV)
        {
            return toEAVRecord(doc);
        }
        else if (formatType == OUTPUT_ARRAY)
        {
            return toArrayRecordComp(doc);
        }
        else if (formatType == OUTPUT_COLUMNS)
        {
            return toSparseRecord(doc);
        }
        return std::string();
    }

    inline std::string toRecordHeader(Message &doc)
    {
        std::ostringstream s;

        for (int headerField : headerFields)
        {
            if (headerField == HEADER_PARTITION)
            {
                s << doc.get_partition();
                s << delimiter;
            }
            else if (headerField == HEADER_OFFSET)
            {
                s << doc.get_offset();
                s << delimiter;
            }
            else if (headerField == HEADER_KEY)
            {
                s << doc.get_key();
                s << delimiter;
            }
            else if (headerField == HEADER_TIMESTAMP)
            {
                s << rd_kafka_message_timestamp(doc.get_handle(), NULL);
                s << delimiter;
            }
        }

        return s.str();
    }

    inline std::string evalValue(const std::string &s, int transform)
    {
        if (transform == HASH)
        {
            int64 i = vhash_impl(s.data(), s.length());
            std::ostringstream ss;
            ss << i;
            return ss.str();
        }
        return s;
    }

    std::string toSparseRecord(Message &doc)
    {
        int totalSize = 0;
        const int fieldCount = jsonFields.size();
        std::vector<std::pair<const char*, int> > values(fieldCount, make_pair("", 0));

        static char transformBuffer[1024];
        int transformOffset = 0;

        jsmn_parser p;
        jsmntok_t t[4098];

        jsmn_init(&p);
        const char *src = (const char *)doc.get_payload().get_data();
        int len = doc.get_payload().get_size();
        int r = jsmn_parse(&p, src, len, t, 4098);
        for (int i = 1; i < r - 1; ++i)
        {
            if (t[i].type == JSMN_STRING && t[i].parent == 0 && t[i + 1].parent == i)
            {
                std::string key(src + t[i].start, t[i].end - t[i].start);

                auto found = jsonFieldIndex.find(key);
                if (found != jsonFieldIndex.end())
                {
                    int ind = found->second;
                    if (fieldFunction[ind] == HASH)
                    {
                        int64 v = vhash_impl(src + t[i + 1].start, t[i + 1].end - t[i + 1].start);
                        int len = sprintf(transformBuffer + transformOffset, "%lld", v);
                        values[ind] = make_pair(transformBuffer + transformOffset, len);
                        transformOffset += len;
                        totalSize += len;
                    }
                    else if (fieldFunction[ind] == IDENTICAL)
                    {
                        values[ind] = make_pair(src + t[i + 1].start, t[i + 1].end - t[i + 1].start);
                        totalSize += t[i + 1].end - t[i + 1].start;
                    }
                }
            }
        }

        const char arrayDelim = ',';
        const char csvDelim = delimiter[0];
        const char defaultDelim = arrayDelim;

        static const int sizeMax = 65000;
        static char buf[sizeMax];
        memset(buf, defaultDelim, min(sizeMax, totalSize + fieldCount + fieldGroupCount * 2));
        int shift = 0;

        int group = 0;
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (fieldGroup[i] != group)
            {
                if (groupSize[group] > 1)
                {
                    buf[shift - 1] = '}';
                    if (defaultDelim != csvDelim)
                    {
                        buf[shift] = csvDelim;
                    }
                    shift += 1;
                }

                if (groupSize[fieldGroup[i]] > 1)
                {
                    buf[shift] = '{';
                    shift += 1;
                }
                group = fieldGroup[i];
            }

            if (values[i].second > 0 && values[i].second + (fieldCount - i - group * 2) < sizeMax)
            {
                memcpy(buf + shift, values[i].first, values[i].second);
                shift += values[i].second;
            }

            if (defaultDelim != arrayDelim && groupSize[group] > 1)
            {
                buf[shift] = arrayDelim;
            }
            else if (defaultDelim != csvDelim && groupSize[group] <= 1)
            {
                buf[shift] = csvDelim;
            }
            shift += 1;
        }
        if (groupSize[group] > 1)
        {
            buf[shift - 1] = '}';
            shift += 1;
        }

        std::string res = toRecordHeader(doc);
        res += std::string(buf, max(0, shift - 1));
        return res + terminator;
    }

    std::string toArrayRecordComp(Message &doc)
    {
        std::string j(doc.get_payload());

        Reader reader;
        SAXJsonParser parser;
        parser.init();

        StringStream ss(j.c_str());
        reader.Parse<kParseNumbersAsStringsFlag>(ss, parser);

        std::vector<std::string> fieldValues(jsonFields.size(), std::string());
        int fieldCount = 0;

        for (auto i = parser.data.begin(); i != parser.data.end() && fieldCount < MAX_FIELD_COUNT; ++i)
        {
            auto found = jsonFieldIndex.find(std::string(i->first));
            if (found != jsonFieldIndex.end())
            {
                int ind = found->second & 0xffff;
                fieldValues[ind] = evalValue(toString(i->second), fieldFunction[ind]);
                fieldCount += 1;
            }
        }

        std::vector<std::string> valueGroups(fieldGroupCount, std::string());
        std::vector<bool> groupIsOpen(fieldGroupCount, false);

        const int indexCapacity = std::min(int(fieldValues.size()), MAX_INDEX_SIZE);
        static char buf[MAX_INDEX_SIZE * 2];
        memset(buf, '0', indexCapacity * 2);

        int indexShift = 0;
        for (int i = 0; i < indexCapacity; ++i)
        {
            int group = fieldGroup[i];

            if (fieldValues[i].size() == 0)
            {
                if (groupIsOpen[group])
                {
                    groupIsOpen[group] = false;
                    int offset = valueGroups[group].size();
                    intToHex(encode256(align256(offset, 0, 128), 128), buf + indexShift, 1);
                }
                indexShift += 2;
                continue;
            }

            int offset = valueGroups[group].size();
            int size = fieldValues[i].size();

            int offsetAligned = align256(offset, size, 128);
            valueGroups[group] += std::string(offsetAligned - offset, ' ');
            valueGroups[group] += fieldValues[i];

            intToHex(encode256(offsetAligned, 128), buf + indexShift, 1);
            indexShift += 2;
            groupIsOpen[group] = true;
        }

        std::string res = toRecordHeader(doc);
        res += std::string(buf, buf + indexShift);
        for (size_t i = 0; i < valueGroups.size(); ++i)
        {
            res += delimiter + valueGroups[i];
        }
        return res + terminator;
    }

    std::string toJSONRecord(Message &doc)
    {
        std::string j(doc.get_payload());
        return toRecordHeader(doc) + j + terminator;
    }

    std::string toEAVRecord(Message &doc)
    {
        std::string h = toRecordHeader(doc);
        std::string j(doc.get_payload());

        Reader reader;
        SAXJsonParser parser;

        parser.init();

        StringStream ss(j.c_str());
        reader.Parse<kParseNumbersAsStringsFlag>(ss, parser);

        std::string line;
        for (auto it = parser.data.begin(); it != parser.data.end(); ++it)
        {
            line += h + it->first + delimiter + toString(it->second) + terminator;
        }
        return line;
    }

};

// kafka consumer

struct PartitionTask
{
    PartitionTask() : partition(INT_MAX), offset(-1), limit(0), isNull(true), commit(false) {}

    int partition;
    int64 offset;
    int64 limit;
    bool isNull;
    bool commit;
};

class KafkaSource
{
    Consumer *consumer;

    const std::string brokers, topic, group;
    std::vector<PartitionTask> partitions;
    int64 limit;

    Sink *sink;

    bool timedout;
    int duration;
    bool subscribe;

public:
    KafkaSource(std::string brokers, std::string topic, std::string partitions, std::string group, Sink *sink)
        : consumer(NULL), brokers(brokers), topic(topic), group(group), limit(0), sink(sink)
    {
        parsePartitions(partitions);
    }

    void setup()
    {
        this->subscribe = partitions.size() == 1 && partitions[0].partition == REBALANCE_PARTITIONS;
        const bool autocommit = subscribe && REBALANCE_AUTOCOMMIT;

        this->timedout = false;
        this->duration = 0;

        Configuration config = {
            { "metadata.broker.list", brokers },
            { "group.id", group },
            { "enable.auto.commit", autocommit },
            { "enable.auto.offset.store", autocommit },
            { "auto.offset.reset", subscribe ? "earliest" : "error" },
        };
        consumer = new Consumer(config);

        int topicPartitionCount = getPartitionCount(consumer->get_handle(), topic.c_str());
        if (partitions.size() == 1 && (partitions[0].partition == ALL_PARTITIONS ||
                                       partitions[0].partition == REBALANCE_PARTITIONS))
        {
            limit = partitions[0].limit;
            std::vector<PartitionTask> allPartitions;
            for (int p = 0; p < topicPartitionCount; ++p)
            {
                PartitionTask t;
                t.isNull = false;
                t.partition = p;
                t.offset = partitions[0].offset;
                t.limit = 0;
                t.commit = partitions[0].commit && !autocommit;
                allPartitions.push_back(t);
            }
            partitions = allPartitions;
        }

        if (subscribe)
        {
            consumer->set_assignment_callback([&](const TopicPartitionList& partitions) {

                std::vector<PartitionTask> assignedPartitions;
                int partitionCount = 1;

                for (TopicPartition p : partitions)
                {
                    PartitionTask t;
                    t.isNull = false;
                    t.partition = p.get_partition();
                    t.offset = -1000;
                    t.limit = 0;
                    t.commit = !REBALANCE_AUTOCOMMIT;
                    assignedPartitions.push_back(t);

                    partitionCount = max(partitionCount, t.partition + 1);
                    fprintf(stderr, "partition %d: rebalance assign %s\n", t.partition, t.commit ? "manual commit" : "autocommit");
                }

                this->partitions.clear();
                this->partitions.resize(partitionCount);
                for (PartitionTask p : assignedPartitions)
                {
                    this->partitions[p.partition] = p;
                }
            });

            consumer->set_revocation_callback([&](const TopicPartitionList& partitions) {
                this->commitOffsets();
            });

            consumer->subscribe({ topic });
        }
        else
        {
            TopicPartitionList offsets;
            for (PartitionTask &p : partitions)
            {
                p.isNull = p.isNull || p.partition < 0 || p.partition >= topicPartitionCount;
                if (!p.isNull)
                {
                    offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                    fprintf(stderr, "partition %d: assign %lld limit %lld\n", p.partition, p.offset, p.limit);
                }
            }

            consumer->assign(offsets);
        }
    }

    void destroy()
    {
        if (subscribe)
        {
            consumer->unsubscribe();
        }
    }

    virtual void process()
    {
        int64 currentLimit = getCurrentLimit();
        //fprintf(stderr, "Consuming messages from topic %s limit %lld", topic.c_str(), currentLimit);
        int batch_size = max(0, (int) min(currentLimit, 100000LL));

        while ((getCurrentLimit() > 0 && !timedout && duration < 2 * POLL_TIMEOUT))
        {
            auto start = std::chrono::steady_clock::now();
            std::vector<Message> msgs = consumer->poll_batch(batch_size, std::chrono::milliseconds(POLL_TIMEOUT));
            auto end = std::chrono::steady_clock::now();

            auto d = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            timedout = (d >= POLL_TIMEOUT);
            duration += d;

            fprintf(stderr, "%lu messages polled: %ld ms poll, %d ms total\n", msgs.size(), d, duration);
            for (Message &msg: msgs)
            {
                if (!msg)
                {
                    continue;
                }

                if (msg.get_error() && !msg.is_eof())
                {
                    fprintf(stderr, "error recieived: %s\n", msg.get_error().to_string().c_str());
                    continue;
                }

                int part = msg.get_partition();

                partitions[part].limit -= 1;
                partitions[part].offset = msg.get_offset() + 1;

                sink->put(msg);
                limit -= 1;
            }
        }

        fprintf(stderr, timedout || duration > 2 * POLL_TIMEOUT ? "timeout\n" : "limit exceeded\n");
        sink->flush();
        commitOffsets();
    }

private:

    int getPartitionCount(rd_kafka_t* rdkafka, const char *topic_name)
    {
        int partitionCount = 0;

        rd_kafka_topic_t *rdtopic = rd_kafka_topic_new(rdkafka, topic_name, 0);
        const rd_kafka_metadata_t *rdmetadata;

        rd_kafka_resp_err_t err = rd_kafka_metadata(rdkafka, 0, rdtopic, &rdmetadata, 30000);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            for (int i = 0; i < rdmetadata->topic_cnt; ++i)
            {
                partitionCount = rdmetadata->topics[i].partition_cnt;
            }
            rd_kafka_metadata_destroy(rdmetadata);
        }

        rd_kafka_topic_destroy(rdtopic);

        return partitionCount;
    }

    void parsePartitions(const std::string &partFmt)
    {
        std::map<std::string, int> partConsts;
        partConsts["*"] = ALL_PARTITIONS;
        partConsts["%"] = REBALANCE_PARTITIONS;

        int partitionCount = 1;

        std::vector<std::string> fmtParts = splitString(partFmt, ',');

        std::vector<PartitionTask> partitionList;
        for (std::string part : fmtParts)
        {
            std::vector<std::string> tuple = splitString(part, ':');

            if (tuple.size() != 3)
            {
                vt_report_error(0, "partition format missmatch: [partition:offset:limit](,[partition:offset:limit])*");
            }

            PartitionTask t;
            t.isNull = false;
            t.partition = partConsts.count(tuple[0]) ? partConsts[tuple[0]] : toInt(tuple[0], -1);
            t.offset = toInt(tuple[1]);
            t.limit = toInt(tuple[2]);
            t.commit = t.offset < 0;

            if (t.partition < 0 && tuple[0] != std::string("*") && tuple[0] != std::string("%"))
            {
                vt_report_error(0, "partition number must be integer, '*' or '%'");
            }
            else if (t.partition < 0 && fmtParts.size() > 1)
            {
                vt_report_error(0, "only one partition clause is expected for '*' or '%'");
            }
            else if (t.offset == NOTINT || (t.offset < 0 && t.offset != -1 && t.offset != -2 && t.offset != -1000))
            {
                vt_report_error(0, "partition offset must be positive integer or -1 for latest or -2 for earlest or -1000 for last read");
            }
            else if (t.partition == REBALANCE_PARTITIONS && t.offset != -1000)
            {
                vt_report_error(0, "subscribe is only available with offset -1000 (last read)");
            }
            else if (t.limit == NOTINT || t.limit < 0)
            {
                vt_report_error(0, "partition limit must be positive integer");
            }
            else
            {
                partitionList.push_back(t);
                partitionCount = max(partitionCount, t.partition + 1);
            }
        }

        if (partitionCount == 1 && (partitionList[0].partition == ALL_PARTITIONS ||
                                    partitionList[0].partition == REBALANCE_PARTITIONS))
        {
            partitions = partitionList;
        }
        else
        {
            partitions.resize(partitionCount);
            for (PartitionTask p : partitionList)
            {
                partitions[p.partition] = p;
            }
        }
    }

    int64 getCurrentLimit() const
    {
        int64 partitionLimit = 0;
        for (PartitionTask p : partitions)
        {
            if (!p.isNull && p.limit > 0)
            {
                partitionLimit += p.limit;
            }
        }
        return max(0LL, max(limit, partitionLimit));
    }

    void commitOffsets()
    {
        TopicPartitionList offsets;
        for (PartitionTask &p : partitions)
        {
            if (!p.isNull && p.commit && p.offset > 0)
            {
                offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                p.offset = -1;
            }
        }
        if (!offsets.empty())
        {
            consumer->store_offsets(offsets);
            consumer->commit(offsets);
        }
    }
};

int main()
{
    //"uuid:string,u:string,uid:integer,eid:integer,dt:timestamp,dtm:timestamp,src_id:integer,src:integer,ua:string,err:integer,ip:string[],ipg:integer,new:boolean,post:boolean,ref:string[],v:integer,ab:string[],bot:boolean,du:string[],geo:array_float[],url:string[],amqp:boolean,app:integer,test:string[],clos:boolean,oc:boolean,bt:integer,email:string[],bool_param:boolean,int_param:integer,rqid:string[],oldEmail:string[],lid:integer,rcid:integer,cid:integer,q:string[],offset:integer,limit:integer,total:integer,shops:integer,oid:integer,ak:string[],contact:string[],contactType:string[],uniqueLink:string[],operatingSystem:string[],event:object[],mcid:integer,aid:integer,buid:integer,iid:integer,mid:string[],fid:integer,tx:string[],crm_eventtype_id:string[],st:integer,uem:string[],type:string[],vin:string[],string_param:string[],location_id:integer,params:array_string[],item_id:integer,event_code:string[],categoryid:integer,campaign_id:string[],hide_phone:boolean,description:string[],group_id:string[],launch_id:string[],phone:string[],userType:integer,smsId:string[],manager_id:integer,esid:string[],sid:integer,name:string[],email2:string[],snid:integer,title:string[],price:number,photos:integer,photos_ids:array_int[],nomail:boolean,timestamp:integer,user_id:integer,errors:array_string[],ax:string[],engine:string[],puid:integer,ignored:boolean,msg:string[],manager:string[],addr:string[],phone1:string[],inn:integer,kpp:integer,addr2:string[],addr1:string[],addrcp:boolean,token:string[],success:boolean,source:integer,machine:boolean,survey_id:integer,engine_version:string[],iids:array_int[],lupint:integer,surv_impid:string[],quick:boolean,cmm:string[],from:string[],status_id:integer,east:boolean,srid:integer,event_group:string[],siid:integer,soid:integer,typeid:integer,cdt:timestamp[],position:integer,adtyped:string[],reason:string[],diid:integer,dvas_applied:boolean,items:integer,news:boolean,notice:boolean,reminders:boolean,android_data_size_total:integer,android_data_size_cache:integer,android_data_size_databases:integer,android_data_size_files:integer,tid:integer,android_data_size_shared_prefs:integer,android_data_size_lib:integer,life_time:integer,android_data_size_app_webview:integer,android_data_size_no_backup:integer,android_data_size_code_cache:integer,vsrc:string[],imgid:integer,imgoid:integer,imgu:string[],x:string[],cm:string[],ida:boolean,errors_detailed:string[],ops:integer,similars:integer,result:string[],cp:boolean,link:string[],desc:string[],sdesc:string[],video:string[],public_stats:boolean,tages:array_int[],srcId:integer,shopId:integer,prob:array_float[],subscribe:boolean,screen:string[],bids:array_float[],cpm_floor:number,ids:array_int[],vid:integer,model:integer,iin:boolean,pctrs:array_float[],reduce:integer,spid:integer,blockcode:integer,bid:integer,rules:string[],domain:string[],fnum:integer,lf_cnt:integer,material:string[],ptype:string[],orderid:integer,ctid:integer,cids:array_int[],img:integer,complete:integer,pctr:number,tariff:integer,lflid:integer,additional_user:integer,pagetype:string[],sum:array_int[],tpl:string[],lang:string[],hash:string[],msgid:integer,sendtime:integer,utmc:string[],subject:string[],vol:integer,icnt:integer,s:string[],did:array_int[],lids:array_int[],pmax:integer,pmin:integer,priceunit:string[],propertytype:string[],amnt:integer,lo:integer,lf_dt:integer,lf_grp:integer,ie:boolean,cost:number,dirty:integer,redirect:boolean,city:string[],cname:string[],photo_ids:array_int[],ts:integer,serviceid:integer,at:integer,file:string[],stack:array_string[],meta:string[],pid:integer,shortcut:integer,lq:string[],mdistance:string[],t:string[],from_page:string[],dname:string[],id:integer,lup:integer,ls:string[],p:integer,consent_act:string[],snname:string[],chid:string[],recipient:string[],version:integer,cprice:number,sku:string[],lf_type:string[],packageid:integer,uids:array_int[],ex:integer,catid:integer,rid:integer,response:string[],dpid:integer,hctr:number,listingfees:string[],microcategoryid:integer,sort:integer,icu:boolean,isa:boolean,itl:boolean,hour:integer,levid:integer,brid:string[],lc:string[],lr:string[],page:string[],uloggrd:boolean,ar:string[],dsp:string[],bpos:string[],adid:string[],adomain:array_string[],delete:boolean,repeat:boolean,copy:boolean,area:number,lgid:integer,floor:integer,result_state:integer,landscape:boolean,portrait:boolean,is_company:boolean,inca:boolean,reg_config:string[],suid:integer,f:string[],message_id:integer,screen_code:integer,_url:string[],menu_item:array_string[],lf_cost:integer,vas_list:array_int[],last_contact:integer,sat:integer,operationid:integer,paysysid:integer,step:integer,vasid:integer,lastpaysysid:integer,onmap:integer,wmap:boolean,rooms:string[],sgtd:integer,searchid:integer,soldbytype:string[],ownid:integer,iafi:boolean,query:string[],qs:number,device_id:string[],subscriptionid:integer,ua_browser:string[],budget:number,social_id:integer,wizard_id:string[],userid:integer,usert:integer,vasfactid:integer,ctotalprice:number,error:integer,ofid:integer,push_token:string[],ua_device:string[],ua_os:string[],ulogged:boolean,wcid:integer,wmid:integer,finish:integer,leid:array_int[],liid:array_int[],start:integer,beh:string[],google_play_services_library_version_code:integer,google_play_services_version_name:string[],chatid:string[],locationid:integer,sortd:string[],paystat:integer,emee_id:integer,emey_id:integer,irs:string[],reschid:string[],souid:integer,ibofp:integer,ise:integer,click_max_tm:string[],clicks:integer,expenses:number,impr_max_tm:string[],imprs:integer,lines:integer,mtime:string[],size:integer,dupstatus:integer,google_play_services_version_code:integer,simulator:boolean,dupid:integer,shortcut_id:string[],csids:string[],dmm:string[],dsids:string[],pcid:integer,cpid:integer,banner_type:string[],auid:integer,vas_type:string[],subjid:integer,abuseid:integer,tsid:integer,google_play_services_compatibility:boolean,google_play_services_library_version_name:string[],notification_id:string[],review:string[],fieldsmaf:string[],assistive_touch_running:boolean,ac:integer,cb:string[],cy:string[],pc:integer,rt:integer,uph:string[],is_official_nd:boolean,status:string[],df_start_time:integer,df_expire_time:integer,status_message:string[],block_details:string[],zk:boolean,test_field_slug:array_string[],unm:string[],bold_text_enabled:boolean,isupgrade:boolean,tmid:integer,sortf:string[],dmessage_id:integer,ap:boolean,body:array_string[],route:string[],sids:string[],response_id:integer,expired:boolean,result_status:integer,result_time:integer,answer_text:string[],answer_points:integer,survey_version:integer,is_test:boolean,srcp:string[],accessibility_talkback:boolean,photo_library_permission:string[],screen_brightness:number,screen_dpi:integer,screen_height_dp:integer,screen_width_dp:integer,count:integer,app_height_dp:integer,cnt:string[],mcids:array_int[],str:string[],words:array_string[],dlvritem:integer,offdelivery:boolean,level:integer,pillarid:integer,isb2c:boolean,isdlvr:boolean,extt:integer,contact_type:string[],accessibility_haptic:boolean,accessibility_is_on:boolean,accessibility_visual:boolean,app_width_dp:integer,camera_permission:string[],configured_default_mail_client:boolean,darker_system_colors:boolean,disk_capacity:number,energy_efficiency:number,force_touch:boolean,free_disk_space:number,grayscale:boolean,guided_access:boolean,invert_colors:boolean,jailbreak:boolean,location_permission:string[],low_power_mode:boolean,manufacturer:string[],memory_warning_count:integer,platform_payment:string[],push_notification_permission:string[],reduce_motion:boolean,reduce_transparency:boolean,resources_size:number,shake_to_undo:boolean,speak_screen:boolean,speak_selection:boolean,startup_item:string[],startup_reason:string[],startup_time:number,switch_control:boolean,terminate_reason:string[],voice_over:boolean,watch_pair:boolean,fprice:number,bsize:array_string[],duration:integer,tab:integer,action:string[],avatar_id:integer,moderator_id:integer,resolution:array_int[],ever:integer,original:string[],show:boolean,abuseids:array_int[],mpact:integer,is_seller:boolean,op_dt:integer,ns_channel:string[],ns_type:string[],ns_value:boolean,is_user_auth:boolean,lf_lim:integer,dev_diid:integer,duid:integer,dpush_id:string[],push_opened:boolean,dpush_delivered:boolean,dpush_error:string[],delivery:string[],user_key:string[],stid:integer,active_icnt:integer,closed_icnt:integer,im:boolean,avatar_exist:boolean,sgtx:string[],sgt_tab:string[],sgt1:string[],sgt2:string[],sgt3:string[],sgt4:string[],sgt5:string[],sgt6:string[],sgt1_mcids:array_int[],sgt2_mcids:array_int[],sgt3_mcids:array_int[],sgt4_mcids:array_int[],sgt5_mcids:array_int[],sgt6_mcids:array_int[],sgtp:integer,is_legit:boolean,sgt7:string[],sgt8:string[],sgt9:string[],sgt7_mcids:array_int[],sgt8_mcids:array_int[],sgt9_mcids:array_int[],x_trace:string[],sesid:string[],sgt:array_string[],sgt_mcids:array_int[],emotion_id:integer,objectid:string[],objecttype:string[],extension:string[],region_id:integer,is_geo:boolean,radius:integer,geo_lat:float,geo_lng:float,geo_acc:integer,geo_time:integer,bspage:integer,is_antihack_phone_confirm:boolean,antihack_confirm_number:integer,antihack_user_password_reset:boolean,color:string[],exif_model:string[],exif_make:string[],str_available:boolean,create_time:integer,update_time:integer,app_type:string[],iscredit:boolean,cperiod:integer,geo_acc_code:integer,ver:string[],is_delivery:boolean,width:float,height:float,admuserid:integer,dur:float,premoderation:boolean,fraud_code_ids:array_int[],comparison_id:integer,duplicate_dur:float,duplicate_cand:object[],fsc:integer,target_id:integer,afraud_version:integer,login_type:string[],is_first_message:boolean,time_to_interact:string[],network_type:string[],date:string[],plate_number:string[],engine_power:string[],transport_type:string[],transport_category:string[],timing:float,timing_duplicate:float,interface_version:string[],admuser_id:integer,duplicate_candidates:object[],connection_speed:float,time_to_content:string[],screen_id:integer,promo:string[],exception_id:string[],duplicates:array_string[],to:string[],abs:array_string[],safedeal:string[],content:string[],login:string[],num_p:integer,dl:boolean,subtype:string[],is_verified_inn:boolean,srd:string[],form_input_field_name:string[],form_input_field_value:string[],channels:array_string[],notification:integer,android_data_size_apk:integer,channels_deny:array_string[],target_type:integer,target:string[],reg_uids:array_int[],cdtm:timestamp[],pmid:integer,datefrom:string[],dateto:string[],subs_vertical:string[],base_item_id:integer,reject_wrong_params:array_int[],state_id:string[],parentstate_id:string[],from_block:integer,from_position:integer,screen_items_viewed:integer,screen_items_ad_viewed:integer,screen_items_vip_viewed:integer,screen_items_xl_viewed:integer,screen_items_common_viewed:integer,screen_items_new_viewed:integer,screen_items_geo_viewed:integer,screen_items_rec_viewed:integer,service_id:array_int[],amount:array_float[],operation_id:array_int[],uris:array_string[],isp:boolean,block_total:integer,page_total:integer,block_items_display:integer,block_items_added:integer,is_anon:boolean,search_area:array_float[],serpdisplay_type:string[],landing_type:string[],retry:boolean,browser_web_push_agreement:boolean,is_auth:boolean,snippet_placement:string[],vasid_prev:integer,snippet_type:string[],_ga:string[],tooltip_id:integer,resultTime:integer,devDiid:integer,advtid:string[],sm:boolean,consentAct:string[],answerPoints:integer,isOfficialNd:boolean,responseId:string[],ach:boolean,premiums:integer,dfStartTime:integer,peid:integer,answerText:string[],resultStatus:integer,dmessageId:integer,surveyVersion:integer,pdrid:integer,dfExpireTime:integer,usedSuggest:boolean,isTest:boolean,platformId:integer,ppeid:integer,surv_id:integer,userToken:string[],img_id:integer,vips:integer,d:boolean,surveyId:integer,uas:integer,sr:string[],groupId:integer,launchId:integer,unique_args:object[],data:object[],talkback:boolean,model_version:string[],subs_price_bonus:float,subs_price_packages:float,subs_price_ext_shop:float,subs_price_total:float,with_shop_flg:boolean,abp:integer,apply_status:string[],has_cv:boolean,phone_request_flg:boolean,msg_image:string[],valid_until:integer,discount_type:string[],rating:float,reviews_cnt:integer,review_id:integer,ssv:integer,user_auth_suggestionshown:boolean,share_place:string[],s_trg:integer,user_authmemo_id:integer,user_auth_memo_id:string[],call_id:string[],is_linked:boolean,rdt:string[],notification_message_id:string[],cnt_default_config:integer,cnt_default_items:integer,default_vas_amount:integer,adm_comment:string[],item_version:integer,is_adblock:boolean,is_phone_new:boolean,helpdesk_agent_state:string[],anonymous_number_service_responce:string[],additional_item:integer,autoload_operation_activate:boolean,autoload_operation_add:boolean,autoload_operation_fee:boolean,autoload_operation_remove:boolean,autoload_operation_stop:boolean,autoload_operation_update:boolean,autoload_operation_vas:boolean,is_refresh:boolean,item_in_moderation:boolean,raw_body:object[],call_action_id:integer,call_action_type:string[],order_cancel_cause:integer,order_cancel_cause_txt:string[],is_verified:boolean,other_phones_number:integer,item_number:integer,phone_action:string[],is_reverified:boolean,phone_action_type:string[],order_cancel_cause_info:string[],selected_suggest_value:string[],i:object[],wrong_phone:boolean,subs_form_type:string[],review_seq_id:integer,errorsDetailed:string[],subs_vertical_ids:array_int[],subs_tariff_id:integer,subs_proposed_bonus_factor:float,subs_extensions:array_string[],subs_packages:object[],caller_phone:string[],caller_user_type:string[],success_url:string[],rent_start_datetime:string[],rent_end_datetime:string[],message_preview:string[],from_notification_channel:string[],car_doors:string[],engine_type:string[],transmission:string[],car_drive:string[],steering_wheel:string[],engine_volume:string[],item_status:string[],aggregation:string[],article_dissatisfaction_reason:string[],domofond_notification_channel:string[],domofond_subscription_type:string[],dnotification_message_id:integer,dnotification_delivered:boolean,dnotification_error:string[],anonymous_phone_number:string[],isbp:boolean,slider_type:string[],redirect_incoming_call_to:string[],anonymous_phone:string[],placement:string[],destination:string[],action_type:string[],from_source:string[],cnt_favourites:integer,clickstream_request_id:string[],ces_score:integer,ces_context:string[],ref_id:string[],parent_ref_id:string[],run_id:string[],pvas_group_old:integer,pvas_group:integer,pvas_groups:array_int[],page_view_session_time:integer,tab_id:string[],ctcallid:integer,is_geo_delivery_widget:boolean,api_path:string[],antihack_reason:string[],buyer_booking_cancel_cause:array_int[],buyer_booking_cancel_cause_text:string[],ticket_comment_id:integer,ticket_comment_dt:integer,src_split_ticket_id:integer,trgt_split_ticket_id:integer,time_to_first_byte:integer,time_to_first_paint:integer,time_to_first_interactive:integer,time_to_dom_ready:integer,time_to_on_load:integer,time_to_target_content:integer,time_for_async_target_content:integer,avito_advice_article:string[],payerid:string[],changed_microcat_id:integer,scenario:string[],userAgent:string[],cv_use_category:boolean,cv_use_title:boolean,requestParams:object[],inputValue:string[],inputField:string[],offerId:integer,cityId:integer,regionId:integer,classifiedId:integer,feedId:integer,clientId:integer,clientNeedId:integer,lifeTime:integer,leadId:integer,memberId:integer,executiveMemberId:integer,executiveType:string[],optionsNumber:integer,str_buyer_contact_result:string[],str_buyer_contact_result_reason:array_int[],str_buyer_contact_result_reason_text:string[],second:float,profile_tab:string[],is_original_user_report:boolean,autoteka_user_type:integer,list_load_number:integer,ssid:integer,screen_orientation:string[],recommendpaysysid:array_int[],parent_uid:integer,app_version:string[],os_version:string[],accounts_number:integer,search_correction_action:string[],search_correction_original:string[],search_correction_corrected:string[],search_correction_method:string[],autosort_images:boolean,cnt_subscribers:integer,is_oasis:boolean,xl:boolean,pvas_dates:array_string[],oneclickpayment:boolean,call_reason:integer,status_code:integer,api_dt:integer,project:string[],location_text_input:string[],cadastralnumber:string[],report_duration:integer,report_status:boolean,other_shop_items:array_int[],page_number:integer,phone2:string[],skype:string[],shop_url:string[],shop_name:string[],shop_site:string[],short_description:string[],long_description:string[],work_regime:string[],info_line:string[],shop_logo:string[],shop_branding:string[],shop_back:string[],shop_photos:array_string[],on_avito_since:integer,ssfid:integer,deep_link:string[],item_add_screen:string[],user_ids:array_int[],empl_id:integer,empl_invite_state:string[],prev_page:string[],shop_fraud_reason_ids:array_int[],shop_moderation_action_id:integer,shop_moderation_action_hash:string[],service_name:string[],deploy_env:string[],cid_tree_branch:array_int[],lid_tree_branch:array_int[],cur_i_status_id:integer,cur_i_status_time:integer,item_version_time:integer,infomodel_v_id:string[],checkbox_an_enable:boolean,message_type:string[],app_version_code:string[],banner_id:string[],shortcut_description:string[],close_timeout:integer,note_text:string[],item_parameter_name:string[],block_id:string[],selling_system:string[],banner_code:string[],wsrc:string[],shops_array:array_int[],rec_item_id:integer,lfpackage_id:integer,subscription_promo:boolean,sub_is_ss:boolean,sub_prolong:string[],target_page:string[],mobile_event_duration:integer,screen_session_uid:string[],screen_name:string[],content_type:string[],mobile_app_page_number:integer,roads:array_int[],reload:boolean,img_download_status:boolean,screen_start_time:integer,service_branch:string[],sgt_cat_flag:boolean,notification_owner:string[],notification_type:string[],ns_owner:string[],operation_ids:array_int[],software_version:string[],build:string[],adpartner:integer,ticket_channel:integer,adslot:string[],statid:integer,is_payed_immediately:boolean,lfpackage_ids:array_int[],current_subs_version:string[],new_subs_version:string[],subscription_edit:boolean,channel_number:integer,channels_screen_count:integer,domofond_item_type:string[],domofond_item_id:string[],item_position:integer,delivery_help_question:string[],domofond_cookie:string[],domofond_user_token:string[],banner_due_date:integer,banner_show_days:boolean,banner_item_id:integer,empty_serp_link_type:string[],domofond_proxy_request_name:string[],chat_error_case:string[],has_messages:boolean,search_address_type:string[],domofond_user_id:integer,js_event_type:string[],dom_node:string[],js_event_slug:string[],attr_title:string[],attr_link:string[],attr_value:string[],key_name:string[],is_ctrl_pressed:boolean,is_alt_pressed:boolean,is_shift_pressed:boolean,color_theme:string[],dom_node_content:string[],is_checkbox_checked:boolean,page_x_coord:integer,page_y_coord:integer,srd_initial:string[],last_show_dt:integer,domofond_button_name:string[],uploaded_files_cnt:integer,review_additional_info:string[],flow_type:integer,flow_id:string[],ces_article:integer,items_locked_count:integer,items_not_locked_count:integer,word_sgt_clicks:integer,metro:array_int[],msg_app_name:string[],msg_request:string[],domofond_category:string[],domofond_subcategory:string[],domofond_filter_region:string[],domofond_filter_location:string[],domofond_filter_rooms:string[],domofond_filter_price_from:string[],domofond_filter_price_up:string[],domofond_filter_rent_type:string[],domofond_search_result_type:string[],moderation_user_score:float,domofond_search_position:integer,domofond_saved_search_result:boolean,domofond_favorites_search_result:boolean,msg_button_id:string[],msg_button_type:string[],action_payload:string[],previous_contacts_cnt:integer,push_allowed:boolean,msg_chat_list_offset:integer,ces_hd:integer,msg_throttling_reason:string[],msg_app_version:string[],RealtyDevelopment_id:string[],metro_list:array_int[],distance_list:array_float[],district_list:array_int[],block_uids:array_int[],deprecated:boolean,msg_blacklist_reason_id:integer,selected_did:integer,selected_metro:integer,selected_road:integer,was_interaction:integer,roads_list:array_int[],msg_random_id:string[],msg_internet_connection:boolean,msg_socket_type:integer,msg_is_push:boolean,cities_list:array_int[],msg_spam_confirm:boolean,uids_rec:array_int[],email_hash:string[],target_hash:string[],click_position:integer,phone_pdhash:string[],anonymous_phone_pdhash:string[],caller_phone_pdhash:string[],avitopro_date_preset:string[],actual_time:float,track_id:string[],event_no:integer,skill_id:integer,cvpackage_id:integer,safedeal_orderid:integer,msg_search_query:string[],msg_search_success:boolean,msg_chat_page_num:integer,option_number:integer,short_term_rent:boolean,profile_onboarding_step:string[],hierarchy_employee_name:string[],issfdl:boolean,helpdesk_call_id:integer,helpdesk_user_id:integer,target_admuser_id:integer,cv_suggest_show_type:string[],review_score:integer,stage:integer,satisfaction_score:integer,satisfaction_score_reason:integer,satisfaction_score_reason_comment:string[],sgt_building_id:string[],display_state:string[],page_from:string[],item_condition:string[],span_end_time:integer,custom_param:string[],subs_vertical_id:integer,shop_on_moderation:boolean,parameter_value_slug:string[],parameter_value_id:integer,query_length:integer,new_category_id:integer,new_params:array_string[],api_method_name:string[],receive_domestic:string[],receive_type:string[],receive_type_comment:string[],receive_type_express_delivery_comment:string[],seller_receive_reason:array_int[],seller_receive_reason_comment:string[],neutral_receive_reason:array_int[],neutral_receive_reason_comment:string[],express_delivery_receive_reason:array_int[],express_delivery_reason_receive_comment:string[],delivery_satisfied:string[],express_delivery_price:string[],courier_final_comment:string[],express_delivery_wanted:string[],price_suitable:string[],courier_survey_price:integer,courier_survey_reasons:array_int[],courier_survey_reasons_comment:string[],courier_survey_receive_type_comment:string[],courier_survey_receive_type:string[],screen_touch_time:integer,msg_reason_id:integer,geo_session:string[],inactive_page:array_string[],location_suggest_text:string[],answer_seq_id:integer,new_param_ids:string[],autoteka_cookie:string[],landing_slug:string[],autoteka_user_id:string[],utm_source:string[],utm_medium:string[],utm_campaign:string[],autoteka_report_price:integer,is_paid:boolean,is_from_avito:boolean,autoteka_payment_method:string[],autoteka_order_id:integer,autoteka_report_id:integer,autoteka_source_names:array_string[],autoteka_source_blocks:array_string[],autoteka_use_package:boolean,phone_id:integer,params_validation_errors:array_string[],tariff_upgrade_or_new:string[],safedeal_services:array_string[],performance_timing_redirect_start:float,performance_timing_redirect_end:float,performance_timing_fetch_start:float,performance_timing_domain_lookup_start:float,performance_timing_domain_lookup_end:float,performance_timing_connect_start:float,performance_timing_secure_connection_start:float,performance_timing_connect_end:float,performance_timing_request_start:float,performance_timing_response_start:float,performance_timing_response_end:float,performance_timing_first_paint:float,performance_timing_first_contentful_paint:float,performance_timing_dom_interactive:float,performance_timing_dom_content_loaded_event_start:float,performance_timing_dom_content_loaded_event_end:float,performance_timing_dom_complete:float,performance_timing_load_event_start:float,performance_timing_load_event_end:float,autoload_tags:array_string[],autoload_not_empty_tags:array_string[],autoload_updated_tags:array_string[],msg_search_over_limit:boolean,screen_width:integer,screen_height:integer,is_new_tab:boolean,autoload_region:string[],autoload_subway:string[],autoload_street:string[],autoload_district:string[],autoload_direction_road:string[],autoload_distance_to_city:integer,autoload_item_id:integer,autoload_vertical_id:integer,tip_type:string[],autoload_id:string[],error_text:string[],geo_location_cookies:array_string[],safedeal_source:string[],item_snapshot_time:float,is_rotation:boolean,abuse_msg:string[],cpa_abuse_id:integer,call_status:string[],alid:string[],sessid:string[],ad_error:integer,req_num:integer,app_startup_time:integer,is_from_ab_test:boolean,upp_call_id:string[],upp_provider_id:integer,upp_virtual_phone:integer,upp_incoming_phone:string[],upp_client:string[],upp_linked_phone:string[],upp_allocate_id:string[],upp_call_eventtype:integer,upp_call_event_time:integer,upp_call_is_blocked:boolean,upp_call_duration:integer,upp_talk_duration:integer,upp_call_accepted_at:integer,upp_call_ended_at:integer,upp_record_url:string[],upp_record:boolean,upp_caller_message:string[],upp_call_receiver_message:string[],upp_transfer_result:string[],form_validation_error_param_ids:array_int[],form_validation_error_texts:array_string[],sgt_item_type:string[],landing_action:string[],chains:array_int[],prof_profile_type:string[],save_type:integer,phone_show_result:integer,perfvas_landing_from:string[],autoload_total_items_cnt:integer,autoload_new_items_cnt:integer,autoload_updated_items_cnt:integer,autoload_reactivated_items_cnt:integer,autoload_deleted_items_cnt:integer,autoload_unchanged_items_cnt:integer,autoload_error_items_cnt:integer,chain:string[],new_chain:string[],is_cached:boolean,form_validation_error_param_slugs:array_string[],color_theme_status:string[],is_external:boolean,service_status:boolean,upp_status:integer,upp_status_date:integer,checkbox_metro_ring:boolean,checkbox_metro_ring_in:boolean,status_date:integer,upp_setting_id:integer,an_setting_id:integer,courier_orderid:integer,iscourier:boolean,call_status_id:integer,calltracking_activated:boolean,realtor_profile_session:string[],realtor_sell_checkbox:array_string[],realtor_rent_checkbox:array_string[],stack_trace:string[],shop_fraud_reason_template_text:string[],images_list:array_int[],sgt_source_query:string[],sgt_user_query:string[],collapse_group_id:integer,valuable_item_id:integer,__x_eid:integer,__x_version:integer,__x_src_id:integer,__x_dtm:float,__x_cid:integer,__x_lid:integer,__x_mcid:integer,__x_app:integer,__x_offset:integer,__x_pmax:integer,__x_pmin:integer,__x_q:string[],__x_engine:string[],__x_srcp:string[],__x_position:integer,__x_show_position:integer,__x_engine_version:string[],__x_iid:integer,suggest_ad_id:string[],rec_item_id_list:array_int[],rec_engine_list:array_string[],rec_engine_version_list:array_string[],ad_click_uuid:object[],click_uuid:string[],video_play_in_gallery:object[],open_video_link:object[],click_from_block:integer,avitopro_search_query:string[],avitopro_search_vas_type:array_string[],avitopro_search_on_avito_from:integer,avitopro_search_on_avito_to:integer,min_ts:integer,max_ts:integer,start_ts:integer,finish_ts:integer,avitopro_search_hierarchy:array_string[],avitopro_search_locations:array_string[],avitopro_search_price_from:integer,avitopro_search_price_to:integer,avitopro_search_categories:array_int[],avitopro_search_autopublication:string[],avitopro_search_sort_type:string[],avitopro_search_sort_ascending:string[],avitopro_search_sort_statistic_date_from:string[],avitopro_search_sort_statistic_date_to:string[],events_count:integer,text_scale:float,platform:integer,courier_field_name:string[],answer_id:integer,courier_survey_reasons_position:array_int[],upp_record_reason:string[],upp_block_reason:string[],content_size:string[],map_zoom:integer,voxim_auth:boolean,appcall_scenario:string[],appcall_id:string[],appcall_choice:string[],call_side:integer,call_rating:integer,mic_access:boolean,appcall_eventtype:integer,caller_id:integer,reciever_id:integer,caller_device:string[],reciever_device:string[],caller_network:string[],reciever_network:string[],appcall_result:string[],rec_items:object[],bundle_type:integer,icon_type:string[],config_update:boolean,last_config_update:float,eid_list:array_int[],app_start_earliest:float,shield_type:string[],from_type:string[],avitopro_search_status_filter:array_string[],avito_search_iids:array_int[],avitopro_search_isvertical:boolean,avitopro_search_stats_type:string[],avitopro_stat_int_type:string[],webouuid:string[],market_price:integer,auto_type_filter:string[],filter_nale:string[],filter_block:boolean,max_range:integer,min_range:integer,voxim_quality:integer,appcall_network:string[],voxim_metric_type:string[],appcall_callduration:integer,appcall_talkduration:integer,apprater_score:integer,appcall_start:integer,is_core_content:boolean,ntp_timestamp:timestamp[],filter_name:string[],filter_block_name:string[],sold_on_avito_feature:boolean,ct_status:boolean,is_control_sample:boolean,vas_promt_type:string[],avitopro_search_params:object[],question:string[],upp_is_abc:boolean,order_cancel_cause_info_details:string[],order_cancel_cause_info_details_id:integer,pin_type:string[],pin_state:string[],performance_user_centric_interactive:float,performance_user_centric_first_input_delay:float,search_suggest_serp:object[],filter_id:string[],infm_clf_id:integer,infm_version:string[],infm_clf_tree:string[],msg_link_text:string[],msg_copy_text:string[],search_features:integer,laas_tooltip_type:string[],laas_answer:integer,laas_tooltip_answer:integer,project_id:string[],scopes:string[],generation:string[],complectation:string[],modification:string[],x_sgt:string[],app_ui_theme:string[],app_ui_theme_setting_value:string[],change_screen:string[],filter_type:string[],autoactivation:boolean,image_draw_time:integer,image_load_time_delta:float,image_type:string[],msg_email_again:boolean,msg_copy_link_checker:boolean,performance_user_centric_total_blocking_time:float,profprofiles_array:array_int[],image_url:string[],image_status:boolean,image_error:string[],ice_breakers_id:integer,ice_breakers_ids:array_int[],raw_params:string[],is_good_teaser:boolean,au_campaign_id:integer,event_landing_slug:string[],story_id:integer,story_ids:array_int[],feedback_id:string[],bytes_consumed:integer,bytes_remained:integer,top_screen:string[],app_state:string[],uptime:integer,uppmon_actiondate:integer,cb_task:string[],cb_reason:string[],cb_result:integer,cb_result_full:string[],uppmon_offered_score:integer,push_sent_time:integer,push_priority_sent:string[],push_priority_recieved:string[],vox_push_ttl:integer,vox_connected:boolean,vox_push:string[],vox_call_id:string[],array_iids:array_int[],vox_userid:string[],antifraud_call_id:string[],antifraud_call_datetime:integer,antifraud_call_result:integer,antifraud_call_result_code:string[],antifraud_call_duration:integer,antifraud_call_hangupcause:integer,item_quality_score:float,filter_value_names:array_string[],filter_value_ids:array_string[],parameter_name:string[],parameter_value:string[],use_suggest_item_add:boolean,upp_project:string[],report_application_cnt:integer"
    //"uuid:string,u:string,uid:integer,eid:integer,dt:timestamp,dtm:timestamp,src_id:integer,src:integer,ua:string,err:integer,ip:string,ipg:integer,new:boolean,post:boolean,ref:string,v:integer,ab:string,bot:boolean,du:string,geo:array_float,url:string,amqp:boolean,app:integer,test:string,clos:boolean,oc:boolean,bt:integer,email:string,bool_param:boolean,int_param:integer,rqid:string,oldEmail:string,lid:integer,rcid:integer,cid:integer,q:string,offset:integer,limit:integer,total:integer,shops:integer,oid:integer,ak:string,contact:string,contactType:string,uniqueLink:string,operatingSystem:string,event:object,mcid:integer,aid:integer,buid:integer,iid:integer,mid:string,fid:integer,tx:string,crm_eventtype_id:string,st:integer,uem:string,type:string,vin:string,string_param:string,location_id:integer,params:array_string,item_id:integer,event_code:string,categoryid:integer,campaign_id:string,hide_phone:boolean,description:string,group_id:string,launch_id:string,phone:string,userType:integer,smsId:string,manager_id:integer,esid:string,sid:integer,name:string,email2:string,snid:integer,title:string,price:number,photos:integer,photos_ids:array_int,nomail:boolean,timestamp:integer,user_id:integer,errors:array_string,ax:string,engine:string,puid:integer,ignored:boolean,msg:string,manager:string,addr:string,phone1:string,inn:integer,kpp:integer,addr2:string,addr1:string,addrcp:boolean,token:string,success:boolean,source:integer,machine:boolean,survey_id:integer,engine_version:string,iids:array_int,lupint:integer,surv_impid:string,quick:boolean,cmm:string,from:string,status_id:integer,east:boolean,srid:integer,event_group:string,siid:integer,soid:integer,typeid:integer,cdt:timestamp,position:integer,adtyped:string,reason:string,diid:integer,dvas_applied:boolean,items:integer,news:boolean,notice:boolean,reminders:boolean,android_data_size_total:integer,android_data_size_cache:integer,android_data_size_databases:integer,android_data_size_files:integer,tid:integer,android_data_size_shared_prefs:integer,android_data_size_lib:integer,life_time:integer,android_data_size_app_webview:integer,android_data_size_no_backup:integer,android_data_size_code_cache:integer,vsrc:string,imgid:integer,imgoid:integer,imgu:string,x:string,cm:string,ida:boolean,errors_detailed:string,ops:integer,similars:integer,result:string,cp:boolean,link:string,desc:string,sdesc:string,video:string,public_stats:boolean,tages:array_int,srcId:integer,shopId:integer,prob:array_float,subscribe:boolean,screen:string,bids:array_float,cpm_floor:number,ids:array_int,vid:integer,model:integer,iin:boolean,pctrs:array_float,reduce:integer,spid:integer,blockcode:integer,bid:integer,rules:string,domain:string,fnum:integer,lf_cnt:integer,material:string,ptype:string,orderid:integer,ctid:integer,cids:array_int,img:integer,complete:integer,pctr:number,tariff:integer,lflid:integer,additional_user:integer,pagetype:string,sum:array_int,tpl:string,lang:string,hash:string,msgid:integer,sendtime:integer,utmc:string,subject:string,vol:integer,icnt:integer,s:string,did:array_int,lids:array_int,pmax:integer,pmin:integer,priceunit:string,propertytype:string,amnt:integer,lo:integer,lf_dt:integer,lf_grp:integer,ie:boolean,cost:number,dirty:integer,redirect:boolean,city:string,cname:string,photo_ids:array_int,ts:integer,serviceid:integer,at:integer,file:string,stack:array_string,meta:string,pid:integer,shortcut:integer,lq:string,mdistance:string,t:string,from_page:string,dname:string,id:integer,lup:integer,ls:string,p:integer,consent_act:string,snname:string,chid:string,recipient:string,version:integer,cprice:number,sku:string,lf_type:string,packageid:integer,uids:array_int,ex:integer,catid:integer,rid:integer,response:string,dpid:integer,hctr:number,listingfees:string,microcategoryid:integer,sort:integer,icu:boolean,isa:boolean,itl:boolean,hour:integer,levid:integer,brid:string,lc:string,lr:string,page:string,uloggrd:boolean,ar:string,dsp:string,bpos:string,adid:string,adomain:array_string,delete:boolean,repeat:boolean,copy:boolean,area:number,lgid:integer,floor:integer,result_state:integer,landscape:boolean,portrait:boolean,is_company:boolean,inca:boolean,reg_config:string,suid:integer,f:string,message_id:integer,screen_code:integer,_url:string,menu_item:array_string,lf_cost:integer,vas_list:array_int,last_contact:integer,sat:integer,operationid:integer,paysysid:integer,step:integer,vasid:integer,lastpaysysid:integer,onmap:integer,wmap:boolean,rooms:string,sgtd:integer,searchid:integer,soldbytype:string,ownid:integer,iafi:boolean,query:string,qs:number,device_id:string,subscriptionid:integer,ua_browser:string,budget:number,social_id:integer,wizard_id:string,userid:integer,usert:integer,vasfactid:integer,ctotalprice:number,error:integer,ofid:integer,push_token:string,ua_device:string,ua_os:string,ulogged:boolean,wcid:integer,wmid:integer,finish:integer,leid:array_int,liid:array_int,start:integer,beh:string,google_play_services_library_version_code:integer,google_play_services_version_name:string,chatid:string,locationid:integer,sortd:string,paystat:integer,emee_id:integer,emey_id:integer,irs:string,reschid:string,souid:integer,ibofp:integer,ise:integer,click_max_tm:string,clicks:integer,expenses:number,impr_max_tm:string,imprs:integer,lines:integer,mtime:string,size:integer,dupstatus:integer,google_play_services_version_code:integer,simulator:boolean,dupid:integer,shortcut_id:string,csids:string,dmm:string,dsids:string,pcid:integer,cpid:integer,banner_type:string,auid:integer,vas_type:string,subjid:integer,abuseid:integer,tsid:integer,google_play_services_compatibility:boolean,google_play_services_library_version_name:string,notification_id:string,review:string,fieldsmaf:string,assistive_touch_running:boolean,ac:integer,cb:string,cy:string,pc:integer,rt:integer,uph:string,is_official_nd:boolean,status:string,df_start_time:integer,df_expire_time:integer,status_message:string,block_details:string,zk:boolean,test_field_slug:array_string,unm:string,bold_text_enabled:boolean,isupgrade:boolean,tmid:integer,sortf:string,dmessage_id:integer,ap:boolean,body:array_string,route:string,sids:string,response_id:integer,expired:boolean,result_status:integer,result_time:integer,answer_text:string,answer_points:integer,survey_version:integer,is_test:boolean,srcp:string,accessibility_talkback:boolean,photo_library_permission:string,screen_brightness:number,screen_dpi:integer,screen_height_dp:integer,screen_width_dp:integer,count:integer,app_height_dp:integer,cnt:string,mcids:array_int,str:string,words:array_string,dlvritem:integer,offdelivery:boolean,level:integer,pillarid:integer,isb2c:boolean,isdlvr:boolean,extt:integer,contact_type:string,accessibility_haptic:boolean,accessibility_is_on:boolean,accessibility_visual:boolean,app_width_dp:integer,camera_permission:string,configured_default_mail_client:boolean,darker_system_colors:boolean,disk_capacity:number,energy_efficiency:number,force_touch:boolean,free_disk_space:number,grayscale:boolean,guided_access:boolean,invert_colors:boolean,jailbreak:boolean,location_permission:string,low_power_mode:boolean,manufacturer:string,memory_warning_count:integer,platform_payment:string,push_notification_permission:string,reduce_motion:boolean,reduce_transparency:boolean,resources_size:number,shake_to_undo:boolean,speak_screen:boolean,speak_selection:boolean,startup_item:string,startup_reason:string,startup_time:number,switch_control:boolean,terminate_reason:string,voice_over:boolean,watch_pair:boolean,fprice:number,bsize:array_string,duration:integer,tab:integer,action:string,avatar_id:integer,moderator_id:integer,resolution:array_int,ever:integer,original:string,show:boolean,abuseids:array_int,mpact:integer,is_seller:boolean,op_dt:integer,ns_channel:string,ns_type:string,ns_value:boolean,is_user_auth:boolean,lf_lim:integer,dev_diid:integer,duid:integer,dpush_id:string,push_opened:boolean,dpush_delivered:boolean,dpush_error:string,delivery:string,user_key:string,stid:integer,active_icnt:integer,closed_icnt:integer,im:boolean,avatar_exist:boolean,sgtx:string,sgt_tab:string,sgt1:string,sgt2:string,sgt3:string,sgt4:string,sgt5:string,sgt6:string,sgt1_mcids:array_int,sgt2_mcids:array_int,sgt3_mcids:array_int,sgt4_mcids:array_int,sgt5_mcids:array_int,sgt6_mcids:array_int,sgtp:integer,is_legit:boolean,sgt7:string,sgt8:string,sgt9:string,sgt7_mcids:array_int,sgt8_mcids:array_int,sgt9_mcids:array_int,x_trace:string,sesid:string,sgt:array_string,sgt_mcids:array_int,emotion_id:integer,objectid:string,objecttype:string,extension:string,region_id:integer,is_geo:boolean,radius:integer,geo_lat:float,geo_lng:float,geo_acc:integer,geo_time:integer,bspage:integer,is_antihack_phone_confirm:boolean,antihack_confirm_number:integer,antihack_user_password_reset:boolean,color:string,exif_model:string,exif_make:string,str_available:boolean,create_time:integer,update_time:integer,app_type:string,iscredit:boolean,cperiod:integer,geo_acc_code:integer,ver:string,is_delivery:boolean,width:float,height:float,admuserid:integer,dur:float,premoderation:boolean,fraud_code_ids:array_int,comparison_id:integer,duplicate_dur:float,duplicate_cand:object,fsc:integer,target_id:integer,afraud_version:integer,login_type:string,is_first_message:boolean,time_to_interact:string,network_type:string,date:string,plate_number:string,engine_power:string,transport_type:string,transport_category:string,timing:float,timing_duplicate:float,interface_version:string,admuser_id:integer,duplicate_candidates:object,connection_speed:float,time_to_content:string,screen_id:integer,promo:string,exception_id:string,duplicates:array_string,to:string,abs:array_string,safedeal:string,content:string,login:string,num_p:integer,dl:boolean,subtype:string,is_verified_inn:boolean,srd:string,form_input_field_name:string,form_input_field_value:string,channels:array_string,notification:integer,android_data_size_apk:integer,channels_deny:array_string,target_type:integer,target:string,reg_uids:array_int,cdtm:timestamp,pmid:integer,datefrom:string,dateto:string,subs_vertical:string,base_item_id:integer,reject_wrong_params:array_int,state_id:string,parentstate_id:string,from_block:integer,from_position:integer,screen_items_viewed:integer,screen_items_ad_viewed:integer,screen_items_vip_viewed:integer,screen_items_xl_viewed:integer,screen_items_common_viewed:integer,screen_items_new_viewed:integer,screen_items_geo_viewed:integer,screen_items_rec_viewed:integer,service_id:array_int,amount:array_float,operation_id:array_int,uris:array_string,isp:boolean,block_total:integer,page_total:integer,block_items_display:integer,block_items_added:integer,is_anon:boolean,search_area:array_float,serpdisplay_type:string,landing_type:string,retry:boolean,browser_web_push_agreement:boolean,is_auth:boolean,snippet_placement:string,vasid_prev:integer,snippet_type:string,_ga:string,tooltip_id:integer,resultTime:integer,devDiid:integer,advtid:string,sm:boolean,consentAct:string,answerPoints:integer,isOfficialNd:boolean,responseId:string,ach:boolean,premiums:integer,dfStartTime:integer,peid:integer,answerText:string,resultStatus:integer,dmessageId:integer,surveyVersion:integer,pdrid:integer,dfExpireTime:integer,usedSuggest:boolean,isTest:boolean,platformId:integer,ppeid:integer,surv_id:integer,userToken:string,img_id:integer,vips:integer,d:boolean,surveyId:integer,uas:integer,sr:string,groupId:integer,launchId:integer,unique_args:object,data:object,talkback:boolean,model_version:string,subs_price_bonus:float,subs_price_packages:float,subs_price_ext_shop:float,subs_price_total:float,with_shop_flg:boolean,abp:integer,apply_status:string,has_cv:boolean,phone_request_flg:boolean,msg_image:string,valid_until:integer,discount_type:string,rating:float,reviews_cnt:integer,review_id:integer,ssv:integer,user_auth_suggestionshown:boolean,share_place:string,s_trg:integer,user_authmemo_id:integer,user_auth_memo_id:string,call_id:string,is_linked:boolean,rdt:string,notification_message_id:string,cnt_default_config:integer,cnt_default_items:integer,default_vas_amount:integer,adm_comment:string,item_version:integer,is_adblock:boolean,is_phone_new:boolean,helpdesk_agent_state:string,anonymous_number_service_responce:string,additional_item:integer,autoload_operation_activate:boolean,autoload_operation_add:boolean,autoload_operation_fee:boolean,autoload_operation_remove:boolean,autoload_operation_stop:boolean,autoload_operation_update:boolean,autoload_operation_vas:boolean,is_refresh:boolean,item_in_moderation:boolean,raw_body:object,call_action_id:integer,call_action_type:string,order_cancel_cause:integer,order_cancel_cause_txt:string,is_verified:boolean,other_phones_number:integer,item_number:integer,phone_action:string,is_reverified:boolean,phone_action_type:string,order_cancel_cause_info:string,selected_suggest_value:string,i:object,wrong_phone:boolean,subs_form_type:string,review_seq_id:integer,errorsDetailed:string,subs_vertical_ids:array_int,subs_tariff_id:integer,subs_proposed_bonus_factor:float,subs_extensions:array_string,subs_packages:object,caller_phone:string,caller_user_type:string,success_url:string,rent_start_datetime:string,rent_end_datetime:string,message_preview:string,from_notification_channel:string,car_doors:string,engine_type:string,transmission:string,car_drive:string,steering_wheel:string,engine_volume:string,item_status:string,aggregation:string,article_dissatisfaction_reason:string,domofond_notification_channel:string,domofond_subscription_type:string,dnotification_message_id:integer,dnotification_delivered:boolean,dnotification_error:string,anonymous_phone_number:string,isbp:boolean,slider_type:string,redirect_incoming_call_to:string,anonymous_phone:string,placement:string,destination:string,action_type:string,from_source:string,cnt_favourites:integer,clickstream_request_id:string,ces_score:integer,ces_context:string,ref_id:string,parent_ref_id:string,run_id:string,pvas_group_old:integer,pvas_group:integer,pvas_groups:array_int,page_view_session_time:integer,tab_id:string,ctcallid:integer,is_geo_delivery_widget:boolean,api_path:string,antihack_reason:string,buyer_booking_cancel_cause:array_int,buyer_booking_cancel_cause_text:string,ticket_comment_id:integer,ticket_comment_dt:integer,src_split_ticket_id:integer,trgt_split_ticket_id:integer,time_to_first_byte:integer,time_to_first_paint:integer,time_to_first_interactive:integer,time_to_dom_ready:integer,time_to_on_load:integer,time_to_target_content:integer,time_for_async_target_content:integer,avito_advice_article:string,payerid:string,changed_microcat_id:integer,scenario:string,userAgent:string,cv_use_category:boolean,cv_use_title:boolean,requestParams:object,inputValue:string,inputField:string,offerId:integer,cityId:integer,regionId:integer,classifiedId:integer,feedId:integer,clientId:integer,clientNeedId:integer,lifeTime:integer,leadId:integer,memberId:integer,executiveMemberId:integer,executiveType:string,optionsNumber:integer,str_buyer_contact_result:string,str_buyer_contact_result_reason:array_int,str_buyer_contact_result_reason_text:string,second:float,profile_tab:string,is_original_user_report:boolean,autoteka_user_type:integer,list_load_number:integer,ssid:integer,screen_orientation:string,recommendpaysysid:array_int,parent_uid:integer,app_version:string,os_version:string,accounts_number:integer,search_correction_action:string,search_correction_original:string,search_correction_corrected:string,search_correction_method:string,autosort_images:boolean,cnt_subscribers:integer,is_oasis:boolean,xl:boolean,pvas_dates:array_string,oneclickpayment:boolean,call_reason:integer,status_code:integer,api_dt:integer,project:string,location_text_input:string,cadastralnumber:string,report_duration:integer,report_status:boolean,other_shop_items:array_int,page_number:integer,phone2:string,skype:string,shop_url:string,shop_name:string,shop_site:string,short_description:string,long_description:string,work_regime:string,info_line:string,shop_logo:string,shop_branding:string,shop_back:string,shop_photos:array_string,on_avito_since:integer,ssfid:integer,deep_link:string,item_add_screen:string,user_ids:array_int,empl_id:integer,empl_invite_state:string,prev_page:string,shop_fraud_reason_ids:array_int,shop_moderation_action_id:integer,shop_moderation_action_hash:string,service_name:string,deploy_env:string,cid_tree_branch:array_int,lid_tree_branch:array_int,cur_i_status_id:integer,cur_i_status_time:integer,item_version_time:integer,infomodel_v_id:string,checkbox_an_enable:boolean,message_type:string,app_version_code:string,banner_id:string,shortcut_description:string,close_timeout:integer,note_text:string,item_parameter_name:string,block_id:string,selling_system:string,banner_code:string,wsrc:string,shops_array:array_int,rec_item_id:integer,lfpackage_id:integer,subscription_promo:boolean,sub_is_ss:boolean,sub_prolong:string,target_page:string,mobile_event_duration:integer,screen_session_uid:string,screen_name:string,content_type:string,mobile_app_page_number:integer,roads:array_int,reload:boolean,img_download_status:boolean,screen_start_time:integer,service_branch:string,sgt_cat_flag:boolean,notification_owner:string,notification_type:string,ns_owner:string,operation_ids:array_int,software_version:string,build:string,adpartner:integer,ticket_channel:integer,adslot:string,statid:integer,is_payed_immediately:boolean,lfpackage_ids:array_int,current_subs_version:string,new_subs_version:string,subscription_edit:boolean,channel_number:integer,channels_screen_count:integer,domofond_item_type:string,domofond_item_id:string,item_position:integer,delivery_help_question:string,domofond_cookie:string,domofond_user_token:string,banner_due_date:integer,banner_show_days:boolean,banner_item_id:integer,empty_serp_link_type:string,domofond_proxy_request_name:string,chat_error_case:string,has_messages:boolean,search_address_type:string,domofond_user_id:integer,js_event_type:string,dom_node:string,js_event_slug:string,attr_title:string,attr_link:string,attr_value:string,key_name:string,is_ctrl_pressed:boolean,is_alt_pressed:boolean,is_shift_pressed:boolean,color_theme:string,dom_node_content:string,is_checkbox_checked:boolean,page_x_coord:integer,page_y_coord:integer,srd_initial:string,last_show_dt:integer,domofond_button_name:string,uploaded_files_cnt:integer,review_additional_info:string,flow_type:integer,flow_id:string,ces_article:integer,items_locked_count:integer,items_not_locked_count:integer,word_sgt_clicks:integer,metro:array_int,msg_app_name:string,msg_request:string,domofond_category:string,domofond_subcategory:string,domofond_filter_region:string,domofond_filter_location:string,domofond_filter_rooms:string,domofond_filter_price_from:string,domofond_filter_price_up:string,domofond_filter_rent_type:string,domofond_search_result_type:string,moderation_user_score:float,domofond_search_position:integer,domofond_saved_search_result:boolean,domofond_favorites_search_result:boolean,msg_button_id:string,msg_button_type:string,action_payload:string,previous_contacts_cnt:integer,push_allowed:boolean,msg_chat_list_offset:integer,ces_hd:integer,msg_throttling_reason:string,msg_app_version:string,RealtyDevelopment_id:string,metro_list:array_int,distance_list:array_float,district_list:array_int,block_uids:array_int,deprecated:boolean,msg_blacklist_reason_id:integer,selected_did:integer,selected_metro:integer,selected_road:integer,was_interaction:integer,roads_list:array_int,msg_random_id:string,msg_internet_connection:boolean,msg_socket_type:integer,msg_is_push:boolean,cities_list:array_int,msg_spam_confirm:boolean,uids_rec:array_int,email_hash:string,target_hash:string,click_position:integer,phone_pdhash:string,anonymous_phone_pdhash:string,caller_phone_pdhash:string,avitopro_date_preset:string,actual_time:float,track_id:string,event_no:integer,skill_id:integer,cvpackage_id:integer,safedeal_orderid:integer,msg_search_query:string,msg_search_success:boolean,msg_chat_page_num:integer,option_number:integer,short_term_rent:boolean,profile_onboarding_step:string,hierarchy_employee_name:string,issfdl:boolean,helpdesk_call_id:integer,helpdesk_user_id:integer,target_admuser_id:integer,cv_suggest_show_type:string,review_score:integer,stage:integer,satisfaction_score:integer,satisfaction_score_reason:integer,satisfaction_score_reason_comment:string,sgt_building_id:string,display_state:string,page_from:string,item_condition:string,span_end_time:integer,custom_param:string,subs_vertical_id:integer,shop_on_moderation:boolean,parameter_value_slug:string,parameter_value_id:integer,query_length:integer,new_category_id:integer,new_params:array_string,api_method_name:string,receive_domestic:string,receive_type:string,receive_type_comment:string,receive_type_express_delivery_comment:string,seller_receive_reason:array_int,seller_receive_reason_comment:string,neutral_receive_reason:array_int,neutral_receive_reason_comment:string,express_delivery_receive_reason:array_int,express_delivery_reason_receive_comment:string,delivery_satisfied:string,express_delivery_price:string,courier_final_comment:string,express_delivery_wanted:string,price_suitable:string,courier_survey_price:integer,courier_survey_reasons:array_int,courier_survey_reasons_comment:string,courier_survey_receive_type_comment:string,courier_survey_receive_type:string,screen_touch_time:integer,msg_reason_id:integer,geo_session:string,inactive_page:array_string,location_suggest_text:string,answer_seq_id:integer,new_param_ids:string,autoteka_cookie:string,landing_slug:string,autoteka_user_id:string,utm_source:string,utm_medium:string,utm_campaign:string,autoteka_report_price:integer,is_paid:boolean,is_from_avito:boolean,autoteka_payment_method:string,autoteka_order_id:integer,autoteka_report_id:integer,autoteka_source_names:array_string,autoteka_source_blocks:array_string,autoteka_use_package:boolean,phone_id:integer,params_validation_errors:array_string,tariff_upgrade_or_new:string,safedeal_services:array_string,performance_timing_redirect_start:float,performance_timing_redirect_end:float,performance_timing_fetch_start:float,performance_timing_domain_lookup_start:float,performance_timing_domain_lookup_end:float,performance_timing_connect_start:float,performance_timing_secure_connection_start:float,performance_timing_connect_end:float,performance_timing_request_start:float,performance_timing_response_start:float,performance_timing_response_end:float,performance_timing_first_paint:float,performance_timing_first_contentful_paint:float,performance_timing_dom_interactive:float,performance_timing_dom_content_loaded_event_start:float,performance_timing_dom_content_loaded_event_end:float,performance_timing_dom_complete:float,performance_timing_load_event_start:float,performance_timing_load_event_end:float,autoload_tags:array_string,autoload_not_empty_tags:array_string,autoload_updated_tags:array_string,msg_search_over_limit:boolean,screen_width:integer,screen_height:integer,is_new_tab:boolean,autoload_region:string,autoload_subway:string,autoload_street:string,autoload_district:string,autoload_direction_road:string,autoload_distance_to_city:integer,autoload_item_id:integer,autoload_vertical_id:integer,tip_type:string,autoload_id:string,error_text:string,geo_location_cookies:array_string,safedeal_source:string,item_snapshot_time:float,is_rotation:boolean,abuse_msg:string,cpa_abuse_id:integer,call_status:string,alid:string,sessid:string,ad_error:integer,req_num:integer,app_startup_time:integer,is_from_ab_test:boolean,upp_call_id:string,upp_provider_id:integer,upp_virtual_phone:integer,upp_incoming_phone:string,upp_client:string,upp_linked_phone:string,upp_allocate_id:string,upp_call_eventtype:integer,upp_call_event_time:integer,upp_call_is_blocked:boolean,upp_call_duration:integer,upp_talk_duration:integer,upp_call_accepted_at:integer,upp_call_ended_at:integer,upp_record_url:string,upp_record:boolean,upp_caller_message:string,upp_call_receiver_message:string,upp_transfer_result:string,form_validation_error_param_ids:array_int,form_validation_error_texts:array_string,sgt_item_type:string,landing_action:string,chains:array_int,prof_profile_type:string,save_type:integer,phone_show_result:integer,perfvas_landing_from:string,autoload_total_items_cnt:integer,autoload_new_items_cnt:integer,autoload_updated_items_cnt:integer,autoload_reactivated_items_cnt:integer,autoload_deleted_items_cnt:integer,autoload_unchanged_items_cnt:integer,autoload_error_items_cnt:integer,chain:string,new_chain:string,is_cached:boolean,form_validation_error_param_slugs:array_string,color_theme_status:string,is_external:boolean,service_status:boolean,upp_status:integer,upp_status_date:integer,checkbox_metro_ring:boolean,checkbox_metro_ring_in:boolean,status_date:integer,upp_setting_id:integer,an_setting_id:integer,courier_orderid:integer,iscourier:boolean,call_status_id:integer,calltracking_activated:boolean,realtor_profile_session:string,realtor_sell_checkbox:array_string,realtor_rent_checkbox:array_string,stack_trace:string,shop_fraud_reason_template_text:string,images_list:array_int,sgt_source_query:string,sgt_user_query:string,collapse_group_id:integer,valuable_item_id:integer,__x_eid:integer,__x_version:integer,__x_src_id:integer,__x_dtm:float,__x_cid:integer,__x_lid:integer,__x_mcid:integer,__x_app:integer,__x_offset:integer,__x_pmax:integer,__x_pmin:integer,__x_q:string,__x_engine:string,__x_srcp:string,__x_position:integer,__x_show_position:integer,__x_engine_version:string,__x_iid:integer,suggest_ad_id:string,rec_item_id_list:array_int,rec_engine_list:array_string,rec_engine_version_list:array_string,ad_click_uuid:object,click_uuid:string,video_play_in_gallery:object,open_video_link:object,click_from_block:integer,avitopro_search_query:string,avitopro_search_vas_type:array_string,avitopro_search_on_avito_from:integer,avitopro_search_on_avito_to:integer,min_ts:integer,max_ts:integer,start_ts:integer,finish_ts:integer,avitopro_search_hierarchy:array_string,avitopro_search_locations:array_string,avitopro_search_price_from:integer,avitopro_search_price_to:integer,avitopro_search_categories:array_int,avitopro_search_autopublication:string,avitopro_search_sort_type:string,avitopro_search_sort_ascending:string,avitopro_search_sort_statistic_date_from:string,avitopro_search_sort_statistic_date_to:string,events_count:integer,text_scale:float,platform:integer,courier_field_name:string,answer_id:integer,courier_survey_reasons_position:array_int,upp_record_reason:string,upp_block_reason:string,content_size:string,map_zoom:integer,voxim_auth:boolean,appcall_scenario:string,appcall_id:string,appcall_choice:string,call_side:integer,call_rating:integer,mic_access:boolean,appcall_eventtype:integer,caller_id:integer,reciever_id:integer,caller_device:string,reciever_device:string,caller_network:string,reciever_network:string,appcall_result:string,rec_items:object,bundle_type:integer,icon_type:string,config_update:boolean,last_config_update:float,eid_list:array_int,app_start_earliest:float,shield_type:string,from_type:string,avitopro_search_status_filter:array_string,avito_search_iids:array_int,avitopro_search_isvertical:boolean,avitopro_search_stats_type:string,avitopro_stat_int_type:string,webouuid:string,market_price:integer,auto_type_filter:string,filter_nale:string,filter_block:boolean,max_range:integer,min_range:integer,voxim_quality:integer,appcall_network:string,voxim_metric_type:string,appcall_callduration:integer,appcall_talkduration:integer,apprater_score:integer,appcall_start:integer,is_core_content:boolean,ntp_timestamp:timestamp,filter_name:string,filter_block_name:string,sold_on_avito_feature:boolean,ct_status:boolean,is_control_sample:boolean,vas_promt_type:string,avitopro_search_params:object,question:string,upp_is_abc:boolean,order_cancel_cause_info_details:string,order_cancel_cause_info_details_id:integer,pin_type:string,pin_state:string,performance_user_centric_interactive:float,performance_user_centric_first_input_delay:float,search_suggest_serp:object,filter_id:string,infm_clf_id:integer,infm_version:string,infm_clf_tree:string,msg_link_text:string,msg_copy_text:string,search_features:integer,laas_tooltip_type:string,laas_answer:integer,laas_tooltip_answer:integer,project_id:string,scopes:string,generation:string,complectation:string,modification:string,x_sgt:string,app_ui_theme:string,app_ui_theme_setting_value:string,change_screen:string,filter_type:string,autoactivation:boolean,image_draw_time:integer,image_load_time_delta:float,image_type:string,msg_email_again:boolean,msg_copy_link_checker:boolean,performance_user_centric_total_blocking_time:float,profprofiles_array:array_int,image_url:string,image_status:boolean,image_error:string,ice_breakers_id:integer,ice_breakers_ids:array_int,raw_params:string,is_good_teaser:boolean,au_campaign_id:integer,event_landing_slug:string,story_id:integer,story_ids:array_int,feedback_id:string,bytes_consumed:integer,bytes_remained:integer,top_screen:string,app_state:string,uptime:integer,uppmon_actiondate:integer,cb_task:string,cb_reason:string,cb_result:integer,cb_result_full:string,uppmon_offered_score:integer,push_sent_time:integer,push_priority_sent:string,push_priority_recieved:string,vox_push_ttl:integer,vox_connected:boolean,vox_push:string,vox_call_id:string,array_iids:array_int,vox_userid:string,antifraud_call_id:string,antifraud_call_datetime:integer,antifraud_call_result:integer,antifraud_call_result_code:string,antifraud_call_duration:integer,antifraud_call_hangupcause:integer,item_quality_score:float,filter_value_names:array_string,filter_value_ids:array_string,parameter_name:string,parameter_value:string,use_suggest_item_add:boolean,upp_project:string,report_application_cnt:integer"
    //"uuid:string,u:string,uid:integer,eid:integer,dt:timestamp,dtm:timestamp,src_id:integer,src:integer,ua:string"

    //Sink *sink = new ARESSink(format);

    Sink *sink = new CSVSink(//"timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua)",
                             //"timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua),err,hash(ip),ipg,new,post,hash(ref),v,hash(ab),bot,hash(du),hash(geo),hash(url),amqp,app,hash(test),clos,oc,bt,hash(email),ArrayBool_param,int_param,hash(rqid),hash(oldEmail),lid,rcid,cid,hash(q),offset,limit,total,shops,oid,hash(ak),hash(contact),hash(contactType),hash(uniqueLink),hash(operatingSystem),hash(event),mcid,aid,buid,iid,hash(mid),fid,hash(tx),hash(crm_eventtype_id),st,hash(uem),hash(type),hash(vin),hash(string_param),location_id,hash(params),item_id,hash(event_code),categoryid,hash(campaign_id),hide_phone,hash(description),hash(group_id),hash(launch_id),hash(phone),userType,hash(smsId),manager_id,hash(esid),sid,hash(name),hash(email2),snid,hash(title),price,photos,hash(photos_ids),nomail,timestamp,user_id,hash(errors),hash(ax),hash(engine),puid,ignored,hash(msg),hash(manager),hash(addr),hash(phone1),inn,kpp,hash(addr2),hash(addr1),addrcp,hash(token),success,source,machine,survey_id,hash(engine_version),hash(iids),lupint,hash(surv_impid),quick,hash(cmm),hash(from),status_id,east,srid,hash(event_group),siid,soid,typeid,cdt,position,hash(adtyped),hash(reason),diid,dvas_applied,items,news,notice,reminders,android_data_size_total,android_data_size_cache,android_data_size_databases,android_data_size_files,tid,android_data_size_shared_prefs,android_data_size_lib,life_time,android_data_size_app_webview,android_data_size_no_backup,android_data_size_code_cache,hash(vsrc),imgid,imgoid,hash(imgu),hash(x),hash(cm),ida,hash(errors_detailed),ops,similars,hash(result),cp,hash(link),hash(desc),hash(sdesc),hash(video),public_stats,hash(tages),srcId,shopId,hash(prob),subscribe,hash(screen),hash(bids),cpm_floor,hash(ids),vid,model,iin,hash(pctrs),reduce,spid,blockcode,bid,hash(rules),hash(domain),fnum,lf_cnt,hash(material),hash(ptype),orderid,ctid,hash(cids),img,complete,pctr,tariff,lflid,additional_user,hash(pagetype),hash(sum),hash(tpl),hash(lang),hash(hash),msgid,sendtime,hash(utmc),hash(subject),vol,icnt,hash(s),hash(did),hash(lids),pmax,pmin,hash(priceunit),hash(propertytype),amnt,lo,lf_dt,lf_grp,ie,cost,dirty,redirect,hash(city),hash(cname),hash(photo_ids),ts,serviceid,at,hash(file),hash(stack),hash(meta),pid,shortcut,hash(lq),hash(mdistance),hash(t),hash(from_page),hash(dname),id,lup,hash(ls),p,hash(consent_act),hash(snname),hash(chid),hash(recipient),version,cprice,hash(sku),hash(lf_type),packageid,hash(uids),ex,catid,rid,hash(response),dpid,hctr,hash(listingfees),microcategoryid,sort,icu,isa,itl,hour,levid,hash(brid),hash(lc),hash(lr),hash(page),uloggrd,hash(ar),hash(dsp),hash(bpos),hash(adid),hash(adomain),delete,repeat,copy,area,lgid,floor,result_state,landscape,portrait,is_company,inca,hash(reg_config),suid,hash(f),message_id,screen_code,hash(_url),hash(menu_item),lf_cost,hash(vas_list),last_contact,sat,operationid,paysysid,step,vasid,lastpaysysid,onmap,wmap,hash(rooms),sgtd,searchid,hash(soldbytype),ownid,iafi,hash(query),qs,hash(device_id),subscriptionid,hash(ua_browser),budget,social_id,hash(wizard_id),userid,usert,vasfactid,ctotalprice,error,ofid,hash(push_token),hash(ua_device),hash(ua_os),ulogged,wcid,wmid,finish,hash(leid),hash(liid),start,hash(beh),google_play_services_library_version_code,hash(google_play_services_version_name),hash(chatid),locationid,hash(sortd),paystat,emee_id,emey_id,hash(irs),hash(reschid),souid,ibofp,ise,hash(click_max_tm),clicks,expenses,hash(impr_max_tm),imprs,lines,hash(mtime),size,dupstatus,google_play_services_version_code,simulator,dupid,hash(shortcut_id),hash(csids),hash(dmm),hash(dsids),pcid,cpid,hash(banner_type),auid,hash(vas_type),subjid,abuseid,tsid,google_play_services_compatibility,hash(google_play_services_library_version_name),hash(notification_id),hash(review),hash(fieldsmaf),assistive_touch_running,ac,hash(cb),hash(cy),pc,rt,hash(uph),is_official_nd,hash(status),df_start_time,df_expire_time,hash(status_message),hash(block_details),zk,hash(test_field_slug),hash(unm),bold_text_enabled,isupgrade,tmid,hash(sortf),dmessage_id,ap,hash(body),hash(route),hash(sids),response_id,expired,result_status,result_time,hash(answer_text),answer_points,survey_version,is_test,hash(srcp),accessibility_talkback,hash(photo_library_permission),screen_brightness,screen_dpi,screen_height_dp,screen_width_dp,count,app_height_dp,hash(cnt),hash(mcids),hash(str),hash(words),dlvritem,offdelivery,level,pillarid,isb2c,isdlvr,extt,hash(contact_type),accessibility_haptic,accessibility_is_on,accessibility_visual,app_width_dp,hash(camera_permission),configured_default_mail_client,darker_system_colors,disk_capacity,energy_efficiency,force_touch,free_disk_space,grayscale,guided_access,invert_colors,jailbreak,hash(location_permission),low_power_mode,hash(manufacturer),memory_warning_count,hash(platform_payment),hash(push_notification_permission),reduce_motion,reduce_transparency,resources_size,shake_to_undo,speak_screen,speak_selection,hash(startup_item),hash(startup_reason),startup_time,switch_control,hash(terminate_reason),voice_over,watch_pair,fprice,hash(bsize),duration,tab,hash(action),avatar_id,moderator_id,hash(resolution),ever,hash(original),show,hash(abuseids),mpact,is_seller,op_dt,hash(ns_channel),hash(ns_type),ns_value,is_user_auth,lf_lim,dev_diid,duid,hash(dpush_id),push_opened,dpush_delivered,hash(dpush_error),hash(delivery),hash(user_key),stid,active_icnt,closed_icnt,im,avatar_exist,hash(sgtx),hash(sgt_tab),hash(sgt1),hash(sgt2),hash(sgt3),hash(sgt4),hash(sgt5),hash(sgt6),hash(sgt1_mcids),hash(sgt2_mcids),hash(sgt3_mcids),hash(sgt4_mcids),hash(sgt5_mcids),hash(sgt6_mcids),sgtp,is_legit,hash(sgt7),hash(sgt8),hash(sgt9),hash(sgt7_mcids),hash(sgt8_mcids),hash(sgt9_mcids),hash(x_trace),hash(sesid),hash(sgt),hash(sgt_mcids),emotion_id,hash(objectid),hash(objecttype),hash(extension),region_id,is_geo,radius,geo_lat,geo_lng,geo_acc,geo_time,bspage,is_antihack_phone_confirm,antihack_confirm_number,antihack_user_password_reset,hash(color),hash(exif_model),hash(exif_make),str_available,create_time,update_time,hash(app_type),iscredit,cperiod,geo_acc_code,hash(ver),is_delivery,width,height,admuserid,dur,premoderation,hash(fraud_code_ids),comparison_id,duplicate_dur,hash(duplicate_cand),fsc,target_id,afraud_version,hash(login_type),is_first_message,hash(time_to_interact),hash(network_type),hash(date),hash(plate_number),hash(engine_power),hash(transport_type),hash(transport_category),timing,timing_duplicate,hash(interface_version),admuser_id,hash(duplicate_candidates),connection_speed,hash(time_to_content),screen_id,hash(promo),hash(exception_id),hash(duplicates),hash(to),hash(abs),hash(safedeal),hash(content),hash(login),num_p,dl,hash(subtype),is_verified_inn,hash(srd),hash(form_input_field_name),hash(form_input_field_value),hash(channels),notification,android_data_size_apk,hash(channels_deny),target_type,hash(target),hash(reg_uids),cdtm,pmid,hash(datefrom),hash(dateto),hash(subs_vertical),base_item_id,hash(reject_wrong_params),hash(state_id),hash(parentstate_id),from_block,from_position,screen_items_viewed,screen_items_ad_viewed,screen_items_vip_viewed,screen_items_xl_viewed,screen_items_common_viewed,screen_items_new_viewed,screen_items_geo_viewed,screen_items_rec_viewed,hash(service_id),hash(amount),hash(operation_id),hash(uris),isp,block_total,page_total,block_items_display,block_items_added,is_anon,hash(search_area),hash(serpdisplay_type),hash(landing_type),retry,browser_web_push_agreement,is_auth,hash(snippet_placement),vasid_prev,hash(snippet_type),hash(_ga),tooltip_id,resultTime,devDiid,hash(advtid),sm,hash(consentAct),answerPoints,isOfficialNd,hash(responseId),ach,premiums,dfStartTime,peid,hash(answerText),resultStatus,dmessageId,surveyVersion,pdrid,dfExpireTime,usedSuggest,isTest,platformId,ppeid,surv_id,hash(userToken),img_id,vips,d,surveyId,uas,hash(sr),groupId,launchId,hash(unique_args),hash(data),talkback,hash(model_version),subs_price_bonus,subs_price_packages,subs_price_ext_shop,subs_price_total,with_shop_flg,abp,hash(apply_status),has_cv,phone_request_flg,hash(msg_image),valid_until,hash(discount_type),rating,reviews_cnt,review_id,ssv,user_auth_suggestionshown,hash(share_place),s_trg,user_authmemo_id,hash(user_auth_memo_id),hash(call_id),is_linked,hash(rdt),hash(notification_message_id),cnt_default_config,cnt_default_items,default_vas_amount,hash(adm_comment),item_version,is_adblock,is_phone_new,hash(helpdesk_agent_state),hash(anonymous_number_service_responce),additional_item,autoload_operation_activate,autoload_operation_add,autoload_operation_fee,autoload_operation_remove,autoload_operation_stop,autoload_operation_update,autoload_operation_vas,is_refresh,item_in_moderation,hash(raw_body),call_action_id,hash(call_action_type),order_cancel_cause,hash(order_cancel_cause_txt),is_verified,other_phones_number,item_number,hash(phone_action),is_reverified,hash(phone_action_type),hash(order_cancel_cause_info),hash(selected_suggest_value),hash(i),wrong_phone,hash(subs_form_type),review_seq_id,hash(errorsDetailed),hash(subs_vertical_ids),subs_tariff_id,subs_proposed_bonus_factor,hash(subs_extensions),hash(subs_packages),hash(caller_phone),hash(caller_user_type),hash(success_url),hash(rent_start_datetime),hash(rent_end_datetime),hash(message_preview),hash(from_notification_channel),hash(car_doors),hash(engine_type),hash(transmission),hash(car_drive),hash(steering_wheel),hash(engine_volume),hash(item_status),hash(aggregation),hash(article_dissatisfaction_reason),hash(domofond_notification_channel),hash(domofond_subscription_type),dnotification_message_id,dnotification_delivered,hash(dnotification_error),hash(anonymous_phone_number),isbp,hash(slider_type),hash(redirect_incoming_call_to),hash(anonymous_phone),hash(placement),hash(destination),hash(action_type),hash(from_source),cnt_favourites,hash(clickstream_request_id),ces_score,hash(ces_context),hash(ref_id),hash(parent_ref_id),hash(run_id),pvas_group_old,pvas_group,hash(pvas_groups),page_view_session_time,hash(tab_id),ctcallid,is_geo_delivery_widget,hash(api_path),hash(antihack_reason),hash(buyer_booking_cancel_cause),hash(buyer_booking_cancel_cause_text),ticket_comment_id,ticket_comment_dt,src_split_ticket_id,trgt_split_ticket_id,time_to_first_byte,time_to_first_paint,time_to_first_interactive,time_to_dom_ready,time_to_on_load,time_to_target_content,time_for_async_target_content,hash(avito_advice_article),hash(payerid),changed_microcat_id,hash(scenario),hash(userAgent),cv_use_category,cv_use_title,hash(requestParams),hash(inputValue),hash(inputField),offerId,cityId,regionId,classifiedId,feedId,clientId,clientNeedId,lifeTime,leadId,memberId,executiveMemberId,hash(executiveType),optionsNumber,hash(str_buyer_contact_result),hash(str_buyer_contact_result_reason),hash(str_buyer_contact_result_reason_text),second,hash(profile_tab),is_original_user_report,autoteka_user_type,list_load_number,ssid,hash(screen_orientation),hash(recommendpaysysid),parent_uid,hash(app_version),hash(os_version),accounts_number,hash(search_correction_action),hash(search_correction_original),hash(search_correction_corrected),hash(search_correction_method),autosort_images,cnt_subscribers,is_oasis,xl,hash(pvas_dates),oneclickpayment,call_reason,status_code,api_dt,hash(project),hash(location_text_input),hash(cadastralnumber),report_duration,report_status,hash(other_shop_items),page_number,hash(phone2),hash(skype),hash(shop_url),hash(shop_name),hash(shop_site),hash(short_description),hash(long_description),hash(work_regime),hash(info_line),hash(shop_logo),hash(shop_branding),hash(shop_back),hash(shop_photos),on_avito_since,ssfid,hash(deep_link),hash(item_add_screen),hash(user_ids),empl_id,hash(empl_invite_state),hash(prev_page),hash(shop_fraud_reason_ids),shop_moderation_action_id,hash(shop_moderation_action_hash),hash(service_name),hash(deploy_env),hash(cid_tree_branch),hash(lid_tree_branch),cur_i_status_id,cur_i_status_time,item_version_time,hash(infomodel_v_id),checkbox_an_enable,hash(message_type),hash(app_version_code),hash(banner_id),hash(shortcut_description),close_timeout,hash(note_text),hash(item_parameter_name),hash(block_id),hash(selling_system),hash(banner_code),hash(wsrc),hash(shops_array),rec_item_id,lfpackage_id,subscription_promo,sub_is_ss,hash(sub_prolong),hash(target_page),mobile_event_duration,hash(screen_session_uid),hash(screen_name),hash(content_type),mobile_app_page_number,hash(roads),reload,img_download_status,screen_start_time,hash(service_branch),sgt_cat_flag,hash(notification_owner),hash(notification_type),hash(ns_owner),hash(operation_ids),hash(software_version),hash(build),adpartner,ticket_channel,hash(adslot),statid,is_payed_immediately,hash(lfpackage_ids),hash(current_subs_version),hash(new_subs_version),subscription_edit,channel_number,channels_screen_count,hash(domofond_item_type),hash(domofond_item_id),item_position,hash(delivery_help_question),hash(domofond_cookie),hash(domofond_user_token),banner_due_date,banner_show_days,banner_item_id,hash(empty_serp_link_type),hash(domofond_proxy_request_name),hash(chat_error_case),has_messages,hash(search_address_type),domofond_user_id,hash(js_event_type),hash(dom_node),hash(js_event_slug),hash(attr_title),hash(attr_link),hash(attr_value),hash(key_name),is_ctrl_pressed,is_alt_pressed,is_shift_pressed,hash(color_theme),hash(dom_node_content),is_checkbox_checked,page_x_coord,page_y_coord,hash(srd_initial),last_show_dt,hash(domofond_button_name),uploaded_files_cnt,hash(review_additional_info),flow_type,hash(flow_id),ces_article,items_locked_count,items_not_locked_count,word_sgt_clicks,hash(metro),hash(msg_app_name),hash(msg_request),hash(domofond_category),hash(domofond_subcategory),hash(domofond_filter_region),hash(domofond_filter_location),hash(domofond_filter_rooms),hash(domofond_filter_price_from),hash(domofond_filter_price_up),hash(domofond_filter_rent_type),hash(domofond_search_result_type),moderation_user_score,domofond_search_position,domofond_saved_search_result,domofond_favorites_search_result,hash(msg_button_id),hash(msg_button_type),hash(action_payload),previous_contacts_cnt,push_allowed,msg_chat_list_offset,ces_hd,hash(msg_throttling_reason),hash(msg_app_version),hash(RealtyDevelopment_id),hash(metro_list),hash(distance_list),hash(district_list),hash(block_uids),deprecated,msg_blacklist_reason_id,selected_did,selected_metro,selected_road,was_interaction,hash(roads_list),hash(msg_random_id),msg_internet_connection,msg_socket_type,msg_is_push,hash(cities_list),msg_spam_confirm,hash(uids_rec),hash(email_hash),hash(target_hash),click_position,hash(phone_pdhash),hash(anonymous_phone_pdhash),hash(caller_phone_pdhash),hash(avitopro_date_preset),actual_time,hash(track_id),event_no,skill_id,cvpackage_id,safedeal_orderid,hash(msg_search_query),msg_search_success,msg_chat_page_num,option_number,short_term_rent,hash(profile_onboarding_step),hash(hierarchy_employee_name),issfdl,helpdesk_call_id,helpdesk_user_id,target_admuser_id,hash(cv_suggest_show_type),review_score,stage,satisfaction_score,satisfaction_score_reason,hash(satisfaction_score_reason_comment),hash(sgt_building_id),hash(display_state),hash(page_from),hash(item_condition),span_end_time,hash(custom_param),subs_vertical_id,shop_on_moderation,hash(parameter_value_slug),parameter_value_id,query_length,new_category_id,hash(new_params),hash(api_method_name),hash(receive_domestic),hash(receive_type),hash(receive_type_comment),hash(receive_type_express_delivery_comment),hash(seller_receive_reason),hash(seller_receive_reason_comment),hash(neutral_receive_reason),hash(neutral_receive_reason_comment),hash(express_delivery_receive_reason),hash(express_delivery_reason_receive_comment),hash(delivery_satisfied),hash(express_delivery_price),hash(courier_final_comment),hash(express_delivery_wanted),hash(price_suitable),courier_survey_price,hash(courier_survey_reasons),hash(courier_survey_reasons_comment),hash(courier_survey_receive_type_comment),hash(courier_survey_receive_type),screen_touch_time,msg_reason_id,hash(geo_session),hash(inactive_page),hash(location_suggest_text),answer_seq_id,hash(new_param_ids),hash(autoteka_cookie),hash(landing_slug),hash(autoteka_user_id),hash(utm_source),hash(utm_medium),hash(utm_campaign),autoteka_report_price,is_paid,is_from_avito,hash(autoteka_payment_method),autoteka_order_id,autoteka_report_id,hash(autoteka_source_names),hash(autoteka_source_blocks),autoteka_use_package,phone_id,hash(params_validation_errors),hash(tariff_upgrade_or_new),hash(safedeal_services),performance_timing_redirect_start,performance_timing_redirect_end,performance_timing_fetch_start,performance_timing_domain_lookup_start,performance_timing_domain_lookup_end,performance_timing_connect_start,performance_timing_secure_connection_start,performance_timing_connect_end,performance_timing_request_start,performance_timing_response_start,performance_timing_response_end,performance_timing_first_paint,performance_timing_first_contentful_paint,performance_timing_dom_interactive,performance_timing_dom_content_loaded_event_start,performance_timing_dom_content_loaded_event_end,performance_timing_dom_complete,performance_timing_load_event_start,performance_timing_load_event_end,hash(autoload_tags),hash(autoload_not_empty_tags),hash(autoload_updated_tags),msg_search_over_limit,screen_width,screen_height,is_new_tab,hash(autoload_region),hash(autoload_subway),hash(autoload_street),hash(autoload_district),hash(autoload_direction_road),autoload_distance_to_city,autoload_item_id,autoload_vertical_id,hash(tip_type),hash(autoload_id),hash(error_text),hash(geo_location_cookies),hash(safedeal_source),item_snapshot_time,is_rotation,hash(abuse_msg),cpa_abuse_id,hash(call_status),hash(alid),hash(sessid),ad_error,req_num,app_startup_time,is_from_ab_test,hash(upp_call_id),upp_provider_id,upp_virtual_phone,hash(upp_incoming_phone),hash(upp_client),hash(upp_linked_phone),hash(upp_allocate_id),upp_call_eventtype,upp_call_event_time,upp_call_is_blocked,upp_call_duration,upp_talk_duration,upp_call_accepted_at,upp_call_ended_at,hash(upp_record_url),upp_record,hash(upp_caller_message),hash(upp_call_receiver_message),hash(upp_transfer_result),hash(form_validation_error_param_ids),hash(form_validation_error_texts),hash(sgt_item_type),hash(landing_action),hash(chains),hash(prof_profile_type),save_type,phone_show_result,hash(perfvas_landing_from),autoload_total_items_cnt,autoload_new_items_cnt,autoload_updated_items_cnt,autoload_reactivated_items_cnt,autoload_deleted_items_cnt,autoload_unchanged_items_cnt,autoload_error_items_cnt,hash(chain),hash(new_chain),is_cached,hash(form_validation_error_param_slugs),hash(color_theme_status),is_external,service_status,upp_status,upp_status_date,checkbox_metro_ring,checkbox_metro_ring_in,status_date,upp_setting_id,an_setting_id,courier_orderid,iscourier,call_status_id,calltracking_activated,hash(realtor_profile_session),hash(realtor_sell_checkbox),hash(realtor_rent_checkbox),hash(stack_trace),hash(shop_fraud_reason_template_text),hash(images_list),hash(sgt_source_query),hash(sgt_user_query),collapse_group_id,valuable_item_id,__x_eid,__x_version,__x_src_id,__x_dtm,__x_cid,__x_lid,__x_mcid,__x_app,__x_offset,__x_pmax,__x_pmin,hash(__x_q),hash(__x_engine),hash(__x_srcp),__x_position,__x_show_position,hash(__x_engine_version),__x_iid,hash(suggest_ad_id),hash(rec_item_id_list),hash(rec_engine_list),hash(rec_engine_version_list),hash(ad_click_uuid),hash(click_uuid),hash(video_play_in_gallery),hash(open_video_link),click_from_block,hash(avitopro_search_query),hash(avitopro_search_vas_type),avitopro_search_on_avito_from,avitopro_search_on_avito_to,min_ts,max_ts,start_ts,finish_ts,hash(avitopro_search_hierarchy),hash(avitopro_search_locations),avitopro_search_price_from,avitopro_search_price_to,hash(avitopro_search_categories),hash(avitopro_search_autopublication),hash(avitopro_search_sort_type),hash(avitopro_search_sort_ascending),hash(avitopro_search_sort_statistic_date_from),hash(avitopro_search_sort_statistic_date_to),events_count,text_scale,platform,hash(courier_field_name),answer_id,hash(courier_survey_reasons_position),hash(upp_record_reason),hash(upp_block_reason),hash(content_size),map_zoom,voxim_auth,hash(appcall_scenario),hash(appcall_id),hash(appcall_choice),call_side,call_rating,mic_access,appcall_eventtype,caller_id,reciever_id,hash(caller_device),hash(reciever_device),hash(caller_network),hash(reciever_network),hash(appcall_result),hash(rec_items),bundle_type,hash(icon_type),config_update,last_config_update,hash(eid_list),app_start_earliest,hash(shield_type),hash(from_type),hash(avitopro_search_status_filter),hash(avito_search_iids),avitopro_search_isvertical,hash(avitopro_search_stats_type),hash(avitopro_stat_int_type),hash(webouuid),market_price,hash(auto_type_filter),hash(filter_nale),filter_block,max_range,min_range,voxim_quality,hash(appcall_network),hash(voxim_metric_type),appcall_callduration,appcall_talkduration,apprater_score,appcall_start,is_core_content,ntp_timestamp,hash(filter_name),hash(filter_block_name),sold_on_avito_feature,ct_status,is_control_sample,hash(vas_promt_type),hash(avitopro_search_params),hash(question),upp_is_abc,hash(order_cancel_cause_info_details),order_cancel_cause_info_details_id,hash(pin_type),hash(pin_state),performance_user_centric_interactive,performance_user_centric_first_input_delay,hash(search_suggest_serp),hash(filter_id),infm_clf_id,hash(infm_version),hash(infm_clf_tree),hash(msg_link_text),hash(msg_copy_text),search_features,hash(laas_tooltip_type),laas_answer,laas_tooltip_answer,hash(project_id),hash(scopes),hash(generation),hash(complectation),hash(modification),hash(x_sgt),hash(app_ui_theme),hash(app_ui_theme_setting_value),hash(change_screen),hash(filter_type),autoactivation,image_draw_time,image_load_time_delta,hash(image_type),msg_email_again,msg_copy_link_checker,performance_user_centric_total_blocking_time,hash(profprofiles_array),hash(image_url),image_status,hash(image_error),ice_breakers_id,hash(ice_breakers_ids),hash(raw_params),is_good_teaser,au_campaign_id,hash(event_landing_slug),story_id,hash(story_ids),hash(feedback_id),bytes_consumed,bytes_remained,hash(top_screen),hash(app_state),uptime,uppmon_actiondate,hash(cb_task),hash(cb_reason),cb_result,hash(cb_result_full),uppmon_offered_score,push_sent_time,hash(push_priority_sent),hash(push_priority_recieved),vox_push_ttl,vox_connected,hash(vox_push),hash(vox_call_id),hash(array_iids),hash(vox_userid),hash(antifraud_call_id),antifraud_call_datetime,antifraud_call_result,hash(antifraud_call_result_code),antifraud_call_duration,antifraud_call_hangupcause,item_quality_score,hash(filter_value_names),hash(filter_value_ids),hash(parameter_name),hash(parameter_value),use_suggest_item_add,hash(upp_project),report_application_cnt",
                             //"timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua),err=INT,hash(ip)=BIGINT,ipg=INT,new=BOOLEAN,post=BOOLEAN,hash(ref)=BIGINT,v=INT,hash(ab)=BIGINT,bot=BOOLEAN,hash(du)=BIGINT,hash(geo)=BIGINT,hash(url)=BIGINT,amqp=BOOLEAN,app=INT,hash(test)=BIGINT,clos=BOOLEAN,oc=BOOLEAN,bt=INT,hash(email)=BIGINT,ArrayBool_param=BOOLEAN,int_param=INT,hash(rqid)=BIGINT,hash(oldEmail)=BIGINT,lid=INT,rcid=INT,cid=INT,hash(q)=BIGINT,offset=INT,limit=INT,total=INT,shops=INT,oid=INT,hash(ak)=BIGINT,hash(contact)=BIGINT,hash(contactType)=BIGINT,hash(uniqueLink)=BIGINT,hash(operatingSystem)=BIGINT,hash(event)=BIGINT,mcid=INT,aid=INT,buid=INT,iid=INT,hash(mid)=BIGINT,fid=INT,hash(tx)=BIGINT,hash(crm_eventtype_id)=BIGINT,st=INT,hash(uem)=BIGINT,hash(type)=BIGINT,hash(vin)=BIGINT,hash(string_param)=BIGINT,location_id=INT,hash(params)=BIGINT,item_id=INT,hash(event_code)=BIGINT,categoryid=INT,hash(campaign_id)=BIGINT,hide_phone=BOOLEAN,hash(description)=BIGINT,hash(group_id)=BIGINT,hash(launch_id)=BIGINT,hash(phone)=BIGINT,userType=INT,hash(smsId)=BIGINT,manager_id=INT,hash(esid)=BIGINT,sid=INT,hash(name)=BIGINT,hash(email2)=BIGINT,snid=INT,hash(title)=BIGINT,price=DOUBLE,photos=INT,hash(photos_ids)=BIGINT,nomail=BOOLEAN,timestamp=INT,user_id=INT,hash(errors)=BIGINT,hash(ax)=BIGINT,hash(engine)=BIGINT,puid=INT,ignored=BOOLEAN,hash(msg)=BIGINT,hash(manager)=BIGINT,hash(addr)=BIGINT,hash(phone1)=BIGINT,inn=INT,kpp=INT,hash(addr2)=BIGINT,hash(addr1)=BIGINT,addrcp=BOOLEAN,hash(token)=BIGINT,success=BOOLEAN,source=INT,machine=BOOLEAN,survey_id=INT,hash(engine_version)=BIGINT,hash(iids)=BIGINT,lupint=INT,hash(surv_impid)=BIGINT,quick=BOOLEAN,hash(cmm)=BIGINT,hash(from)=BIGINT,status_id=INT,east=BOOLEAN,srid=INT,hash(event_group)=BIGINT,siid=INT,soid=INT,typeid=INT,cdt=INT,position=INT,hash(adtyped)=BIGINT,hash(reason)=BIGINT,diid=INT,dvas_applied=BOOLEAN,items=INT,news=BOOLEAN,notice=BOOLEAN,reminders=BOOLEAN,android_data_size_total=INT,android_data_size_cache=INT,android_data_size_databases=INT,android_data_size_files=INT,tid=INT,android_data_size_shared_prefs=INT,android_data_size_lib=INT,life_time=INT,android_data_size_app_webview=INT,android_data_size_no_backup=INT,android_data_size_code_cache=INT,hash(vsrc)=BIGINT,imgid=BIGINT,imgoid=BIGINT,hash(imgu)=BIGINT,hash(x)=BIGINT,hash(cm)=BIGINT,ida=BOOLEAN,hash(errors_detailed)=BIGINT,ops=INT,similars=INT,hash(result)=BIGINT,cp=BOOLEAN,hash(link)=BIGINT,hash(desc)=BIGINT,hash(sdesc)=BIGINT,hash(video)=BIGINT,public_stats=BOOLEAN,hash(tages)=BIGINT,srcId=INT,shopId=INT,hash(prob)=BIGINT,subscribe=BOOLEAN,hash(screen)=BIGINT,hash(bids)=BIGINT,cpm_floor=DOUBLE,hash(ids)=BIGINT,vid=INT,model=INT,iin=BOOLEAN,hash(pctrs)=BIGINT,reduce=INT,spid=INT,blockcode=INT,bid=INT,hash(rules)=BIGINT,hash(domain)=BIGINT,fnum=INT,lf_cnt=INT,hash(material)=BIGINT,hash(ptype)=BIGINT,orderid=INT,ctid=INT,hash(cids)=BIGINT,img=INT,complete=INT,pctr=DOUBLE,tariff=INT,lflid=INT,additional_user=INT,hash(pagetype)=BIGINT,hash(sum)=BIGINT,hash(tpl)=BIGINT,hash(lang)=BIGINT,hash(hash)=BIGINT,msgid=INT,sendtime=INT,hash(utmc)=BIGINT,hash(subject)=BIGINT,vol=INT,icnt=INT,hash(s)=BIGINT,hash(did)=BIGINT,hash(lids)=BIGINT,pmax=INT,pmin=INT,hash(priceunit)=BIGINT,hash(propertytype)=BIGINT,amnt=INT,lo=INT,lf_dt=INT,lf_grp=INT,ie=BOOLEAN,cost=DOUBLE,dirty=INT,redirect=BOOLEAN,hash(city)=BIGINT,hash(cname)=BIGINT,hash(photo_ids)=BIGINT,ts=INT,serviceid=INT,at=INT,hash(file)=BIGINT,hash(stack)=BIGINT,hash(meta)=BIGINT,pid=INT,shortcut=INT,hash(lq)=BIGINT,hash(mdistance)=BIGINT,hash(t)=BIGINT,hash(from_page)=BIGINT,hash(dname)=BIGINT,id=INT,lup=INT,hash(ls)=BIGINT,p=INT,hash(consent_act)=BIGINT,hash(snname)=BIGINT,hash(chid)=BIGINT,hash(recipient)=BIGINT,version=INT,cprice=DOUBLE,hash(sku)=BIGINT,hash(lf_type)=BIGINT,packageid=INT,hash(uids)=BIGINT,ex=INT,catid=INT,rid=INT,hash(response)=BIGINT,dpid=INT,hctr=DOUBLE,hash(listingfees)=BIGINT,microcategoryid=INT,sort=INT,icu=BOOLEAN,isa=BOOLEAN,itl=BOOLEAN,hour=INT,levid=INT,hash(brid)=BIGINT,hash(lc)=BIGINT,hash(lr)=BIGINT,hash(page)=BIGINT,uloggrd=BOOLEAN,hash(ar)=BIGINT,hash(dsp)=BIGINT,hash(bpos)=BIGINT,hash(adid)=BIGINT,hash(adomain)=BIGINT,delete=BOOLEAN,repeat=BOOLEAN,copy=BOOLEAN,area=DOUBLE,lgid=INT,floor=INT,result_state=INT,landscape=BOOLEAN,portrait=BOOLEAN,is_company=BOOLEAN,inca=BOOLEAN,hash(reg_config)=BIGINT,suid=INT,hash(f)=BIGINT,message_id=INT,screen_code=INT,hash(_url)=BIGINT,hash(menu_item)=BIGINT,lf_cost=INT,hash(vas_list)=BIGINT,last_contact=INT,sat=INT,operationid=INT,paysysid=INT,step=INT,vasid=INT,lastpaysysid=INT,onmap=INT,wmap=BOOLEAN,hash(rooms)=BIGINT,sgtd=INT,searchid=INT,hash(soldbytype)=BIGINT,ownid=INT,iafi=BOOLEAN,hash(query)=BIGINT,qs=DOUBLE,hash(device_id)=BIGINT,subscriptionid=INT,hash(ua_browser)=BIGINT,budget=DOUBLE,social_id=INT,hash(wizard_id)=BIGINT,userid=INT,usert=INT,vasfactid=INT,ctotalprice=DOUBLE,error=INT,ofid=INT,hash(push_token)=BIGINT,hash(ua_device)=BIGINT,hash(ua_os)=BIGINT,ulogged=BOOLEAN,wcid=INT,wmid=INT,finish=INT,hash(leid)=BIGINT,hash(liid)=BIGINT,start=INT,hash(beh)=BIGINT,google_play_services_library_version_code=INT,hash(google_play_services_version_name)=BIGINT,hash(chatid)=BIGINT,locationid=INT,hash(sortd)=BIGINT,paystat=INT,emee_id=INT,emey_id=INT,hash(irs)=BIGINT,hash(reschid)=BIGINT,souid=INT,ibofp=INT,ise=INT,hash(click_max_tm)=BIGINT,clicks=INT,expenses=DOUBLE,hash(impr_max_tm)=BIGINT,imprs=INT,lines=INT,hash(mtime)=BIGINT,size=INT,dupstatus=INT,google_play_services_version_code=INT,simulator=BOOLEAN,dupid=INT,hash(shortcut_id)=BIGINT,hash(csids)=BIGINT,hash(dmm)=BIGINT,hash(dsids)=BIGINT,pcid=INT,cpid=INT,hash(banner_type)=BIGINT,auid=INT,hash(vas_type)=BIGINT,subjid=INT,abuseid=INT,tsid=INT,google_play_services_compatibility=BOOLEAN,hash(google_play_services_library_version_name)=BIGINT,hash(notification_id)=BIGINT,hash(review)=BIGINT,hash(fieldsmaf)=BIGINT,assistive_touch_running=BOOLEAN,ac=INT,hash(cb)=BIGINT,hash(cy)=BIGINT,pc=INT,rt=INT,hash(uph)=BIGINT,is_official_nd=BOOLEAN,hash(status)=BIGINT,df_start_time=INT,df_expire_time=INT,hash(status_message)=BIGINT,hash(block_details)=BIGINT,zk=BOOLEAN,hash(test_field_slug)=BIGINT,hash(unm)=BIGINT,bold_text_enabled=BOOLEAN,isupgrade=BOOLEAN,tmid=INT,hash(sortf)=BIGINT,dmessage_id=INT,ap=BOOLEAN,hash(body)=BIGINT,hash(route)=BIGINT,hash(sids)=BIGINT,response_id=INT,expired=BOOLEAN,result_status=INT,result_time=INT,hash(answer_text)=BIGINT,answer_points=INT,survey_version=INT,is_test=BOOLEAN,hash(srcp)=BIGINT,accessibility_talkback=BOOLEAN,hash(photo_library_permission)=BIGINT,screen_brightness=DOUBLE,screen_dpi=INT,screen_height_dp=INT,screen_width_dp=INT,count=INT,app_height_dp=INT,hash(cnt)=BIGINT,hash(mcids)=BIGINT,hash(str)=BIGINT,hash(words)=BIGINT,dlvritem=INT,offdelivery=BOOLEAN,level=INT,pillarid=INT,isb2c=BOOLEAN,isdlvr=BOOLEAN,extt=INT,hash(contact_type)=BIGINT,accessibility_haptic=BOOLEAN,accessibility_is_on=BOOLEAN,accessibility_visual=BOOLEAN,app_width_dp=INT,hash(camera_permission)=BIGINT,configured_default_mail_client=BOOLEAN,darker_system_colors=BOOLEAN,disk_capacity=DOUBLE,energy_efficiency=DOUBLE,force_touch=BOOLEAN,free_disk_space=DOUBLE,grayscale=BOOLEAN,guided_access=BOOLEAN,invert_colors=BOOLEAN,jailbreak=BOOLEAN,hash(location_permission)=BIGINT,low_power_mode=BOOLEAN,hash(manufacturer)=BIGINT,memory_warning_count=INT,hash(platform_payment)=BIGINT,hash(push_notification_permission)=BIGINT,reduce_motion=BOOLEAN,reduce_transparency=BOOLEAN,resources_size=DOUBLE,shake_to_undo=BOOLEAN,speak_screen=BOOLEAN,speak_selection=BOOLEAN,hash(startup_item)=BIGINT,hash(startup_reason)=BIGINT,startup_time=DOUBLE,switch_control=BOOLEAN,hash(terminate_reason)=BIGINT,voice_over=BOOLEAN,watch_pair=BOOLEAN,fprice=DOUBLE,hash(bsize)=BIGINT,duration=INT,tab=INT,hash(action)=BIGINT,avatar_id=INT,moderator_id=INT,hash(resolution)=BIGINT,ever=INT,hash(original)=BIGINT,show=BOOLEAN,hash(abuseids)=BIGINT,mpact=INT,is_seller=BOOLEAN,op_dt=INT,hash(ns_channel)=BIGINT,hash(ns_type)=BIGINT,ns_value=BOOLEAN,is_user_auth=BOOLEAN,lf_lim=INT,dev_diid=INT,duid=INT,hash(dpush_id)=BIGINT,push_opened=BOOLEAN,dpush_delivered=BOOLEAN,hash(dpush_error)=BIGINT,hash(delivery)=BIGINT,hash(user_key)=BIGINT,stid=INT,active_icnt=INT,closed_icnt=INT,im=BOOLEAN,avatar_exist=BOOLEAN,hash(sgtx)=BIGINT,hash(sgt_tab)=BIGINT,hash(sgt1)=BIGINT,hash(sgt2)=BIGINT,hash(sgt3)=BIGINT,hash(sgt4)=BIGINT,hash(sgt5)=BIGINT,hash(sgt6)=BIGINT,hash(sgt1_mcids)=BIGINT,hash(sgt2_mcids)=BIGINT,hash(sgt3_mcids)=BIGINT,hash(sgt4_mcids)=BIGINT,hash(sgt5_mcids)=BIGINT,hash(sgt6_mcids)=BIGINT,sgtp=INT,is_legit=BOOLEAN,hash(sgt7)=BIGINT,hash(sgt8)=BIGINT,hash(sgt9)=BIGINT,hash(sgt7_mcids)=BIGINT,hash(sgt8_mcids)=BIGINT,hash(sgt9_mcids)=BIGINT,hash(x_trace)=BIGINT,hash(sesid)=BIGINT,hash(sgt)=BIGINT,hash(sgt_mcids)=BIGINT,emotion_id=INT,hash(objectid)=BIGINT,hash(objecttype)=BIGINT,hash(extension)=BIGINT,region_id=INT,is_geo=BOOLEAN,radius=INT,geo_lat=DOUBLE,geo_lng=DOUBLE,geo_acc=INT,geo_time=INT,bspage=INT,is_antihack_phone_confirm=BOOLEAN,antihack_confirm_number=INT,antihack_user_password_reset=BOOLEAN,hash(color)=BIGINT,hash(exif_model)=BIGINT,hash(exif_make)=BIGINT,str_available=BOOLEAN,create_time=INT,update_time=INT,hash(app_type)=BIGINT,iscredit=BOOLEAN,cperiod=INT,geo_acc_code=INT,hash(ver)=BIGINT,is_delivery=BOOLEAN,width=DOUBLE,height=DOUBLE,admuserid=INT,dur=DOUBLE,premoderation=BOOLEAN,hash(fraud_code_ids)=BIGINT,comparison_id=INT,duplicate_dur=DOUBLE,hash(duplicate_cand)=BIGINT,fsc=INT,target_id=INT,afraud_version=INT,hash(login_type)=BIGINT,is_first_message=BOOLEAN,hash(time_to_interact)=BIGINT,hash(network_type)=BIGINT,hash(date)=BIGINT,hash(plate_number)=BIGINT,hash(engine_power)=BIGINT,hash(transport_type)=BIGINT,hash(transport_category)=BIGINT,timing=DOUBLE,timing_duplicate=DOUBLE,hash(interface_version)=BIGINT,admuser_id=INT,hash(duplicate_candidates)=BIGINT,connection_speed=DOUBLE,hash(time_to_content)=BIGINT,screen_id=INT,hash(promo)=BIGINT,hash(exception_id)=BIGINT,hash(duplicates)=BIGINT,hash(to)=BIGINT,hash(abs)=BIGINT,hash(safedeal)=BIGINT,hash(content)=BIGINT,hash(login)=BIGINT,num_p=INT,dl=BOOLEAN,hash(subtype)=BIGINT,is_verified_inn=BOOLEAN,hash(srd)=BIGINT,hash(form_input_field_name)=BIGINT,hash(form_input_field_value)=BIGINT,hash(channels)=BIGINT,notification=INT,android_data_size_apk=INT,hash(channels_deny)=BIGINT,target_type=INT,hash(target)=BIGINT,hash(reg_uids)=BIGINT,cdtm=DOUBLE,pmid=INT,hash(datefrom)=BIGINT,hash(dateto)=BIGINT,hash(subs_vertical)=BIGINT,base_item_id=INT,hash(reject_wrong_params)=BIGINT,hash(state_id)=BIGINT,hash(parentstate_id)=BIGINT,from_block=INT,from_position=INT,screen_items_viewed=INT,screen_items_ad_viewed=INT,screen_items_vip_viewed=INT,screen_items_xl_viewed=INT,screen_items_common_viewed=INT,screen_items_new_viewed=INT,screen_items_geo_viewed=INT,screen_items_rec_viewed=INT,hash(service_id)=BIGINT,hash(amount)=BIGINT,hash(operation_id)=BIGINT,hash(uris)=BIGINT,isp=BOOLEAN,block_total=INT,page_total=INT,block_items_display=INT,block_items_added=INT,is_anon=BOOLEAN,hash(search_area)=BIGINT,hash(serpdisplay_type)=BIGINT,hash(landing_type)=BIGINT,retry=BOOLEAN,browser_web_push_agreement=BOOLEAN,is_auth=BOOLEAN,hash(snippet_placement)=BIGINT,vasid_prev=INT,hash(snippet_type)=BIGINT,hash(_ga)=BIGINT,tooltip_id=INT,resultTime=INT,devDiid=INT,hash(advtid)=BIGINT,sm=BOOLEAN,hash(consentAct)=BIGINT,answerPoints=INT,isOfficialNd=BOOLEAN,hash(responseId)=BIGINT,ach=BOOLEAN,premiums=INT,dfStartTime=INT,peid=INT,hash(answerText)=BIGINT,resultStatus=INT,dmessageId=INT,surveyVersion=INT,pdrid=INT,dfExpireTime=INT,usedSuggest=BOOLEAN,isTest=BOOLEAN,platformId=INT,ppeid=INT,surv_id=INT,hash(userToken)=BIGINT,img_id=INT,vips=INT,d=BOOLEAN,surveyId=INT,uas=INT,hash(sr)=BIGINT,groupId=INT,launchId=INT,hash(unique_args)=BIGINT,hash(data)=BIGINT,talkback=BOOLEAN,hash(model_version)=BIGINT,subs_price_bonus=DOUBLE,subs_price_packages=DOUBLE,subs_price_ext_shop=DOUBLE,subs_price_total=DOUBLE,with_shop_flg=BOOLEAN,abp=INT,hash(apply_status)=BIGINT,has_cv=BOOLEAN,phone_request_flg=BOOLEAN,hash(msg_image)=BIGINT,valid_until=INT,hash(discount_type)=BIGINT,rating=DOUBLE,reviews_cnt=INT,review_id=INT,ssv=INT,user_auth_suggestionshown=BOOLEAN,hash(share_place)=BIGINT,s_trg=INT,user_authmemo_id=INT,hash(user_auth_memo_id)=BIGINT,hash(call_id)=BIGINT,is_linked=BOOLEAN,hash(rdt)=BIGINT,hash(notification_message_id)=BIGINT,cnt_default_config=INT,cnt_default_items=INT,default_vas_amount=INT,hash(adm_comment)=BIGINT,item_version=INT,is_adblock=BOOLEAN,is_phone_new=BOOLEAN,hash(helpdesk_agent_state)=BIGINT,hash(anonymous_number_service_responce)=BIGINT,additional_item=INT,autoload_operation_activate=BOOLEAN,autoload_operation_add=BOOLEAN,autoload_operation_fee=BOOLEAN,autoload_operation_remove=BOOLEAN,autoload_operation_stop=BOOLEAN,autoload_operation_update=BOOLEAN,autoload_operation_vas=BOOLEAN,is_refresh=BOOLEAN,item_in_moderation=BOOLEAN,hash(raw_body)=BIGINT,call_action_id=INT,hash(call_action_type)=BIGINT,order_cancel_cause=INT,hash(order_cancel_cause_txt)=BIGINT,is_verified=BOOLEAN,other_phones_number=INT,item_number=INT,hash(phone_action)=BIGINT,is_reverified=BOOLEAN,hash(phone_action_type)=BIGINT,hash(order_cancel_cause_info)=BIGINT,hash(selected_suggest_value)=BIGINT,hash(i)=BIGINT,wrong_phone=BOOLEAN,hash(subs_form_type)=BIGINT,review_seq_id=INT,hash(errorsDetailed)=BIGINT,hash(subs_vertical_ids)=BIGINT,subs_tariff_id=INT,subs_proposed_bonus_factor=DOUBLE,hash(subs_extensions)=BIGINT,hash(subs_packages)=BIGINT,hash(caller_phone)=BIGINT,hash(caller_user_type)=BIGINT,hash(success_url)=BIGINT,hash(rent_start_datetime)=BIGINT,hash(rent_end_datetime)=BIGINT,hash(message_preview)=BIGINT,hash(from_notification_channel)=BIGINT,hash(car_doors)=BIGINT,hash(engine_type)=BIGINT,hash(transmission)=BIGINT,hash(car_drive)=BIGINT,hash(steering_wheel)=BIGINT,hash(engine_volume)=BIGINT,hash(item_status)=BIGINT,hash(aggregation)=BIGINT,hash(article_dissatisfaction_reason)=BIGINT,hash(domofond_notification_channel)=BIGINT,hash(domofond_subscription_type)=BIGINT,dnotification_message_id=INT,dnotification_delivered=BOOLEAN,hash(dnotification_error)=BIGINT,hash(anonymous_phone_number)=BIGINT,isbp=BOOLEAN,hash(slider_type)=BIGINT,hash(redirect_incoming_call_to)=BIGINT,hash(anonymous_phone)=BIGINT,hash(placement)=BIGINT,hash(destination)=BIGINT,hash(action_type)=BIGINT,hash(from_source)=BIGINT,cnt_favourites=INT,hash(clickstream_request_id)=BIGINT,ces_score=INT,hash(ces_context)=BIGINT,hash(ref_id)=BIGINT,hash(parent_ref_id)=BIGINT,hash(run_id)=BIGINT,pvas_group_old=INT,pvas_group=INT,hash(pvas_groups)=BIGINT,page_view_session_time=INT,hash(tab_id)=BIGINT,ctcallid=INT,is_geo_delivery_widget=BOOLEAN,hash(api_path)=BIGINT,hash(antihack_reason)=BIGINT,hash(buyer_booking_cancel_cause)=BIGINT,hash(buyer_booking_cancel_cause_text)=BIGINT,ticket_comment_id=INT,ticket_comment_dt=INT,src_split_ticket_id=INT,trgt_split_ticket_id=INT,time_to_first_byte=INT,time_to_first_paint=INT,time_to_first_interactive=INT,time_to_dom_ready=INT,time_to_on_load=INT,time_to_target_content=INT,time_for_async_target_content=INT,hash(avito_advice_article)=BIGINT,hash(payerid)=BIGINT,changed_microcat_id=INT,hash(scenario)=BIGINT,hash(userAgent)=BIGINT,cv_use_category=BOOLEAN,cv_use_title=BOOLEAN,hash(requestParams)=BIGINT,hash(inputValue)=BIGINT,hash(inputField)=BIGINT,offerId=INT,cityId=INT,regionId=INT,classifiedId=INT,feedId=INT,clientId=INT,clientNeedId=INT,lifeTime=INT,leadId=INT,memberId=INT,executiveMemberId=INT,hash(executiveType)=BIGINT,optionsNumber=INT,hash(str_buyer_contact_result)=BIGINT,hash(str_buyer_contact_result_reason)=BIGINT,hash(str_buyer_contact_result_reason_text)=BIGINT,second=DOUBLE,hash(profile_tab)=BIGINT,is_original_user_report=BOOLEAN,autoteka_user_type=INT,list_load_number=INT,ssid=INT,hash(screen_orientation)=BIGINT,hash(recommendpaysysid)=BIGINT,parent_uid=INT,hash(app_version)=BIGINT,hash(os_version)=BIGINT,accounts_number=INT,hash(search_correction_action)=BIGINT,hash(search_correction_original)=BIGINT,hash(search_correction_corrected)=BIGINT,hash(search_correction_method)=BIGINT,autosort_images=BOOLEAN,cnt_subscribers=INT,is_oasis=BOOLEAN,xl=BOOLEAN,hash(pvas_dates)=BIGINT,oneclickpayment=BOOLEAN,call_reason=INT,status_code=INT,api_dt=INT,hash(project)=BIGINT,hash(location_text_input)=BIGINT,hash(cadastralnumber)=BIGINT,report_duration=INT,report_status=BOOLEAN,hash(other_shop_items)=BIGINT,page_number=INT,hash(phone2)=BIGINT,hash(skype)=BIGINT,hash(shop_url)=BIGINT,hash(shop_name)=BIGINT,hash(shop_site)=BIGINT,hash(short_description)=BIGINT,hash(long_description)=BIGINT,hash(work_regime)=BIGINT,hash(info_line)=BIGINT,hash(shop_logo)=BIGINT,hash(shop_branding)=BIGINT,hash(shop_back)=BIGINT,hash(shop_photos)=BIGINT,on_avito_since=INT,ssfid=INT,hash(deep_link)=BIGINT,hash(item_add_screen)=BIGINT,hash(user_ids)=BIGINT,empl_id=INT,hash(empl_invite_state)=BIGINT,hash(prev_page)=BIGINT,hash(shop_fraud_reason_ids)=BIGINT,shop_moderation_action_id=INT,hash(shop_moderation_action_hash)=BIGINT,hash(service_name)=BIGINT,hash(deploy_env)=BIGINT,hash(cid_tree_branch)=BIGINT,hash(lid_tree_branch)=BIGINT,cur_i_status_id=INT,cur_i_status_time=INT,item_version_time=INT,hash(infomodel_v_id)=BIGINT,checkbox_an_enable=BOOLEAN,hash(message_type)=BIGINT,hash(app_version_code)=BIGINT,hash(banner_id)=BIGINT,hash(shortcut_description)=BIGINT,close_timeout=INT,hash(note_text)=BIGINT,hash(item_parameter_name)=BIGINT,hash(block_id)=BIGINT,hash(selling_system)=BIGINT,hash(banner_code)=BIGINT,hash(wsrc)=BIGINT,hash(shops_array)=BIGINT,rec_item_id=INT,lfpackage_id=INT,subscription_promo=BOOLEAN,sub_is_ss=BOOLEAN,hash(sub_prolong)=BIGINT,hash(target_page)=BIGINT,mobile_event_duration=INT,hash(screen_session_uid)=BIGINT,hash(screen_name)=BIGINT,hash(content_type)=BIGINT,mobile_app_page_number=INT,hash(roads)=BIGINT,reload=BOOLEAN,img_download_status=BOOLEAN,screen_start_time=BIGINT,hash(service_branch)=BIGINT,sgt_cat_flag=BOOLEAN,hash(notification_owner)=BIGINT,hash(notification_type)=BIGINT,hash(ns_owner)=BIGINT,hash(operation_ids)=BIGINT,hash(software_version)=BIGINT,hash(build)=BIGINT,adpartner=INT,ticket_channel=INT,hash(adslot)=BIGINT,statid=INT,is_payed_immediately=BOOLEAN,hash(lfpackage_ids)=BIGINT,hash(current_subs_version)=BIGINT,hash(new_subs_version)=BIGINT,subscription_edit=BOOLEAN,channel_number=INT,channels_screen_count=INT,hash(domofond_item_type)=BIGINT,hash(domofond_item_id)=BIGINT,item_position=INT,hash(delivery_help_question)=BIGINT,hash(domofond_cookie)=BIGINT,hash(domofond_user_token)=BIGINT,banner_due_date=INT,banner_show_days=BOOLEAN,banner_item_id=INT,hash(empty_serp_link_type)=BIGINT,hash(domofond_proxy_request_name)=BIGINT,hash(chat_error_case)=BIGINT,has_messages=BOOLEAN,hash(search_address_type)=BIGINT,domofond_user_id=INT,hash(js_event_type)=BIGINT,hash(dom_node)=BIGINT,hash(js_event_slug)=BIGINT,hash(attr_title)=BIGINT,hash(attr_link)=BIGINT,hash(attr_value)=BIGINT,hash(key_name)=BIGINT,is_ctrl_pressed=BOOLEAN,is_alt_pressed=BOOLEAN,is_shift_pressed=BOOLEAN,hash(color_theme)=BIGINT,hash(dom_node_content)=BIGINT,is_checkbox_checked=BOOLEAN,page_x_coord=INT,page_y_coord=INT,hash(srd_initial)=BIGINT,last_show_dt=INT,hash(domofond_button_name)=BIGINT,uploaded_files_cnt=INT,hash(review_additional_info)=BIGINT,flow_type=INT,hash(flow_id)=BIGINT,ces_article=INT,items_locked_count=INT,items_not_locked_count=INT,word_sgt_clicks=INT,hash(metro)=BIGINT,hash(msg_app_name)=BIGINT,hash(msg_request)=BIGINT,hash(domofond_category)=BIGINT,hash(domofond_subcategory)=BIGINT,hash(domofond_filter_region)=BIGINT,hash(domofond_filter_location)=BIGINT,hash(domofond_filter_rooms)=BIGINT,hash(domofond_filter_price_from)=BIGINT,hash(domofond_filter_price_up)=BIGINT,hash(domofond_filter_rent_type)=BIGINT,hash(domofond_search_result_type)=BIGINT,moderation_user_score=DOUBLE,domofond_search_position=INT,domofond_saved_search_result=BOOLEAN,domofond_favorites_search_result=BOOLEAN,hash(msg_button_id)=BIGINT,hash(msg_button_type)=BIGINT,hash(action_payload)=BIGINT,previous_contacts_cnt=INT,push_allowed=BOOLEAN,msg_chat_list_offset=INT,ces_hd=INT,hash(msg_throttling_reason)=BIGINT,hash(msg_app_version)=BIGINT,hash(RealtyDevelopment_id)=BIGINT,hash(metro_list)=BIGINT,hash(distance_list)=BIGINT,hash(district_list)=BIGINT,hash(block_uids)=BIGINT,deprecated=BOOLEAN,msg_blacklist_reason_id=INT,selected_did=INT,selected_metro=INT,selected_road=INT,was_interaction=INT,hash(roads_list)=BIGINT,hash(msg_random_id)=BIGINT,msg_internet_connection=BOOLEAN,msg_socket_type=INT,msg_is_push=BOOLEAN,hash(cities_list)=BIGINT,msg_spam_confirm=BOOLEAN,hash(uids_rec)=BIGINT,hash(email_hash)=BIGINT,hash(target_hash)=BIGINT,click_position=INT,hash(phone_pdhash)=BIGINT,hash(anonymous_phone_pdhash)=BIGINT,hash(caller_phone_pdhash)=BIGINT,hash(avitopro_date_preset)=BIGINT,actual_time=DOUBLE,hash(track_id)=BIGINT,event_no=INT,skill_id=INT,cvpackage_id=INT,safedeal_orderid=INT,hash(msg_search_query)=BIGINT,msg_search_success=BOOLEAN,msg_chat_page_num=INT,option_number=INT,short_term_rent=BOOLEAN,hash(profile_onboarding_step)=BIGINT,hash(hierarchy_employee_name)=BIGINT,issfdl=BOOLEAN,helpdesk_call_id=INT,helpdesk_user_id=INT,target_admuser_id=INT,hash(cv_suggest_show_type)=BIGINT,review_score=INT,stage=INT,satisfaction_score=INT,satisfaction_score_reason=INT,hash(satisfaction_score_reason_comment)=BIGINT,hash(sgt_building_id)=BIGINT,hash(display_state)=BIGINT,hash(page_from)=BIGINT,hash(item_condition)=BIGINT,span_end_time=BIGINT,hash(custom_param)=BIGINT,subs_vertical_id=INT,shop_on_moderation=BOOLEAN,hash(parameter_value_slug)=BIGINT,parameter_value_id=INT,query_length=INT,new_category_id=INT,hash(new_params)=BIGINT,hash(api_method_name)=BIGINT,hash(receive_domestic)=BIGINT,hash(receive_type)=BIGINT,hash(receive_type_comment)=BIGINT,hash(receive_type_express_delivery_comment)=BIGINT,hash(seller_receive_reason)=BIGINT,hash(seller_receive_reason_comment)=BIGINT,hash(neutral_receive_reason)=BIGINT,hash(neutral_receive_reason_comment)=BIGINT,hash(express_delivery_receive_reason)=BIGINT,hash(express_delivery_reason_receive_comment)=BIGINT,hash(delivery_satisfied)=BIGINT,hash(express_delivery_price)=BIGINT,hash(courier_final_comment)=BIGINT,hash(express_delivery_wanted)=BIGINT,hash(price_suitable)=BIGINT,courier_survey_price=INT,hash(courier_survey_reasons)=BIGINT,hash(courier_survey_reasons_comment)=BIGINT,hash(courier_survey_receive_type_comment)=BIGINT,hash(courier_survey_receive_type)=BIGINT,screen_touch_time=BIGINT,msg_reason_id=INT,hash(geo_session)=BIGINT,hash(inactive_page)=BIGINT,hash(location_suggest_text)=BIGINT,answer_seq_id=INT,hash(new_param_ids)=BIGINT,hash(autoteka_cookie)=BIGINT,hash(landing_slug)=BIGINT,hash(autoteka_user_id)=BIGINT,hash(utm_source)=BIGINT,hash(utm_medium)=BIGINT,hash(utm_campaign)=BIGINT,autoteka_report_price=INT,is_paid=BOOLEAN,is_from_avito=BOOLEAN,hash(autoteka_payment_method)=BIGINT,autoteka_order_id=INT,autoteka_report_id=INT,hash(autoteka_source_names)=BIGINT,hash(autoteka_source_blocks)=BIGINT,autoteka_use_package=BOOLEAN,phone_id=INT,hash(params_validation_errors)=BIGINT,hash(tariff_upgrade_or_new)=BIGINT,hash(safedeal_services)=BIGINT,performance_timing_redirect_start=DOUBLE,performance_timing_redirect_end=DOUBLE,performance_timing_fetch_start=DOUBLE,performance_timing_domain_lookup_start=DOUBLE,performance_timing_domain_lookup_end=DOUBLE,performance_timing_connect_start=DOUBLE,performance_timing_secure_connection_start=DOUBLE,performance_timing_connect_end=DOUBLE,performance_timing_request_start=DOUBLE,performance_timing_response_start=DOUBLE,performance_timing_response_end=DOUBLE,performance_timing_first_paint=DOUBLE,performance_timing_first_contentful_paint=DOUBLE,performance_timing_dom_interactive=DOUBLE,performance_timing_dom_content_loaded_event_start=DOUBLE,performance_timing_dom_content_loaded_event_end=DOUBLE,performance_timing_dom_complete=DOUBLE,performance_timing_load_event_start=DOUBLE,performance_timing_load_event_end=DOUBLE,hash(autoload_tags)=BIGINT,hash(autoload_not_empty_tags)=BIGINT,hash(autoload_updated_tags)=BIGINT,msg_search_over_limit=BOOLEAN,screen_width=INT,screen_height=INT,is_new_tab=BOOLEAN,hash(autoload_region)=BIGINT,hash(autoload_subway)=BIGINT,hash(autoload_street)=BIGINT,hash(autoload_district)=BIGINT,hash(autoload_direction_road)=BIGINT,autoload_distance_to_city=INT,autoload_item_id=INT,autoload_vertical_id=INT,hash(tip_type)=BIGINT,hash(autoload_id)=BIGINT,hash(error_text)=BIGINT,hash(geo_location_cookies)=BIGINT,hash(safedeal_source)=BIGINT,item_snapshot_time=DOUBLE,is_rotation=BOOLEAN,hash(abuse_msg)=BIGINT,cpa_abuse_id=INT,hash(call_status)=BIGINT,hash(alid)=BIGINT,hash(sessid)=BIGINT,ad_error=INT,req_num=INT,app_startup_time=BIGINT,is_from_ab_test=BOOLEAN,hash(upp_call_id)=BIGINT,upp_provider_id=INT,upp_virtual_phone=BIGINT,hash(upp_incoming_phone)=BIGINT,hash(upp_client)=BIGINT,hash(upp_linked_phone)=BIGINT,hash(upp_allocate_id)=BIGINT,upp_call_eventtype=INT,upp_call_event_time=INT,upp_call_is_blocked=BOOLEAN,upp_call_duration=INT,upp_talk_duration=INT,upp_call_accepted_at=INT,upp_call_ended_at=INT,hash(upp_record_url)=BIGINT,upp_record=BOOLEAN,hash(upp_caller_message)=BIGINT,hash(upp_call_receiver_message)=BIGINT,hash(upp_transfer_result)=BIGINT,hash(form_validation_error_param_ids)=BIGINT,hash(form_validation_error_texts)=BIGINT,hash(sgt_item_type)=BIGINT,hash(landing_action)=BIGINT,hash(chains)=BIGINT,hash(prof_profile_type)=BIGINT,save_type=INT,phone_show_result=INT,hash(perfvas_landing_from)=BIGINT,autoload_total_items_cnt=INT,autoload_new_items_cnt=INT,autoload_updated_items_cnt=INT,autoload_reactivated_items_cnt=INT,autoload_deleted_items_cnt=INT,autoload_unchanged_items_cnt=INT,autoload_error_items_cnt=INT,hash(chain)=BIGINT,hash(new_chain)=BIGINT,is_cached=BOOLEAN,hash(form_validation_error_param_slugs)=BIGINT,hash(color_theme_status)=BIGINT,is_external=BOOLEAN,service_status=BOOLEAN,upp_status=INT,upp_status_date=INT,checkbox_metro_ring=BOOLEAN,checkbox_metro_ring_in=BOOLEAN,status_date=INT,upp_setting_id=INT,an_setting_id=INT,courier_orderid=INT,iscourier=BOOLEAN,call_status_id=INT,calltracking_activated=BOOLEAN,hash(realtor_profile_session)=BIGINT,hash(realtor_sell_checkbox)=BIGINT,hash(realtor_rent_checkbox)=BIGINT,hash(stack_trace)=BIGINT,hash(shop_fraud_reason_template_text)=BIGINT,hash(images_list)=BIGINT,hash(sgt_source_query)=BIGINT,hash(sgt_user_query)=BIGINT,collapse_group_id=INT,valuable_item_id=INT,__x_eid=INT,__x_version=INT,__x_src_id=INT,__x_dtm=DOUBLE,__x_cid=INT,__x_lid=INT,__x_mcid=INT,__x_app=INT,__x_offset=INT,__x_pmax=INT,__x_pmin=INT,hash(__x_q)=BIGINT,hash(__x_engine)=BIGINT,hash(__x_srcp)=BIGINT,__x_position=INT,__x_show_position=INT,hash(__x_engine_version)=BIGINT,__x_iid=INT,hash(suggest_ad_id)=BIGINT,hash(rec_item_id_list)=BIGINT,hash(rec_engine_list)=BIGINT,hash(rec_engine_version_list)=BIGINT,hash(ad_click_uuid)=BIGINT,hash(click_uuid)=BIGINT,hash(video_play_in_gallery)=BIGINT,hash(open_video_link)=BIGINT,click_from_block=INT,hash(avitopro_search_query)=BIGINT,hash(avitopro_search_vas_type)=BIGINT,avitopro_search_on_avito_from=INT,avitopro_search_on_avito_to=INT,min_ts=BIGINT,max_ts=BIGINT,start_ts=BIGINT,finish_ts=BIGINT,hash(avitopro_search_hierarchy)=BIGINT,hash(avitopro_search_locations)=BIGINT,avitopro_search_price_from=INT,avitopro_search_price_to=INT,hash(avitopro_search_categories)=BIGINT,hash(avitopro_search_autopublication)=BIGINT,hash(avitopro_search_sort_type)=BIGINT,hash(avitopro_search_sort_ascending)=BIGINT,hash(avitopro_search_sort_statistic_date_from)=BIGINT,hash(avitopro_search_sort_statistic_date_to)=BIGINT,events_count=INT,text_scale=DOUBLE,platform=INT,hash(courier_field_name)=BIGINT,answer_id=INT,hash(courier_survey_reasons_position)=BIGINT,hash(upp_record_reason)=BIGINT,hash(upp_block_reason)=BIGINT,hash(content_size)=BIGINT,map_zoom=INT,voxim_auth=BOOLEAN,hash(appcall_scenario)=BIGINT,hash(appcall_id)=BIGINT,hash(appcall_choice)=BIGINT,call_side=INT,call_rating=INT,mic_access=BOOLEAN,appcall_eventtype=INT,caller_id=INT,reciever_id=INT,hash(caller_device)=BIGINT,hash(reciever_device)=BIGINT,hash(caller_network)=BIGINT,hash(reciever_network)=BIGINT,hash(appcall_result)=BIGINT,hash(rec_items)=BIGINT,bundle_type=INT,hash(icon_type)=BIGINT,config_update=BOOLEAN,last_config_update=DOUBLE,hash(eid_list)=BIGINT,app_start_earliest=DOUBLE,hash(shield_type)=BIGINT,hash(from_type)=BIGINT,hash(avitopro_search_status_filter)=BIGINT,hash(avito_search_iids)=BIGINT,avitopro_search_isvertical=BOOLEAN,hash(avitopro_search_stats_type)=BIGINT,hash(avitopro_stat_int_type)=BIGINT,hash(webouuid)=BIGINT,market_price=INT,hash(auto_type_filter)=BIGINT,hash(filter_nale)=BIGINT,filter_block=BOOLEAN,max_range=INT,min_range=INT,voxim_quality=INT,hash(appcall_network)=BIGINT,hash(voxim_metric_type)=BIGINT,appcall_callduration=INT,appcall_talkduration=INT,apprater_score=INT,appcall_start=INT,is_core_content=BOOLEAN,ntp_timestamp=DOUBLE,hash(filter_name)=BIGINT,hash(filter_block_name)=BIGINT,sold_on_avito_feature=BOOLEAN,ct_status=BOOLEAN,is_control_sample=BOOLEAN,hash(vas_promt_type)=BIGINT,hash(avitopro_search_params)=BIGINT,hash(question)=BIGINT,upp_is_abc=BOOLEAN,hash(order_cancel_cause_info_details)=BIGINT,order_cancel_cause_info_details_id=INT,hash(pin_type)=BIGINT,hash(pin_state)=BIGINT,performance_user_centric_interactive=DOUBLE,performance_user_centric_first_input_delay=DOUBLE,hash(search_suggest_serp)=BIGINT,hash(filter_id)=BIGINT,infm_clf_id=INT,hash(infm_version)=BIGINT,hash(infm_clf_tree)=BIGINT,hash(msg_link_text)=BIGINT,hash(msg_copy_text)=BIGINT,search_features=INT,hash(laas_tooltip_type)=BIGINT,laas_answer=INT,laas_tooltip_answer=INT,hash(project_id)=BIGINT,hash(scopes)=BIGINT,hash(generation)=BIGINT,hash(complectation)=BIGINT,hash(modification)=BIGINT,hash(x_sgt)=BIGINT,hash(app_ui_theme)=BIGINT,hash(app_ui_theme_setting_value)=BIGINT,hash(change_screen)=BIGINT,hash(filter_type)=BIGINT,autoactivation=BOOLEAN,image_draw_time=BIGINT,image_load_time_delta=DOUBLE,hash(image_type)=BIGINT,msg_email_again=BOOLEAN,msg_copy_link_checker=BOOLEAN,performance_user_centric_total_blocking_time=DOUBLE,hash(profprofiles_array)=BIGINT,hash(image_url)=BIGINT,image_status=BOOLEAN,hash(image_error)=BIGINT,ice_breakers_id=INT,hash(ice_breakers_ids)=BIGINT,hash(raw_params)=BIGINT,is_good_teaser=BOOLEAN,au_campaign_id=INT,hash(event_landing_slug)=BIGINT,story_id=INT,hash(story_ids)=BIGINT,hash(feedback_id)=BIGINT,bytes_consumed=INT,bytes_remained=INT,hash(top_screen)=BIGINT,hash(app_state)=BIGINT,uptime=INT,uppmon_actiondate=INT,hash(cb_task)=BIGINT,hash(cb_reason)=BIGINT,cb_result=INT,hash(cb_result_full)=BIGINT,uppmon_offered_score=INT,push_sent_time=INT,hash(push_priority_sent)=BIGINT,hash(push_priority_recieved)=BIGINT,vox_push_ttl=INT,vox_connected=BOOLEAN,hash(vox_push)=BIGINT,hash(vox_call_id)=BIGINT,hash(array_iids)=BIGINT,hash(vox_userid)=BIGINT,hash(antifraud_call_id)=BIGINT,antifraud_call_datetime=INT,antifraud_call_result=INT,hash(antifraud_call_result_code)=BIGINT,antifraud_call_duration=INT,antifraud_call_hangupcause=INT,item_quality_score=DOUBLE,hash(filter_value_names)=BIGINT,hash(filter_value_ids)=BIGINT,hash(parameter_name)=BIGINT,hash(parameter_value)=BIGINT,use_suggest_item_add=BOOLEAN,hash(upp_project)=BIGINT,report_application_cnt=INT",
                             //"timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua),err=INT,ipg=INT,new=BOOLEAN,post=BOOLEAN,v=INT,bot=BOOLEAN,amqp=BOOLEAN,app=INT,clos=BOOLEAN,oc=BOOLEAN,bt=INT,ArrayBool_param=BOOLEAN,int_param=INT,lid=INT,rcid=INT,cid=INT,offset=INT,limit=INT,total=INT,shops=INT,oid=INT,mcid=INT,aid=INT,buid=INT,iid=INT,fid=INT,st=INT,location_id=INT,item_id=INT,categoryid=INT,hide_phone=BOOLEAN,userType=INT,manager_id=INT,sid=INT,snid=INT,price=DOUBLE,photos=INT,nomail=BOOLEAN,timestamp=INT,user_id=INT,puid=INT,ignored=BOOLEAN,inn=INT,kpp=INT,addrcp=BOOLEAN,success=BOOLEAN,source=INT,machine=BOOLEAN,survey_id=INT,lupint=INT,quick=BOOLEAN,status_id=INT,east=BOOLEAN,srid=INT,siid=INT,soid=INT,typeid=INT,cdt=INT,position=INT,diid=INT,dvas_applied=BOOLEAN,items=INT,news=BOOLEAN,notice=BOOLEAN,reminders=BOOLEAN,android_data_size_total=INT,android_data_size_cache=INT,android_data_size_databases=INT,android_data_size_files=INT,tid=INT,android_data_size_shared_prefs=INT,android_data_size_lib=INT,life_time=INT,android_data_size_app_webview=INT,android_data_size_no_backup=INT,android_data_size_code_cache=INT,ida=BOOLEAN,ops=INT,similars=INT,cp=BOOLEAN,public_stats=BOOLEAN,srcId=INT,shopId=INT,subscribe=BOOLEAN,cpm_floor=DOUBLE,vid=INT,model=INT,iin=BOOLEAN,reduce=INT,spid=INT,blockcode=INT,bid=INT,fnum=INT,lf_cnt=INT,orderid=INT,ctid=INT,img=INT,complete=INT,pctr=DOUBLE,tariff=INT,lflid=INT,additional_user=INT,msgid=INT,sendtime=INT,vol=INT,icnt=INT,pmax=INT,pmin=INT,amnt=INT,lo=INT,lf_dt=INT,lf_grp=INT,ie=BOOLEAN,cost=DOUBLE,dirty=INT,redirect=BOOLEAN,ts=INT,serviceid=INT,at=INT,pid=INT,shortcut=INT,id=INT,lup=INT,p=INT,version=INT,cprice=DOUBLE,packageid=INT,ex=INT,catid=INT,rid=INT,dpid=INT,hctr=DOUBLE,microcategoryid=INT,sort=INT,icu=BOOLEAN,isa=BOOLEAN,itl=BOOLEAN,hour=INT,levid=INT,uloggrd=BOOLEAN,delete=BOOLEAN,repeat=BOOLEAN,copy=BOOLEAN,area=DOUBLE,lgid=INT,floor=INT,result_state=INT,landscape=BOOLEAN,portrait=BOOLEAN,is_company=BOOLEAN,inca=BOOLEAN,suid=INT,message_id=INT,screen_code=INT,lf_cost=INT,last_contact=INT,sat=INT,operationid=INT,paysysid=INT,step=INT,vasid=INT,lastpaysysid=INT,onmap=INT,wmap=BOOLEAN,sgtd=INT,searchid=INT,ownid=INT,iafi=BOOLEAN,qs=DOUBLE,subscriptionid=INT,budget=DOUBLE,social_id=INT,userid=INT,usert=INT,vasfactid=INT,ctotalprice=DOUBLE,error=INT,ofid=INT,ulogged=BOOLEAN,wcid=INT,wmid=INT,finish=INT,start=INT,google_play_services_library_version_code=INT,locationid=INT,paystat=INT,emee_id=INT,emey_id=INT,souid=INT,ibofp=INT,ise=INT,clicks=INT,expenses=DOUBLE,imprs=INT,lines=INT,size=INT,dupstatus=INT,google_play_services_version_code=INT,simulator=BOOLEAN,dupid=INT,pcid=INT,cpid=INT,auid=INT,subjid=INT,abuseid=INT,tsid=INT,google_play_services_compatibility=BOOLEAN,assistive_touch_running=BOOLEAN,ac=INT,pc=INT,rt=INT,is_official_nd=BOOLEAN,df_start_time=INT,df_expire_time=INT,zk=BOOLEAN,bold_text_enabled=BOOLEAN,isupgrade=BOOLEAN,tmid=INT,dmessage_id=INT,ap=BOOLEAN,response_id=INT,expired=BOOLEAN,result_status=INT,result_time=INT,answer_points=INT,survey_version=INT,is_test=BOOLEAN,accessibility_talkback=BOOLEAN,screen_brightness=DOUBLE,screen_dpi=INT,screen_height_dp=INT,screen_width_dp=INT,count=INT,app_height_dp=INT,dlvritem=INT,offdelivery=BOOLEAN,level=INT,pillarid=INT,isb2c=BOOLEAN,isdlvr=BOOLEAN,extt=INT,accessibility_haptic=BOOLEAN,accessibility_is_on=BOOLEAN,accessibility_visual=BOOLEAN,app_width_dp=INT,configured_default_mail_client=BOOLEAN,darker_system_colors=BOOLEAN,disk_capacity=DOUBLE,energy_efficiency=DOUBLE,force_touch=BOOLEAN,free_disk_space=DOUBLE,grayscale=BOOLEAN,guided_access=BOOLEAN,invert_colors=BOOLEAN,jailbreak=BOOLEAN,low_power_mode=BOOLEAN,memory_warning_count=INT,reduce_motion=BOOLEAN,reduce_transparency=BOOLEAN,resources_size=DOUBLE,shake_to_undo=BOOLEAN,speak_screen=BOOLEAN,speak_selection=BOOLEAN,startup_time=DOUBLE,switch_control=BOOLEAN,voice_over=BOOLEAN,watch_pair=BOOLEAN,fprice=DOUBLE,duration=INT,tab=INT,avatar_id=INT,moderator_id=INT,ever=INT,show=BOOLEAN,mpact=INT,is_seller=BOOLEAN,op_dt=INT,ns_value=BOOLEAN,is_user_auth=BOOLEAN,lf_lim=INT,dev_diid=INT,duid=INT,push_opened=BOOLEAN,dpush_delivered=BOOLEAN,stid=INT,active_icnt=INT,closed_icnt=INT,im=BOOLEAN,avatar_exist=BOOLEAN,sgtp=INT,is_legit=BOOLEAN,emotion_id=INT,region_id=INT,is_geo=BOOLEAN,radius=INT,geo_lat=DOUBLE,geo_lng=DOUBLE,geo_acc=INT,geo_time=INT,bspage=INT,is_antihack_phone_confirm=BOOLEAN,antihack_confirm_number=INT,antihack_user_password_reset=BOOLEAN,str_available=BOOLEAN,create_time=INT,update_time=INT,iscredit=BOOLEAN,cperiod=INT,geo_acc_code=INT,is_delivery=BOOLEAN,width=DOUBLE,height=DOUBLE,admuserid=INT,dur=DOUBLE,premoderation=BOOLEAN,comparison_id=INT,duplicate_dur=DOUBLE,fsc=INT,target_id=INT,afraud_version=INT,is_first_message=BOOLEAN,timing=DOUBLE,timing_duplicate=DOUBLE,admuser_id=INT,connection_speed=DOUBLE,screen_id=INT,num_p=INT,dl=BOOLEAN,is_verified_inn=BOOLEAN,notification=INT,android_data_size_apk=INT,target_type=INT,cdtm=DOUBLE,pmid=INT,base_item_id=INT,from_block=INT,from_position=INT,screen_items_viewed=INT,screen_items_ad_viewed=INT,screen_items_vip_viewed=INT,screen_items_xl_viewed=INT,screen_items_common_viewed=INT,screen_items_new_viewed=INT,screen_items_geo_viewed=INT,screen_items_rec_viewed=INT,isp=BOOLEAN,block_total=INT,page_total=INT,block_items_display=INT,block_items_added=INT,is_anon=BOOLEAN,retry=BOOLEAN,browser_web_push_agreement=BOOLEAN,is_auth=BOOLEAN,vasid_prev=INT,tooltip_id=INT,resultTime=INT,devDiid=INT,sm=BOOLEAN,answerPoints=INT,isOfficialNd=BOOLEAN,ach=BOOLEAN,premiums=INT,dfStartTime=INT,peid=INT,resultStatus=INT,dmessageId=INT,surveyVersion=INT,pdrid=INT,dfExpireTime=INT,usedSuggest=BOOLEAN,isTest=BOOLEAN,platformId=INT,ppeid=INT,surv_id=INT,img_id=INT,vips=INT,d=BOOLEAN,surveyId=INT,uas=INT,groupId=INT,launchId=INT,talkback=BOOLEAN,subs_price_bonus=DOUBLE,subs_price_packages=DOUBLE,subs_price_ext_shop=DOUBLE,subs_price_total=DOUBLE,with_shop_flg=BOOLEAN,abp=INT,has_cv=BOOLEAN,phone_request_flg=BOOLEAN,valid_until=INT,rating=DOUBLE,reviews_cnt=INT,review_id=INT,ssv=INT,user_auth_suggestionshown=BOOLEAN,s_trg=INT,user_authmemo_id=INT,is_linked=BOOLEAN,cnt_default_config=INT,cnt_default_items=INT,default_vas_amount=INT,item_version=INT,is_adblock=BOOLEAN,is_phone_new=BOOLEAN,additional_item=INT,autoload_operation_activate=BOOLEAN,autoload_operation_add=BOOLEAN,autoload_operation_fee=BOOLEAN,autoload_operation_remove=BOOLEAN,autoload_operation_stop=BOOLEAN,autoload_operation_update=BOOLEAN,autoload_operation_vas=BOOLEAN,is_refresh=BOOLEAN,item_in_moderation=BOOLEAN,call_action_id=INT,order_cancel_cause=INT,is_verified=BOOLEAN,other_phones_number=INT,item_number=INT,is_reverified=BOOLEAN,wrong_phone=BOOLEAN,review_seq_id=INT,subs_tariff_id=INT,subs_proposed_bonus_factor=DOUBLE,dnotification_message_id=INT,dnotification_delivered=BOOLEAN,isbp=BOOLEAN,cnt_favourites=INT,ces_score=INT,pvas_group_old=INT,pvas_group=INT,page_view_session_time=INT,ctcallid=INT,is_geo_delivery_widget=BOOLEAN,ticket_comment_id=INT,ticket_comment_dt=INT,src_split_ticket_id=INT,trgt_split_ticket_id=INT,time_to_first_byte=INT,time_to_first_paint=INT,time_to_first_interactive=INT,time_to_dom_ready=INT,time_to_on_load=INT,time_to_target_content=INT,time_for_async_target_content=INT,changed_microcat_id=INT,cv_use_category=BOOLEAN,cv_use_title=BOOLEAN,offerId=INT,cityId=INT,regionId=INT,classifiedId=INT,feedId=INT,clientId=INT,clientNeedId=INT,lifeTime=INT,leadId=INT,memberId=INT,executiveMemberId=INT,optionsNumber=INT,second=DOUBLE,is_original_user_report=BOOLEAN,autoteka_user_type=INT,list_load_number=INT,ssid=INT,parent_uid=INT,accounts_number=INT,autosort_images=BOOLEAN,cnt_subscribers=INT,is_oasis=BOOLEAN,xl=BOOLEAN,oneclickpayment=BOOLEAN,call_reason=INT,status_code=INT,api_dt=INT,report_duration=INT,report_status=BOOLEAN,page_number=INT,on_avito_since=INT,ssfid=INT,empl_id=INT,shop_moderation_action_id=INT,cur_i_status_id=INT,cur_i_status_time=INT,item_version_time=INT,checkbox_an_enable=BOOLEAN,close_timeout=INT,rec_item_id=INT,lfpackage_id=INT,subscription_promo=BOOLEAN,sub_is_ss=BOOLEAN,mobile_event_duration=INT,mobile_app_page_number=INT,reload=BOOLEAN,img_download_status=BOOLEAN,sgt_cat_flag=BOOLEAN,adpartner=INT,ticket_channel=INT,statid=INT,is_payed_immediately=BOOLEAN,subscription_edit=BOOLEAN,channel_number=INT,channels_screen_count=INT,item_position=INT,banner_due_date=INT,banner_show_days=BOOLEAN,banner_item_id=INT,has_messages=BOOLEAN,domofond_user_id=INT,is_ctrl_pressed=BOOLEAN,is_alt_pressed=BOOLEAN,is_shift_pressed=BOOLEAN,is_checkbox_checked=BOOLEAN,page_x_coord=INT,page_y_coord=INT,last_show_dt=INT,uploaded_files_cnt=INT,flow_type=INT,ces_article=INT,items_locked_count=INT,items_not_locked_count=INT,word_sgt_clicks=INT,moderation_user_score=DOUBLE,domofond_search_position=INT,domofond_saved_search_result=BOOLEAN,domofond_favorites_search_result=BOOLEAN,previous_contacts_cnt=INT,push_allowed=BOOLEAN,msg_chat_list_offset=INT,ces_hd=INT,deprecated=BOOLEAN,msg_blacklist_reason_id=INT,selected_did=INT,selected_metro=INT,selected_road=INT,was_interaction=INT,msg_internet_connection=BOOLEAN,msg_socket_type=INT,msg_is_push=BOOLEAN,msg_spam_confirm=BOOLEAN,click_position=INT,actual_time=DOUBLE,event_no=INT,skill_id=INT,cvpackage_id=INT,safedeal_orderid=INT,msg_search_success=BOOLEAN,msg_chat_page_num=INT,option_number=INT,short_term_rent=BOOLEAN,issfdl=BOOLEAN,helpdesk_call_id=INT,helpdesk_user_id=INT,target_admuser_id=INT,review_score=INT,stage=INT,satisfaction_score=INT,satisfaction_score_reason=INT,subs_vertical_id=INT,shop_on_moderation=BOOLEAN,parameter_value_id=INT,query_length=INT,new_category_id=INT,courier_survey_price=INT,msg_reason_id=INT,answer_seq_id=INT,autoteka_report_price=INT,is_paid=BOOLEAN,is_from_avito=BOOLEAN,autoteka_order_id=INT,autoteka_report_id=INT,autoteka_use_package=BOOLEAN,phone_id=INT,performance_timing_redirect_start=DOUBLE,performance_timing_redirect_end=DOUBLE,performance_timing_fetch_start=DOUBLE,performance_timing_domain_lookup_start=DOUBLE,performance_timing_domain_lookup_end=DOUBLE,performance_timing_connect_start=DOUBLE,performance_timing_secure_connection_start=DOUBLE,performance_timing_connect_end=DOUBLE,performance_timing_request_start=DOUBLE,performance_timing_response_start=DOUBLE,performance_timing_response_end=DOUBLE,performance_timing_first_paint=DOUBLE,performance_timing_first_contentful_paint=DOUBLE,performance_timing_dom_interactive=DOUBLE,performance_timing_dom_content_loaded_event_start=DOUBLE,performance_timing_dom_content_loaded_event_end=DOUBLE,performance_timing_dom_complete=DOUBLE,performance_timing_load_event_start=DOUBLE,performance_timing_load_event_end=DOUBLE,msg_search_over_limit=BOOLEAN,screen_width=INT,screen_height=INT,is_new_tab=BOOLEAN,autoload_distance_to_city=INT,autoload_item_id=INT,autoload_vertical_id=INT,item_snapshot_time=DOUBLE,is_rotation=BOOLEAN,cpa_abuse_id=INT,ad_error=INT,req_num=INT,is_from_ab_test=BOOLEAN,upp_provider_id=INT,upp_call_eventtype=INT,upp_call_event_time=INT,upp_call_is_blocked=BOOLEAN,upp_call_duration=INT,upp_talk_duration=INT,upp_call_accepted_at=INT,upp_call_ended_at=INT,upp_record=BOOLEAN,save_type=INT,phone_show_result=INT,autoload_total_items_cnt=INT,autoload_new_items_cnt=INT,autoload_updated_items_cnt=INT,autoload_reactivated_items_cnt=INT,autoload_deleted_items_cnt=INT,autoload_unchanged_items_cnt=INT,autoload_error_items_cnt=INT,is_cached=BOOLEAN,is_external=BOOLEAN,service_status=BOOLEAN,upp_status=INT,upp_status_date=INT,checkbox_metro_ring=BOOLEAN,checkbox_metro_ring_in=BOOLEAN,status_date=INT,upp_setting_id=INT,an_setting_id=INT,courier_orderid=INT,iscourier=BOOLEAN,call_status_id=INT,calltracking_activated=BOOLEAN,collapse_group_id=INT,valuable_item_id=INT,__x_eid=INT,__x_version=INT,__x_src_id=INT,__x_dtm=DOUBLE,__x_cid=INT,__x_lid=INT,__x_mcid=INT,__x_app=INT,__x_offset=INT,__x_pmax=INT,__x_pmin=INT,__x_position=INT,__x_show_position=INT,__x_iid=INT,click_from_block=INT,avitopro_search_on_avito_from=INT,avitopro_search_on_avito_to=INT,avitopro_search_price_from=INT,avitopro_search_price_to=INT,events_count=INT,text_scale=DOUBLE,platform=INT,answer_id=INT,map_zoom=INT,voxim_auth=BOOLEAN,call_side=INT,call_rating=INT,mic_access=BOOLEAN,appcall_eventtype=INT,caller_id=INT,reciever_id=INT,bundle_type=INT,config_update=BOOLEAN,last_config_update=DOUBLE,app_start_earliest=DOUBLE,avitopro_search_isvertical=BOOLEAN,market_price=INT,filter_block=BOOLEAN,max_range=INT,min_range=INT,voxim_quality=INT,appcall_callduration=INT,appcall_talkduration=INT,apprater_score=INT,appcall_start=INT,is_core_content=BOOLEAN,ntp_timestamp=DOUBLE,sold_on_avito_feature=BOOLEAN,ct_status=BOOLEAN,is_control_sample=BOOLEAN,upp_is_abc=BOOLEAN,order_cancel_cause_info_details_id=INT,performance_user_centric_interactive=DOUBLE,performance_user_centric_first_input_delay=DOUBLE,infm_clf_id=INT,search_features=INT,laas_answer=INT,laas_tooltip_answer=INT,autoactivation=BOOLEAN,image_load_time_delta=DOUBLE,msg_email_again=BOOLEAN,msg_copy_link_checker=BOOLEAN,performance_user_centric_total_blocking_time=DOUBLE,image_status=BOOLEAN,ice_breakers_id=INT,is_good_teaser=BOOLEAN,au_campaign_id=INT,story_id=INT,bytes_consumed=INT,bytes_remained=INT,uptime=INT,uppmon_actiondate=INT,cb_result=INT,uppmon_offered_score=INT,push_sent_time=INT,vox_push_ttl=INT,vox_connected=BOOLEAN,antifraud_call_datetime=INT,antifraud_call_result=INT,antifraud_call_duration=INT,antifraud_call_hangupcause=INT,item_quality_score=DOUBLE,use_suggest_item_add=BOOLEAN,report_application_cnt=INT",
                             "timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua),err,ipg,new,post,v,bot,amqp,app,clos,oc,bt,ArrayBool_param,int_param,lid,rcid,cid,offset,limit,total,shops,oid,mcid,aid,buid,iid,fid,st,location_id,item_id,categoryid,hide_phone,userType,manager_id,sid,snid,price,photos,nomail,timestamp,user_id,puid,ignored,inn,kpp,addrcp,success,source,machine,survey_id,lupint,quick,status_id,east,srid,siid,soid,typeid,cdt,position,diid,dvas_applied,items,news,notice,reminders,android_data_size_total,android_data_size_cache,android_data_size_databases,android_data_size_files,tid,android_data_size_shared_prefs,android_data_size_lib,life_time,android_data_size_app_webview,android_data_size_no_backup,android_data_size_code_cache,ida,ops,similars,cp,public_stats,srcId,shopId,subscribe,cpm_floor,vid,model,iin,reduce,spid,blockcode,bid,fnum,lf_cnt,orderid,ctid,img,complete,pctr,tariff,lflid,additional_user,msgid,sendtime,vol,icnt,pmax,pmin,amnt,lo,lf_dt,lf_grp,ie,cost,dirty,redirect,ts,serviceid,at,pid,shortcut,id,lup,p,version,cprice,packageid,ex,catid,rid,dpid,hctr,microcategoryid,sort,icu,isa,itl,hour,levid,uloggrd,delete,repeat,copy,area,lgid,floor,result_state,landscape,portrait,is_company,inca,suid,message_id,screen_code,lf_cost,last_contact,sat,operationid,paysysid,step,vasid,lastpaysysid,onmap,wmap,sgtd,searchid,ownid,iafi,qs,subscriptionid,budget,social_id,userid,usert,vasfactid,ctotalprice,error,ofid,ulogged,wcid,wmid,finish,start,google_play_services_library_version_code,locationid,paystat,emee_id,emey_id,souid,ibofp,ise,clicks,expenses,imprs,lines,size,dupstatus,google_play_services_version_code,simulator,dupid,pcid,cpid,auid,subjid,abuseid,tsid,google_play_services_compatibility,assistive_touch_running,ac,pc,rt,is_official_nd,df_start_time,df_expire_time,zk,bold_text_enabled,isupgrade,tmid,dmessage_id,ap,response_id,expired,result_status,result_time,answer_points,survey_version,is_test,accessibility_talkback,screen_brightness,screen_dpi,screen_height_dp,screen_width_dp,count,app_height_dp,dlvritem,offdelivery,level,pillarid,isb2c,isdlvr,extt,accessibility_haptic,accessibility_is_on,accessibility_visual,app_width_dp,configured_default_mail_client,darker_system_colors,disk_capacity,energy_efficiency,force_touch,free_disk_space,grayscale,guided_access,invert_colors,jailbreak,low_power_mode,memory_warning_count,reduce_motion,reduce_transparency,resources_size,shake_to_undo,speak_screen,speak_selection,startup_time,switch_control,voice_over,watch_pair,fprice,duration,tab,avatar_id,moderator_id,ever,show,mpact,is_seller,op_dt,ns_value,is_user_auth,lf_lim,dev_diid,duid,push_opened,dpush_delivered,stid,active_icnt,closed_icnt,im,avatar_exist,sgtp,is_legit,emotion_id,region_id,is_geo,radius,geo_lat,geo_lng,geo_acc,geo_time,bspage,is_antihack_phone_confirm,antihack_confirm_number,antihack_user_password_reset,str_available,create_time,update_time,iscredit,cperiod,geo_acc_code,is_delivery,width,height,admuserid,dur,premoderation,comparison_id,duplicate_dur,fsc,target_id,afraud_version,is_first_message,timing,timing_duplicate,admuser_id,connection_speed,screen_id,num_p,dl,is_verified_inn,notification,android_data_size_apk,target_type,cdtm,pmid,base_item_id,from_block,from_position,screen_items_viewed,screen_items_ad_viewed,screen_items_vip_viewed,screen_items_xl_viewed,screen_items_common_viewed,screen_items_new_viewed,screen_items_geo_viewed,screen_items_rec_viewed,isp,block_total,page_total,block_items_display,block_items_added,is_anon,retry,browser_web_push_agreement,is_auth,vasid_prev,tooltip_id,resultTime,devDiid,sm,answerPoints,isOfficialNd,ach,premiums,dfStartTime,peid,resultStatus,dmessageId,surveyVersion,pdrid,dfExpireTime,usedSuggest,isTest,platformId,ppeid,surv_id,img_id,vips,d,surveyId,uas,groupId,launchId,talkback,subs_price_bonus,subs_price_packages,subs_price_ext_shop,subs_price_total,with_shop_flg,abp,has_cv,phone_request_flg,valid_until,rating,reviews_cnt,review_id,ssv,user_auth_suggestionshown,s_trg,user_authmemo_id,is_linked,cnt_default_config,cnt_default_items,default_vas_amount,item_version,is_adblock,is_phone_new,additional_item,autoload_operation_activate,autoload_operation_add,autoload_operation_fee,autoload_operation_remove,autoload_operation_stop,autoload_operation_update,autoload_operation_vas,is_refresh,item_in_moderation,call_action_id,order_cancel_cause,is_verified,other_phones_number,item_number,is_reverified,wrong_phone,review_seq_id,subs_tariff_id,subs_proposed_bonus_factor,dnotification_message_id,dnotification_delivered,isbp,cnt_favourites,ces_score,pvas_group_old,pvas_group,page_view_session_time,ctcallid,is_geo_delivery_widget,ticket_comment_id,ticket_comment_dt,src_split_ticket_id,trgt_split_ticket_id,time_to_first_byte,time_to_first_paint,time_to_first_interactive,time_to_dom_ready,time_to_on_load,time_to_target_content,time_for_async_target_content,changed_microcat_id,cv_use_category,cv_use_title,offerId,cityId,regionId,classifiedId,feedId,clientId,clientNeedId,lifeTime,leadId,memberId,executiveMemberId,optionsNumber,second,is_original_user_report,autoteka_user_type,list_load_number,ssid,parent_uid,accounts_number,autosort_images,cnt_subscribers,is_oasis,xl,oneclickpayment,call_reason,status_code,api_dt,report_duration,report_status,page_number,on_avito_since,ssfid,empl_id,shop_moderation_action_id,cur_i_status_id,cur_i_status_time,item_version_time,checkbox_an_enable,close_timeout,rec_item_id,lfpackage_id,subscription_promo,sub_is_ss,mobile_event_duration,mobile_app_page_number,reload,img_download_status,sgt_cat_flag,adpartner,ticket_channel,statid,is_payed_immediately,subscription_edit,channel_number,channels_screen_count,item_position,banner_due_date,banner_show_days,banner_item_id,has_messages,domofond_user_id,is_ctrl_pressed,is_alt_pressed,is_shift_pressed,is_checkbox_checked,page_x_coord,page_y_coord,last_show_dt,uploaded_files_cnt,flow_type,ces_article,items_locked_count,items_not_locked_count,word_sgt_clicks,moderation_user_score,domofond_search_position,domofond_saved_search_result,domofond_favorites_search_result,previous_contacts_cnt,push_allowed,msg_chat_list_offset,ces_hd,deprecated,msg_blacklist_reason_id,selected_did,selected_metro,selected_road,was_interaction,msg_internet_connection,msg_socket_type,msg_is_push,msg_spam_confirm,click_position,actual_time,event_no,skill_id,cvpackage_id,safedeal_orderid,msg_search_success,msg_chat_page_num,option_number,short_term_rent,issfdl,helpdesk_call_id,helpdesk_user_id,target_admuser_id,review_score,stage,satisfaction_score,satisfaction_score_reason,subs_vertical_id,shop_on_moderation,parameter_value_id,query_length,new_category_id,courier_survey_price,msg_reason_id,answer_seq_id,autoteka_report_price,is_paid,is_from_avito,autoteka_order_id,autoteka_report_id,autoteka_use_package,phone_id,performance_timing_redirect_start,performance_timing_redirect_end,performance_timing_fetch_start,performance_timing_domain_lookup_start,performance_timing_domain_lookup_end,performance_timing_connect_start,performance_timing_secure_connection_start,performance_timing_connect_end,performance_timing_request_start,performance_timing_response_start,performance_timing_response_end,performance_timing_first_paint,performance_timing_first_contentful_paint,performance_timing_dom_interactive,performance_timing_dom_content_loaded_event_start,performance_timing_dom_content_loaded_event_end,performance_timing_dom_complete,performance_timing_load_event_start,performance_timing_load_event_end,msg_search_over_limit,screen_width,screen_height,is_new_tab,autoload_distance_to_city,autoload_item_id,autoload_vertical_id,item_snapshot_time,is_rotation,cpa_abuse_id,ad_error,req_num,is_from_ab_test,upp_provider_id,upp_call_eventtype,upp_call_event_time,upp_call_is_blocked,upp_call_duration,upp_talk_duration,upp_call_accepted_at,upp_call_ended_at,upp_record,save_type,phone_show_result,autoload_total_items_cnt,autoload_new_items_cnt,autoload_updated_items_cnt,autoload_reactivated_items_cnt,autoload_deleted_items_cnt,autoload_unchanged_items_cnt,autoload_error_items_cnt,is_cached,is_external,service_status,upp_status,upp_status_date,checkbox_metro_ring,checkbox_metro_ring_in,status_date,upp_setting_id,an_setting_id,courier_orderid,iscourier,call_status_id,calltracking_activated,collapse_group_id,valuable_item_id,__x_eid,__x_version,__x_src_id,__x_dtm,__x_cid,__x_lid,__x_mcid,__x_app,__x_offset,__x_pmax,__x_pmin,__x_position,__x_show_position,__x_iid,click_from_block,avitopro_search_on_avito_from,avitopro_search_on_avito_to,avitopro_search_price_from,avitopro_search_price_to,events_count,text_scale,platform,answer_id,map_zoom,voxim_auth,call_side,call_rating,mic_access,appcall_eventtype,caller_id,reciever_id,bundle_type,config_update,last_config_update,app_start_earliest,avitopro_search_isvertical,market_price,filter_block,max_range,min_range,voxim_quality,appcall_callduration,appcall_talkduration,apprater_score,appcall_start,is_core_content,ntp_timestamp,sold_on_avito_feature,ct_status,is_control_sample,upp_is_abc,order_cancel_cause_info_details_id,performance_user_centric_interactive,performance_user_centric_first_input_delay,infm_clf_id,search_features,laas_answer,laas_tooltip_answer,autoactivation,image_load_time_delta,msg_email_again,msg_copy_link_checker,performance_user_centric_total_blocking_time,image_status,ice_breakers_id,is_good_teaser,au_campaign_id,story_id,bytes_consumed,bytes_remained,uptime,uppmon_actiondate,cb_result,uppmon_offered_score,push_sent_time,vox_push_ttl,vox_connected,antifraud_call_datetime,antifraud_call_result,antifraud_call_duration,antifraud_call_hangupcause,item_quality_score,use_suggest_item_add,report_application_cnt",
                             "|", "\n");
                             //"\1", "\2");

    KafkaSource src("avi-anpl09:9092,avi-anpl10:9092,avi-anpl11:9092",
                    "user-keyed-clickstream",
                    //"1:-2:100000",
                    "%:-1000:5000000",
                    //"test-omnisci-local",
                    "test-omnisci",
                    sink);
    src.setup();
    src.process();
    src.destroy();
    return 0;
}
