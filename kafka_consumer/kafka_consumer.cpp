
//#include "Vertica.h"
#include "vhash.h"

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
#define NOTINT INT_MAX

//using namespace Vertica;
using namespace std;
using namespace cppkafka;

typedef unsigned char  uint8;
typedef unsigned long uint32;
typedef signed int     int32;

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

int64 toInt(std::string s, int64 defultValue = NOTINT)
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


class Sink
{
public:
    virtual void put(Message &doc) = 0;
    virtual void flush() = 0;
};


// ARES

/*
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

*/

// ORC


#include <orc/Writer.hh>
#include <orc/Type.hh>
#include <orc/OrcFile.hh>

using namespace orc;


class ORCSink : public Sink
{
    ORC_UNIQUE_PTR<OutputStream> outStream;
    ORC_UNIQUE_PTR<Type> schema;
    ORC_UNIQUE_PTR<Writer> writer;

    uint64_t batchSize;
    uint64_t rows;
    ORC_UNIQUE_PTR<ColumnVectorBatch> batch;
    StructVectorBatch *root;

    DataBuffer<char> buffer;
    uint64_t offset;

    tsl::hopscotch_map<std::string, int, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, int> >, 30, true, tsl::power_of_two_growth_policy> fieldIndex;

    std::vector<std::string> fieldName;
    std::vector<int> fieldType;

public:

    ORCSink(std::string columnTypeFmt) :
        batchSize(1024), rows(0),
        buffer(*orc::getDefaultPool(), 350 * 100 * 1024), offset(0)
    {
        outStream = writeLocalFile("my-file.orc");
        schema = ORC_UNIQUE_PTR<Type>(Type::buildTypeFromString("struct<" + columnTypeFmt + ">"));
        WriterOptions options;
        writer = createWriter(*schema, outStream.get(), options);

        batch = writer->createRowBatch(batchSize);
        root = dynamic_cast<StructVectorBatch *>(batch.get());

        for (size_t j = 0; j < schema->getSubtypeCount(); ++j)
        {
            std::string key = schema->getFieldName(j);
            fieldIndex[key] = fieldName.size();
            fieldName.push_back(key);
            fieldType.push_back(schema->getSubtype(j)->getKind());
        }
    }

    void put(Message &doc)
    {
        int totalSize = 0;
        const int fieldCount = fieldName.size();
        std::vector<std::pair<const char*, int> > values(fieldCount, make_pair("", 0));

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

                auto found = fieldIndex.find(key);
                if (found != fieldIndex.end())
                {
                    int ind = found->second;

                    char * s = (char*)src + t[i + 1].start;
                    int size = t[i + 1].end - t[i + 1].start;

                    if (t[i + 1].type == JSMN_STRING)
                    {
                        size = unescape(s, size);
                    }
                    if (size == 4 && strncmp(s, "null", 4) == 0)
                    {
                        size = 0;
                    }
                    if (size == 0)
                    {
                        continue;
                    }
                    values[ind] = make_pair(s, size);
                    totalSize += size;
                }
            }
        }

        // fill columns
        for (auto i = 0; i < values.size(); ++i)
        {
            ColumnVectorBatch *batch = root->fields[i];
            if (values[i].second == 0)
            {
                batch->notNull[rows] = 0;
                batch->hasNulls = true;
                continue;
            }
            batch->notNull[rows] = 1;

            if (fieldType[i] == orc::LONG)
            {
                LongVectorBatch *c = (LongVectorBatch *)(batch);
                string v(values[i].first, values[i].second);
                c->data[rows] = strtol(v.c_str(), 0, 10);
            }
            else if (fieldType[i] == orc::VARCHAR)
            {
                StringVectorBatch *stringBatch = (StringVectorBatch *)(batch);
                preserve(values[i].second, stringBatch);
                memcpy(buffer.data() + offset, values[i].first, values[i].second);

                stringBatch->data[rows] = buffer.data() + offset;
                stringBatch->length[rows] = static_cast<int64_t>(values[i].second);
                offset += values[i].second;
            }
            else if (fieldType[i] == orc::DOUBLE)
            {
                DoubleVectorBatch *c = (DoubleVectorBatch *)(batch);
                string v(values[i].first, values[i].second);
                c->data[rows] = strtod(v.c_str(), 0);
            }
            else if (fieldType[i] == orc::BOOLEAN)
            {
                LongVectorBatch *c = (LongVectorBatch*)(batch);
                c->data[rows] = (strncmp(values[i].first, "t", 1) == 0);
            }
            else
            {
                batch->notNull[rows] = 0;
                batch->hasNulls = true;
            }
        }

        rows++;
        if (rows == batchSize)
        {
            writeBatch();
        }
    }

    void flush()
    {
        if (rows != 0)
        {
            writeBatch();
        }
        writer->close();
    }

private:

    void writeBatch()
    {
        root->numElements = rows;
        for (size_t i = 0; i < fieldName.size(); ++ i)
        {
            ColumnVectorBatch *batch = root->fields[i];
            batch->numElements = rows;
        }
        writer->add(*batch);
        for (size_t i = 0; i < fieldName.size(); ++ i)
        {
            ColumnVectorBatch *batch = root->fields[i];
            batch->hasNulls = false;
        }
        rows = 0;
    }

    void preserve(size_t valueSize, StringVectorBatch *stringBatch)
    {
        // Resize the buffer in case buffer does not have remaining space to store the next string.
        char* oldBufferAddress = buffer.data();
        while (buffer.size() - offset < valueSize)
        {
            buffer.resize(buffer.size() * 2);
        }
        char* newBufferAddress = buffer.data();

        // Refill stringBatch->data with the new addresses, if buffer's address has changed.
        if (newBufferAddress != oldBufferAddress)
        {
            for (uint64_t refillIndex = 0; refillIndex < rows; ++refillIndex)
            {
                stringBatch->data[refillIndex] = stringBatch->data[refillIndex] - oldBufferAddress + newBufferAddress;
            }
        }
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

    std::vector<std::string> fieldName;
    tsl::hopscotch_map<std::string, int, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, int> >, 30, true, tsl::power_of_two_growth_policy> fieldIndex;

    int fieldGroupCount;
    std::vector<int> fieldGroup;
    std::vector<int> groupFieldCount;
    std::vector<int> fieldFunction;

    std::vector<int> fieldReorderDirect;
    std::vector<int> fieldReorderReverse;
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

            std::string group = keyParts.size() > 1 ? keyParts[1] : (defaultGroupping ? "" : key);
            auto it = groupFields.find(group);
            if (it == groupFields.end())
            {
                it = groupFields.insert(std::make_pair(group, std::vector<std::pair<std::string,int> >())).first;
                groups.push_back(group);
            }
            it->second.push_back(std::make_pair(key, transform));

            fieldIndex[key] = j;
        }

        fieldReorderDirect = std::vector<int>(keys.size(), -1);
        fieldReorderReverse = std::vector<int>(keys.size(), -1);

        for (size_t k = 0; k < groups.size(); ++k)
        {
            std::vector<std::pair<std::string,int> > fields = groupFields[groups[k]];
            for (size_t f = 0; f < fields.size(); ++f)
            {
                fieldReorderDirect[fieldIndex[fields[f].first]] = fieldName.size();
                fieldReorderReverse[fieldName.size()] = fieldIndex[fields[f].first];

                fieldIndex[fields[f].first] = fieldName.size();
                fieldName.push_back(fields[f].first);
                fieldFunction.push_back(fields[f].second);
                fieldGroup.push_back(k);
            }
            groupFieldCount.push_back(fields.size());
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
        else if (formatType == OUTPUT_COLUMNS)
        {
            return toSparseRecord(doc);
        }
        else if (formatType == OUTPUT_EAV)
        {
            return toEAVRecord(doc);
        }
        else if (formatType == OUTPUT_ARRAY)
        {
            return toArrayRecordComp(doc);
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
        const int fieldCount = fieldName.size();
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

                auto found = fieldIndex.find(key);
                if (found != fieldIndex.end())
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
                        char * s = (char*)src + t[i + 1].start;
                        int size = t[i + 1].end - t[i + 1].start;

//                        if (t[i + 1].type == JSMN_STRING)
//                        {
//                            size = unescape(s, size);
//                        }
                        if (size == 4 && strncmp(s, "null", 4) == 0)
                        {
                            size = 0;
                        }
                        if (size == 0)
                        {
                            continue;
                        }
                        values[ind] = make_pair(s, size);
                        totalSize += size;
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

        int group = -1;
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (fieldGroup[i] != group)
            {
                if (groupFieldCount[group] > 1)
                {
                    buf[shift - 1] = '}';
                    if (defaultDelim != csvDelim)
                    {
                        buf[shift] = csvDelim;
                    }
                    shift += 1;
                }

                if (groupFieldCount[fieldGroup[i]] > 1)
                {
                    buf[shift] = '{';
                    shift += 1;
                }
                group = fieldGroup[i];
            }


            if (values[i].second > 0 && values[i].second + (fieldCount - i + group * 2) < sizeMax)
            {
                memcpy(buf + shift, values[i].first, values[i].second);
                shift += values[i].second;
            }

            if (defaultDelim != arrayDelim && groupFieldCount[group] > 1)
            {
                buf[shift] = arrayDelim;
            }
            else if (defaultDelim != csvDelim && groupFieldCount[group] <= 1)
            {
                buf[shift] = csvDelim;
            }
            shift += 1;
        }
        if (groupFieldCount[group] > 1)
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
        static const int BUF_SIZE = 65000;
        static char buf[BUF_SIZE];
        int bufOffset = 0;
        const char delim = delimiter[0];
        const int fieldCount = fieldName.size();

        const char *src = (const char *)doc.get_payload().get_data();
        int len = doc.get_payload().get_size();

        std::string header(std::move(toRecordHeader(doc)));

        // rough size check
        int rowSize = header.size() + (2 * fieldCount) + (fieldGroupCount + len) + 1;
        if (rowSize > BUF_SIZE)
        {
            return std::string();
        }

        // parse json
        std::vector<std::pair<const char*, int> > values(fieldCount, make_pair("", 0));

        std::vector<int> presentIndexes;
        presentIndexes.reserve(fieldCount);

        jsmn_parser p;
        jsmntok_t t[4098];

        jsmn_init(&p);
        int r = jsmn_parse(&p, src, len, t, 4098);
        for (int i = 1; i < r - 1; ++i)
        {
            if (t[i].type == JSMN_STRING && t[i].parent == 0 && t[i + 1].parent == i)
            {
                std::string key(src + t[i].start, t[i].end - t[i].start);

                auto found = fieldIndex.find(key);
                if (found != fieldIndex.end())
                {
                    int ind = found->second & 0xffff;

                    char * s = (char*)src + t[i + 1].start;
                    int size = t[i + 1].end - t[i + 1].start;

//                    if (t[i + 1].type == JSMN_STRING)
//                    {
//                        size = unescape(s, size);
//                    }
                    if (size == 4 && strncmp(s, "null", 4) == 0)
                    {
                        size = 0;
                    }
                    if (size == 0)
                    {
                        continue;
                    }
                    values[ind] = make_pair(s, size);

                    presentIndexes.push_back(ind);
                    if (ind + 1 < (int)values.size() && fieldGroup[ind] == fieldGroup[ind + 1])
                    {
                        presentIndexes.push_back(ind + 1);
                    }
                }
            }
        }
        std::sort(presentIndexes.begin(), presentIndexes.end());
        presentIndexes.erase(std::unique(presentIndexes.begin(), presentIndexes.end()), presentIndexes.end());

        // format header
        memcpy(buf + bufOffset, header.data(), header.size());
        bufOffset += header.size();

        // format index
        int indexCapacity = int(values.size());
        memset(buf + bufOffset, '0', indexCapacity * 2);

        int valueSizeTotal = 0;
        std::vector<int> groupSize(fieldGroupCount, 0);
        for (int ind : presentIndexes)
        {
            int group = fieldGroup[ind];
            int size = values[ind].second;

            int offsetAligned = align256(groupSize[group], size, 128);
            valueSizeTotal += offsetAligned + size - groupSize[group];
            intToHex(encode256(offsetAligned, 128), buf + bufOffset + fieldReorderReverse[ind] * 2, 1);
            groupSize[group] = offsetAligned + size;
        }
        bufOffset += indexCapacity * 2;
        buf[bufOffset++] = delim;

        // format value groups
        memset(buf + bufOffset, ' ', valueSizeTotal + fieldGroupCount);

        int currentGroup = 0;
        int groupOffset = 0;
        for (int i : presentIndexes)
        {
            if (fieldGroup[i] != currentGroup)
            {
                bufOffset += groupOffset;
                groupOffset = 0;

                memset(buf + bufOffset, delim, fieldGroup[i] - currentGroup);
                bufOffset += fieldGroup[i] - currentGroup;
                currentGroup = fieldGroup[i];
            }

            int size = values[i].second;
            if (size == 0)
            {
                continue;
            }

            int offset = groupOffset;
            int offsetAligned = align256(offset, size, 128);

            memcpy(buf + bufOffset + offsetAligned, values[i].first, size);
            groupOffset = offsetAligned + size;
        }
        bufOffset += groupOffset;
        memset(buf + bufOffset, delim, fieldGroupCount - currentGroup);
        bufOffset += fieldGroupCount - currentGroup;

        buf[bufOffset++] = terminator[0];

        return std::string(buf, bufOffset);
    }

    std::string toJSONRecord(Message &doc)
    {
        std::string j(doc.get_payload());
        return toRecordHeader(doc) + j + terminator;
    }

    std::string toEAVRecord(Message &doc)
    {
        std::string line;
        std::string h = toRecordHeader(doc);

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
                std::string value(src + t[i + 1].start, t[i + 1].end - t[i + 1].start);
                line += h + key + delimiter + value + terminator;
            }
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
//    Sink *sink = new CSVSink("timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua)", "|", "\n");

//    KafkaSource src("clickstream-kafka01:9092",
//                    "user-keyed-clickstream",
//                    "*:-2:500000",
//                    "test-omnisci",
//                    sink);

//    Sink *sink = new CSVSink("partition;offset;timestamp;key;json", "|", "\n");

//    KafkaSource src("avi-lbus01:9092",
//                    "tableau",
//                    "*:-2:500000",
//                    "test-omnisci",
//                    sink);

//    Sink *sink = new CSVSink("timestamp;partition;offset;key;columns:src_id,err,ip,uid,ref,v,src,u,ab,bot,du,eid,geo,ua,url,app,dt,bt,email,bool_param,lid,cid,q,offset,limit,total,oid,mcid,aid,buid,iid,mid,fid,tx,st,uem,type,vin,params,item_id,categoryid,campaign_id,hide_phone,group_id,launch_id,phone,esid,sid,name,title,price,photos,photos_ids,user_id,engine,puid,msg,manager,addr,phone1,inn,addr2,addr1,addrcp,token,success,source,engine_version,quick,cmm,from,status_id,typeid,position,reason,items,tid,vsrc,imgid,x,cm,errors_detailed,shopId,prob,screen,vid,rules,lf_cnt,orderid,_ctid,cids,img,complete,tariff,lflid,additional_user,pagetype,sum,msgid,vol,icnt,s,did,lids,pmax,pmin,amnt,lf_dt,ie,city,serviceid,at,pid,shortcut,t,from_page,id,chid,version,lf_type,packageid,uids,ex,catid,dpid,listingfees,microcategoryid,sort,page,delete,f,operationid,paysysid,step,vasid,lastpaysysid,onmap,rooms,sgtd,device_id,subscriptionid,social_id,push_token,chatid,locationid,sortd,paystat,reschid,souid,dupid,pcid,banner_type,auid,vas_type,abuseid,tsid,notification_id,cb,cy,rt,status,isupgrade,tmid,sortf,ap,sids,answer_text,srcp,mcids,str,words,dlvritem,offdelivery,level,pillarid,isdlvr,extt,contact_type,free_disk_space,tab,action,dtm,abuseids,is_seller,ns_channel,ns_type,ns_value,is_user_auth,duid,user_key,im,sgtx,sgtp,is_legit,sgt,sgt_mcids,objectid,objecttype,extension,geo_lat,geo_lng,is_antihack_phone_confirm,antihack_confirm_number,antihack_user_password_reset,color,exif_model,exif_make,create_time,update_time,app_type,ver,width,height,admuserid,premoderation,fraud_code_ids,afraud_version,login_type,is_first_message,network_type,date,plate_number,engine_power,transport_type,transport_category,timing,timing_duplicate,int8erface_version,admuser_id,time_to_content,exception_id,duplicates,abs,safedeal,dl,subtype,is_verified_inn,srd,form_input_field_name,form_input_field_value,channels,channels_deny,target_type,pmid,datefrom,dateto,base_item_id,reject_wrong_params,from_block,from_position,service_id,amount,operation_id,isp,block_items_display,block_items_added,is_anon,search_area,retry,browser_web_push_agreement,is_auth,snippet_placement,vasid_prev,_ga,advtid,sm,ach,premiums,peid,pdrid,usedSuggest,ppeid,userToken,img_id,vips,d,uas,sr,groupId,launchId,unique_args,model_version,subs_price_bonus,subs_price_packages,subs_price_ext_shop,subs_price_total,with_shop_flg,has_cv,phone_request_flg,msg_image,valid_until,rating,reviews_cnt,review_id,share_place,user_auth_memo_id,call_id,is_linked,rdt,notification_message_id,adm_comment,item_version,is_adblock,helpdesk_agent_state,anonymous_number_service_responce,additional_item,call_action_type,order_cancel_cause,order_cancel_cause_txt,is_verified,other_phones_number,item_number,phone_action,is_reverified,phone_action_type,order_cancel_cause_info,review_seq_id,errorsDetailed,subs_vertical_ids,subs_tariff_id,subs_proposed_bonus_factor,subs_extensions,subs_packages,caller_user_type,message_preview,car_doors,engine_type,transmission,car_drive,steering_wheel,engine_volume,article_dissatisfaction_reason,redirect_incoming_call_to,placement,action_type,from_source,cnt_favourites,ces_score,pvas_group_old,pvas_group,pvas_groups,page_view_session_time,ctcallid,is_geo_delivery_widget,api_path,antihack_reason,buyer_booking_cancel_cause,ticket_comment_id,src_split_ticket_id,trgt_split_ticket_id,time_to_first_byte,time_to_first_paint8,time_to_first_int8eractive,time_to_dom_ready,time_to_on_load,time_to_target_content,time_for_async_target_content,changed_microcat_id,scenario,cv_use_category,cv_use_title,str_buyer_contact_result_reason,profile_tab,is_original_user_report,autoteka_user_type,list_load_number,ssid,recommendpaysysid,app_version,os_version,accounts_number,search_correction_action,search_correction_original,search_correction_corrected,search_correction_method,autosort_images,cnt_subscribers,is_oasis,pvas_dates,oneclickpayment,project,location_text_input,cadastralnumber,report_duration,report_status,page_number,deep_link,item_add_screen,user_ids,shop_fraud_reason_ids,shop_moderation_action_hash,checkbox_an_enable,message_type,app_version_code,banner_id,shortcut_description,close_timeout,selling_system,banner_code,wsrc,shops_array,subscription_promo,sub_is_ss,sub_prolong,target_page,mobile_event_duration,screen_name,content_type,mobile_app_page_number,roads,img_download_status,screen_start_time,sgt_cat_flag,ns_owner,software_version,build,adpartner,ticket_channel,adslot,statid,current_subs_version,new_subs_version,subscription_edit,channel_number,channels_screen_count,delivery_help_question,banner_due_date,banner_show_days,banner_item_id,chat_error_case,has_messages,search_address_type,js_event_type,dom_node,js_event_slug,attr_title,attr_link,attr_value,key_name,is_ctrl_pressed,is_alt_pressed,is_shift_pressed,color_theme,dom_node_content,is_checkbox_checked,page_x_coord,page_y_coord,srd_initial,uploaded_files_cnt,review_additional_info,flow_type,flow_id,ces_article,items_locked_count,items_not_locked_count,word_sgt_clicks,metro,msg_app_name,msg_request,moderation_user_score,msg_button_type,action_payload,msg_chat_list_offset,ces_hd,msg_throttling_reason,msg_app_version,RealtyDevelopment_id,metro_list,distance_list,district_list,block_uids,msg_blacklist_reason_id,roads_list,msg_random_id,msg_int8ernet_connection,msg_socket_type,msg_is_push,cities_list,uids_rec,email_hash,target_hash,click_position,phone_pdhash,caller_phone_pdhash,avitopro_date_preset,skill_id,safedeal_orderid,msg_search_query,msg_search_success,msg_chat_page_num,option_number,short_term_rent,issfdl,helpdesk_user_id,cv_suggest_show_type,review_score,stage,sgt_building_id,page_from,item_condition,span_end_time,custom_param,subs_vertical_id,shop_on_moderation,parameter_value_slug,parameter_value_id,query_length,new_category_id,api_method_name,courier_survey_reasons,courier_survey_reasons_comment,screen_touch_time,msg_reason_id,geo_session,inactive_page,location_suggest_text,answer_seq_id,new_param_ids,autoteka_cookie,landing_slug,autoteka_user_id,utm_source,utm_medium,utm_campaign,is_paid,is_from_avito,autoteka_order_id,autoteka_report_id,safedeal_services,performance_timing_redirect_start,performance_timing_redirect_end,performance_timing_fetch_start,performance_timing_domain_lookup_start,performance_timing_domain_lookup_end,performance_timing_connect_start,performance_timing_secure_connection_start,performance_timing_connect_end,performance_timing_request_start,performance_timing_response_start,performance_timing_response_end,performance_timing_first_paint8,performance_timing_first_contentful_paint8,performance_timing_dom_int8eractive,performance_timing_dom_content_loaded_event_start,performance_timing_dom_content_loaded_event_end,performance_timing_dom_complete,performance_timing_load_event_start,performance_timing_load_event_end,autoload_tags,autoload_not_empty_tags,screen_width,screen_height,is_new_tab,autoload_region,autoload_subway,autoload_street,autoload_district,autoload_direction_road,autoload_distance_to_city,autoload_item_id,tip_type,error_text,abuse_msg,cpa_abuse_id,call_status,alid,ad_error,req_num,app_startup_time,is_from_ab_test,upp_call_id,upp_provider_id,upp_virtual_phone,upp_incoming_phone,upp_client,upp_linked_phone,upp_allocate_id,upp_call_eventtype,upp_call_event_time,upp_call_is_blocked,upp_call_duration,upp_talk_duration,upp_call_accepted_at,upp_call_ended_at,upp_record_url,upp_record,upp_caller_message,upp_call_receiver_message,upp_transfer_result,form_validation_error_texts,sgt_item_type,landing_action,prof_profile_type,save_type,phone_show_result,perfvas_landing_from,autoload_total_items_cnt,autoload_new_items_cnt,autoload_updated_items_cnt,autoload_reactivated_items_cnt,autoload_deleted_items_cnt,autoload_unchanged_items_cnt,autoload_error_items_cnt,chain,new_chain,is_cached,form_validation_error_param_slugs,color_theme_status,service_status,upp_status,upp_status_date,checkbox_metro_ring,checkbox_metro_ring_in,status_date,upp_setting_id,an_setting_id,courier_orderid,iscourier,call_status_id,calltracking_activated,stack_trace,sgt_source_query,sgt_user_query,collapse_group_id,valuable_item_id,suggest_ad_id,click_uuid,click_from_block,avitopro_search_query,min_ts,max_ts,avitopro_search_sort_statistic_date_from,avitopro_search_sort_statistic_date_to,events_count,text_scale,courier_field_name,answer_id,courier_survey_reasons_position,upp_record_reason,upp_block_reason,content_size,map_zoom,voxim_auth,appcall_scenario,appcall_id,appcall_choice,call_side,call_rating,mic_access,appcall_eventtype,caller_id,reciever_id,appcall_result,bundle_type,icon_type,config_update,last_config_update,app_start_earliest,shield_type,avitopro_stat_int8_type,webouuid,market_price,max_range,min_range,voxim_quality,appcall_network,voxim_metric_type,appcall_callduration,appcall_talkduration,apprater_score,appcall_start,is_core_content,filter_name,filter_block_name,sold_on_avito_feature,is_control_sample,vas_promt_type,question,upp_is_abc,order_cancel_cause_info_details,order_cancel_cause_info_details_id,pin_type,pin_state,performance_user_centric_int8eractive,performance_user_centric_first_input_delay,search_suggest_serp,filter_id,infm_clf_id,infm_version,infm_clf_tree,msg_link_text,msg_copy_text,search_features,laas_tooltip_type,laas_tooltip_answer,project_id,scopes,generation,complectation,modification,x_sgt,app_ui_theme,app_ui_theme_setting_value,change_screen,filter_type,autoactivation,image_draw_time,image_load_time_delta,image_type,msg_email_again,msg_copy_link_checker,performance_user_centric_total_blocking_time,image_status,image_error,ice_breakers_id,ice_breakers_ids,is_good_teaser,event_landing_slug,story_id,story_ids,feedback_id,bytes_consumed,bytes_remained,top_screen,app_state,uptime,uppmon_actiondate,cb_task,cb_reason,cb_result,cb_result_full,uppmon_offered_score,push_sent_time,push_priority_sent,push_priority_recieved,vox_push_ttl,vox_connected,vox_push,vox_call_id,array_iids,vox_userid,antifraud_call_id,antifraud_call_datetime,antifraud_call_result,antifraud_call_result_code,antifraud_call_duration,antifraud_call_hangupcause,parameter_name,parameter_value,use_suggest_item_add,upp_project,report_application_cnt,tax_form,access,filter_specialoffers,special_offers_new_price,special_offers_change_default_discount,special_offers_sbc_to_userid,folder_name,tag_name,messenger_folder_onbording_action_name,msg_move_to_folder_from,probability,model_calculation_date,helpdesk_call_str_id,does_send_to_email,push_freq,reason_data,stories_number,feedback_type,moder_display_type,upp_provider_events_log,infmodel_version,infm_raw_params,infm_auto_catalog_params,multi_loc_cnt,is_multiple,old_price,local_priority,stories_title,appcall_system_notification,story_position,delivery_status,delivery_service,saved_history_search,is_marketplace,is_stock,upp_incoming_provider,upp_linked_provider,upp_incoming_phone_is_abc,auto_bundle_type,pin_price,laas_rule,marketplace_count,auto_bundle_feedback_variant,auto_bundle_feedback_text,mindbox_id,main_page_footer_link_text,suggest_l1_source,app_bucket,ctcall_spam_show,sessidhash,ctcall_spam_mark,quasi_map_answer,notification_provider,performance_user_centric_largest_contentful_paint8,autoprolongation_from,is_extended,vox_push_sending_status,vox_push_ios_error,vox_push_android_error,appcall_decline_feedback_variant,appcall_decline_feedback_text,appcall_decline_feedback_viewtime,autoteka_product_id,cpa_arb_reason_id,error_level,autoload_tag_name,autoload_package_id,autoload_content_type,autoload_upload_error_code,autoload_report_id,is_search_use,no_suitable_option,newdev_catalog_params,email_count,transition,step_id,transition_type,perfvas_landing_faq_number,badge_ids,badge_id,is_app,how_close_banner,is_new_device,autoload_item_action_type,broker_calc_data,broker_banks_data,message_contact_share_type,broker_session,broker_city,broker_send_point8,sav_serp_banner_action,phone_brand,model_id,question_id,id_answer,vas_type_queue_services,cpa_call_price,advance_bill_create_time,cpa_items_advance,cpa_calls_min,cpa_calls_avg,cpa_config_id,days_in_calculation_cpa_advance,coordinates_resolve_time,coordinates_resolved,is_dct_historical_allocation,bot_flow_version,prev_step_id,business_platform,survey_answer_job_id,survey_question_job_id,survey_answer_job_text,is_new_account,performance_user_centric_cumulative_layout_shift,developer_jk_view_src,developer_purchase_way,developer_phone_view_src,cart_stepper,item_draft_id,ios_network_error_type,ios_network_error_subtype,ios_network_error_text,is_single_item,captcha_score,score_captcha_reason", "|", "\n");

    Sink *sink = new ORCSink("src_id:bigint,err:bigint,ip:string,uid:bigint,ref:string,v:bigint,src:bigint,u:string,ab:string,bot:boolean,du:string,eid:bigint,geo:string,ua:string,url:string,app:bigint,dt:bigint,bt:bigint,email:string,bool_param:boolean,lid:bigint,cid:bigint,q:string,offset:bigint,limit:bigint,total:bigint,oid:bigint,mcid:bigint,aid:bigint,buid:bigint,iid:bigint,mid:string,fid:bigint,tx:string,st:bigint,uem:string,type:string,vin:string,params:string,item_id:bigint,categoryid:bigint,campaign_id:string,hide_phone:boolean,group_id:string,launch_id:string,phone:string,esid:string,sid:bigint,name:string,title:string,price:double,photos:bigint,photos_ids:string,user_id:bigint,engine:string,puid:bigint,msg:string,manager:string,addr:string,phone1:string,inn:bigint,addr2:string,addr1:string,addrcp:boolean,token:string,success:boolean,source:bigint,engine_version:string,quick:boolean,cmm:string,from:string,status_id:bigint,typeid:bigint,position:bigint,reason:string,items:bigint,tid:bigint,vsrc:string,imgid:bigint,x:string,cm:string,errors_detailed:string,shopId:bigint,prob:string,screen:string,vid:bigint,rules:string,lf_cnt:bigint,orderid:bigint,_ctid:bigint,cids:string,img:bigint,complete:bigint,tariff:bigint,lflid:bigint,additional_user:bigint,pagetype:string,sum:string,msgid:bigint,vol:bigint,icnt:bigint,s:string,did:string,lids:string,pmax:bigint,pmin:bigint,amnt:bigint,lf_dt:bigint,ie:boolean,city:string,serviceid:bigint,at:bigint,pid:bigint,shortcut:bigint,t:string,from_page:string,id:bigint,chid:string,version:bigint,lf_type:string,packageid:bigint,uids:string,ex:bigint,catid:bigint,dpid:bigint,listingfees:string,microcategoryid:bigint,sort:bigint,page:string,delete:boolean,f:string,operationid:bigint,paysysid:bigint,step:bigint,vasid:bigint,lastpaysysid:bigint,onmap:bigint,rooms:string,sgtd:bigint,device_id:string,subscriptionid:bigint,social_id:bigint,push_token:string,chatid:string,locationid:bigint,sortd:string,paystat:bigint,reschid:string,souid:bigint,dupid:bigint,pcid:bigint,banner_type:string,auid:bigint,vas_type:string,abuseid:bigint,tsid:bigint,notification_id:string,cb:string,cy:string,rt:bigint,status:string,isupgrade:boolean,tmid:bigint,sortf:string,ap:boolean,sids:string,answer_text:string,srcp:string,mcids:string,str:string,words:string,dlvritem:bigint,offdelivery:boolean,level:bigint,pillarid:bigint,isdlvr:boolean,extt:bigint,contact_type:string,free_disk_space:bigint,tab:bigint,action:string,dtm:double,abuseids:string,is_seller:boolean,ns_channel:string,ns_type:string,ns_value:boolean,is_user_auth:boolean,duid:bigint,user_key:string,im:boolean,sgtx:string,sgtp:bigint,is_legit:boolean,sgt:string,sgt_mcids:string,objectid:string,objecttype:string,extension:string,geo_lat:double,geo_lng:double,is_antihack_phone_confirm:boolean,antihack_confirm_number:bigint,antihack_user_password_reset:boolean,color:string,exif_model:string,exif_make:string,create_time:bigint,update_time:bigint,app_type:string,ver:string,width:double,height:double,admuserid:bigint,premoderation:boolean,fraud_code_ids:string,afraud_version:bigint,login_type:string,is_first_message:boolean,network_type:string,date:string,plate_number:string,engine_power:string,transport_type:string,transport_category:string,timing:double,timing_duplicate:double,int8erface_version:string,admuser_id:bigint,time_to_content:string,exception_id:string,duplicates:string,abs:string,safedeal:string,dl:boolean,subtype:string,is_verified_inn:boolean,srd:string,form_input_field_name:string,form_input_field_value:string,channels:string,channels_deny:string,target_type:bigint,pmid:bigint,datefrom:string,dateto:string,base_item_id:bigint,reject_wrong_params:string,from_block:bigint,from_position:bigint,service_id:string,amount:string,operation_id:string,isp:boolean,block_items_display:bigint,block_items_added:bigint,is_anon:boolean,search_area:string,retry:boolean,browser_web_push_agreement:boolean,is_auth:boolean,snippet_placement:string,vasid_prev:bigint,_ga:string,advtid:string,sm:boolean,ach:boolean,premiums:bigint,peid:bigint,pdrid:bigint,usedSuggest:boolean,ppeid:bigint,userToken:string,img_id:bigint,vips:bigint,d:boolean,uas:bigint,sr:string,groupId:bigint,launchId:bigint,unique_args:string,model_version:string,subs_price_bonus:double,subs_price_packages:double,subs_price_ext_shop:double,subs_price_total:double,with_shop_flg:boolean,has_cv:boolean,phone_request_flg:boolean,msg_image:string,valid_until:bigint,rating:double,reviews_cnt:bigint,review_id:bigint,share_place:string,user_auth_memo_id:string,call_id:string,is_linked:boolean,rdt:string,notification_message_id:string,adm_comment:string,item_version:bigint,is_adblock:boolean,helpdesk_agent_state:string,anonymous_number_service_responce:string,additional_item:bigint,call_action_type:string,order_cancel_cause:bigint,order_cancel_cause_txt:string,is_verified:boolean,other_phones_number:bigint,item_number:bigint,phone_action:string,is_reverified:boolean,phone_action_type:string,order_cancel_cause_info:string,review_seq_id:bigint,errorsDetailed:string,subs_vertical_ids:string,subs_tariff_id:bigint,subs_proposed_bonus_factor:double,subs_extensions:string,subs_packages:string,caller_user_type:string,message_preview:string,car_doors:string,engine_type:string,transmission:string,car_drive:string,steering_wheel:string,engine_volume:string,article_dissatisfaction_reason:string,redirect_incoming_call_to:string,placement:string,action_type:string,from_source:string,cnt_favourites:bigint,ces_score:bigint,pvas_group_old:bigint,pvas_group:bigint,pvas_groups:string,page_view_session_time:bigint,ctcallid:bigint,is_geo_delivery_widget:boolean,api_path:string,antihack_reason:string,buyer_booking_cancel_cause:string,ticket_comment_id:bigint,src_split_ticket_id:bigint,trgt_split_ticket_id:bigint,time_to_first_byte:bigint,time_to_first_paint8:bigint,time_to_first_int8eractive:bigint,time_to_dom_ready:bigint,time_to_on_load:bigint,time_to_target_content:bigint,time_for_async_target_content:bigint,changed_microcat_id:bigint,scenario:string,cv_use_category:boolean,cv_use_title:boolean,str_buyer_contact_result_reason:string,profile_tab:string,is_original_user_report:boolean,autoteka_user_type:bigint,list_load_number:bigint,ssid:bigint,recommendpaysysid:string,app_version:string,os_version:string,accounts_number:bigint,search_correction_action:string,search_correction_original:string,search_correction_corrected:string,search_correction_method:string,autosort_images:boolean,cnt_subscribers:bigint,is_oasis:boolean,pvas_dates:string,oneclickpayment:boolean,project:string,location_text_input:string,cadastralnumber:string,report_duration:bigint,report_status:boolean,page_number:bigint,deep_link:string,item_add_screen:string,user_ids:string,shop_fraud_reason_ids:string,shop_moderation_action_hash:string,checkbox_an_enable:boolean,message_type:string,app_version_code:string,banner_id:string,shortcut_description:string,close_timeout:bigint,selling_system:string,banner_code:string,wsrc:string,shops_array:string,subscription_promo:boolean,sub_is_ss:boolean,sub_prolong:string,target_page:string,mobile_event_duration:bigint,screen_name:string,content_type:string,mobile_app_page_number:bigint,roads:string,img_download_status:boolean,screen_start_time:bigint,sgt_cat_flag:boolean,ns_owner:string,software_version:string,build:string,adpartner:bigint,ticket_channel:bigint,adslot:string,statid:bigint,current_subs_version:string,new_subs_version:string,subscription_edit:boolean,channel_number:bigint,channels_screen_count:bigint,delivery_help_question:string,banner_due_date:bigint,banner_show_days:boolean,banner_item_id:bigint,chat_error_case:string,has_messages:boolean,search_address_type:string,js_event_type:string,dom_node:string,js_event_slug:string,attr_title:string,attr_link:string,attr_value:string,key_name:string,is_ctrl_pressed:boolean,is_alt_pressed:boolean,is_shift_pressed:boolean,color_theme:string,dom_node_content:string,is_checkbox_checked:boolean,page_x_coord:bigint,page_y_coord:bigint,srd_initial:string,uploaded_files_cnt:bigint,review_additional_info:string,flow_type:bigint,flow_id:string,ces_article:bigint,items_locked_count:bigint,items_not_locked_count:bigint,word_sgt_clicks:bigint,metro:string,msg_app_name:string,msg_request:string,moderation_user_score:double,msg_button_type:string,action_payload:string,msg_chat_list_offset:bigint,ces_hd:bigint,msg_throttling_reason:string,msg_app_version:string,RealtyDevelopment_id:string,metro_list:string,distance_list:string,district_list:string,block_uids:string,msg_blacklist_reason_id:bigint,roads_list:string,msg_random_id:string,msg_int8ernet_connection:boolean,msg_socket_type:bigint,msg_is_push:boolean,cities_list:string,uids_rec:string,email_hash:string,target_hash:string,click_position:bigint,phone_pdhash:string,caller_phone_pdhash:string,avitopro_date_preset:string,skill_id:bigint,safedeal_orderid:bigint,msg_search_query:string,msg_search_success:boolean,msg_chat_page_num:bigint,option_number:bigint,short_term_rent:boolean,issfdl:boolean,helpdesk_user_id:bigint,cv_suggest_show_type:string,review_score:bigint,stage:bigint,sgt_building_id:string,page_from:string,item_condition:string,span_end_time:bigint,custom_param:string,subs_vertical_id:bigint,shop_on_moderation:boolean,parameter_value_slug:string,parameter_value_id:bigint,query_length:bigint,new_category_id:bigint,api_method_name:string,courier_survey_reasons:string,courier_survey_reasons_comment:string,screen_touch_time:bigint,msg_reason_id:bigint,geo_session:string,inactive_page:string,location_suggest_text:string,answer_seq_id:bigint,new_param_ids:string,autoteka_cookie:string,landing_slug:string,autoteka_user_id:string,utm_source:string,utm_medium:string,utm_campaign:string,is_paid:boolean,is_from_avito:boolean,autoteka_order_id:bigint,autoteka_report_id:bigint,safedeal_services:string,performance_timing_redirect_start:double,performance_timing_redirect_end:double,performance_timing_fetch_start:double,performance_timing_domain_lookup_start:double,performance_timing_domain_lookup_end:double,performance_timing_connect_start:double,performance_timing_secure_connection_start:double,performance_timing_connect_end:double,performance_timing_request_start:double,performance_timing_response_start:double,performance_timing_response_end:double,performance_timing_first_paint8:double,performance_timing_first_contentful_paint8:double,performance_timing_dom_int8eractive:double,performance_timing_dom_content_loaded_event_start:double,performance_timing_dom_content_loaded_event_end:double,performance_timing_dom_complete:double,performance_timing_load_event_start:double,performance_timing_load_event_end:double,autoload_tags:string,autoload_not_empty_tags:string,screen_width:bigint,screen_height:bigint,is_new_tab:boolean,autoload_region:string,autoload_subway:string,autoload_street:string,autoload_district:string,autoload_direction_road:string,autoload_distance_to_city:bigint,autoload_item_id:bigint,tip_type:string,error_text:string,abuse_msg:string,cpa_abuse_id:bigint,call_status:string,alid:string,ad_error:bigint,req_num:bigint,app_startup_time:bigint,is_from_ab_test:boolean,upp_call_id:string,upp_provider_id:bigint,upp_virtual_phone:bigint,upp_incoming_phone:string,upp_client:string,upp_linked_phone:string,upp_allocate_id:string,upp_call_eventtype:bigint,upp_call_event_time:bigint,upp_call_is_blocked:boolean,upp_call_duration:bigint,upp_talk_duration:bigint,upp_call_accepted_at:bigint,upp_call_ended_at:bigint,upp_record_url:string,upp_record:boolean,upp_caller_message:string,upp_call_receiver_message:string,upp_transfer_result:string,form_validation_error_texts:string,sgt_item_type:string,landing_action:string,prof_profile_type:string,save_type:bigint,phone_show_result:bigint");

    KafkaSource src("clickstream-kafka01:9092",
                    "user-keyed-clickstream",
                    "*:-2:1000000",
                    "test-omnisci",
                    sink);

    src.setup();
    src.process();
    src.destroy();
    return 0;
}
