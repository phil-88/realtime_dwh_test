
#include "csv.h"
#include "vhash.h"
#include "utils.h"

#define MAX_INDEX_SIZE 2024
#define MAX_FIELD_COUNT 256
#define MAX_GROUP_COUNT 64
#define MAX_VALUES_SIZE 16000

using namespace std;
using namespace cppkafka;


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


CSVSink::CSVSink(std::string format, std::string delimiter, std::string terminator)
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

void CSVSink::parseFields(const std::string &format, bool defaultGroupping)
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

void CSVSink::put(Message &doc)
{
    std::cout << toRecord(doc);
}

void CSVSink::flush()
{
}

std::string CSVSink::toRecord(Message &doc)
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

inline std::string CSVSink::toRecordHeader(Message &doc)
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

inline std::string CSVSink::evalValue(const std::string &s, int transform)
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

std::string CSVSink::toSparseRecord(Message &doc)
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
                    int len = sprintf(transformBuffer + transformOffset, "%" PRId64, v);
                    values[ind] = make_pair(transformBuffer + transformOffset, len);
                    transformOffset += len;
                    totalSize += len;
                }
                else if (fieldFunction[ind] == IDENTICAL)
                {
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

std::string CSVSink::toArrayRecordComp(Message &doc)
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

//                if (t[i + 1].type == JSMN_STRING)
//                {
//                    size = unescape(s, size);
//                }
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

std::string CSVSink::toJSONRecord(Message &doc)
{
    std::string j(doc.get_payload());
    return toRecordHeader(doc) + j + terminator;
}

std::string CSVSink::toEAVRecord(Message &doc)
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

