#ifndef _CSV_SINK_H
#define _CSV_SINK_H

#include "sink.h"
#include "tsl/hopscotch_map.h"


enum OutputFormat
{
    OUTPUT_JSON,
    OUTPUT_EAV,
    OUTPUT_ARRAY,
    OUTPUT_COLUMNS
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

    CSVSink(std::string format, std::string delimiter, std::string terminator);

    void put(cppkafka::Message &doc);
    void flush();

private:
    void parseFields(const std::string &format, bool defaultGroupping);

    inline std::string toRecordHeader(cppkafka::Message &doc);
    inline std::string evalValue(const std::string &s, int transform);

    std::string toRecord(cppkafka::Message &doc);
    std::string toSparseRecord(cppkafka::Message &doc);
    std::string toArrayRecordComp(cppkafka::Message &doc);
    std::string toJSONRecord(cppkafka::Message &doc);
    std::string toEAVRecord(cppkafka::Message &doc);
};

#endif
