
#ifndef _CLICKHOUSE_SINK_H
#define _CLICKHOUSE_SINK_H

#include "sink.h"
#include "utils.h"
#include "tsl/hopscotch_map.h"

#include <clickhouse/client.h>


using namespace clickhouse;


struct ColumnData
{
    std::vector<uint8> nulls;
    std::vector<uint8>  value_uint8;
    std::vector<uint32> value_uint32;
    std::vector<uint64> value_uint64;
    std::vector<float>  value_float;
    std::vector<double> value_double;
    ColumnString *value_string;
    int last_value_ind;
};


class ClickhouseSink : public Sink
{
    Client *client;
    int blockSize;
    int row;
    std::string tableName;
    bool hasNulls;
    bool useCompression;

    tsl::hopscotch_map<std::string, int, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, int> >, 30, true, tsl::power_of_two_growth_policy> fieldIndex;

    std::vector<std::string> fieldName;
    std::vector<int> fieldType;
    std::vector<ColumnData*> fieldValues;

    std::vector<int> serviceFields;
    std::vector<int> serviceTypes;

public:
    ClickhouseSink(std::string tableName, std::string host, int port, std::string database, std::string user, std::string password);

    void put(cppkafka::Message &doc);
    void flush();

private:
    void writeBlock();
};

#endif
