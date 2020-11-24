
#ifndef _ORC_SINK_H
#define _ORC_SINK_H

#include "sink.h"
#include "tsl/hopscotch_map.h"

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

    ORCSink(std::string columnTypeFmt, std::string filename);

    void put(cppkafka::Message &doc);
    void flush();

private:

    void writeBatch();
    void preserve(size_t valueSize, StringVectorBatch *stringBatch);
};

#endif
