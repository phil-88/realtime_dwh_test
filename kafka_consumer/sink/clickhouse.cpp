
#include <thread>

#include "clickhouse.h"
#include "vhash.h"
#include "utils.h"

using namespace std;
using namespace cppkafka;


ClickhouseSink::ClickhouseSink(string tableName,
    string host, int port, string database, string user, string password,
    int batchSize, int threadCount, bool hasNulls, bool useCompression)
    : tableName(tableName), blockSize(batchSize), blockParts(threadCount), row(0),
      hasNulls(hasNulls), useCompression(useCompression)
{
    ClientOptions opt;
    opt.SetHost(host);
    opt.SetPort(port);
    opt.SetDefaultDatabase(database);
    opt.SetUser(user);
    opt.SetPassword(password);
    if (useCompression)
    {
        opt.SetCompressionMethod(CompressionMethod::LZ4);
    }

    for (int i = 0; i < threadCount; ++i)
    {
        clients.push_back(new Client(opt));
    }

    Client *client = clients.front();

    unordered_map<string, int> typeId;
    typeId["UInt8"] = Type::UInt8;
    typeId["UInt32"] = Type::UInt32;
    typeId["UInt64"] = Type::UInt64;
    typeId["Float32"] = Type::Float32;
    typeId["Float64"] = Type::Float64;
    typeId["String"] = Type::String;

    client->Select(
        "select name, type "
        "from system.columns "
        "where database || '.' || table = '" + tableName + "'"
        "  and default_kind = ''",
        [&](const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) 
            {
                string name(block[0]->As<ColumnString>()->At(i));
                string type(block[1]->As<ColumnString>()->At(i));

                int ind = fieldName.size();
                fieldIndex[name] = ind;
                fieldName.push_back(name);
                fieldType.push_back(typeId[type]);

                if (name == "_partition")
                {
                    serviceFields.push_back(ind);
                    serviceTypes.push_back(HEADER_PARTITION);
                }
                else if (name == "_offset")
                {
                    serviceFields.push_back(ind);
                    serviceTypes.push_back(HEADER_OFFSET);
                }
                else if (name == "_timestamp")
                {
                    serviceFields.push_back(ind);
                    serviceTypes.push_back(HEADER_TIMESTAMP);
                }
            }
        }
    );

    fieldValues.resize(fieldName.size());
}


#define allocateColumn(Type, Default) \
    if (!fieldValues[i]) \
    { \
        fieldValues[i] = new ColumnData(); \
        fieldValues[i]->nulls = vector<uint8>(blockSize, 1); \
        fieldValues[i]->value_ ## Type = vector<Type>(blockSize, Default); \
    }

#define allocateMandatoryColumn(Type, Default) \
    if (!fieldValues[i]) \
    { \
        fieldValues[i] = new ColumnData(); \
        fieldValues[i]->nulls = vector<uint8>(blockSize, 0); \
        fieldValues[i]->value_ ## Type = vector<Type>(blockSize, Default); \
    }

#define isTrueStr(cstr_len_pair) \
    isTrue(cstr_len_pair.first, cstr_len_pair.second)


void ClickhouseSink::put(Message &doc)
{
    int totalSize = 0;
    const int fieldCount = fieldName.size();
    static auto emptyPair = make_pair("", 0);
    static std::vector<std::pair<const char*, int> > values(fieldCount, emptyPair);
    memset((void*)values.data(), 0, sizeof emptyPair * fieldCount);

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

    // fill header
    for (size_t h = 0; h < serviceFields.size(); ++h)
    {
        int i = serviceFields[h];
        values[i] = emptyPair;
        if (serviceTypes[h] == HEADER_PARTITION)
        {
            allocateMandatoryColumn(uint32, 0)
            fieldValues[i]->value_uint32[row] = doc.get_partition();
        }
        else if (serviceTypes[h] == HEADER_OFFSET)
        {
            allocateMandatoryColumn(uint64, 0)
            fieldValues[i]->value_uint64[row] = doc.get_offset();
        }
        else if (serviceTypes[h] == HEADER_TIMESTAMP)
        {
            allocateMandatoryColumn(uint64, 0)
            fieldValues[i]->value_uint64[row] = rd_kafka_message_timestamp(doc.get_handle(), NULL);
        }
    }

    // fill columns
    int partSize = blockSize / blockParts;

    for (size_t i = 0; i < values.size(); ++i)
    {
        if (values[i].second == 0)
        {
            continue;
        }

        if (fieldType[i] == Type::UInt8)
        {
            allocateColumn(uint8, 0)
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_uint8[row] = strtol(values[i].first, 0, 10);
            if (fieldValues[i]->value_uint8[row] == 0 && isTrueStr(values[i]))
                fieldValues[i]->value_uint8[row] = 1;
        }
        else if (fieldType[i] == Type::UInt32)
        {
            allocateColumn(uint32, 0)
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_uint32[row] = strtoul(values[i].first, 0, 10);
            if (fieldValues[i]->value_uint32[row] == 0 && isTrueStr(values[i]))
                fieldValues[i]->value_uint32[row] = 1;
        }
        else if (fieldType[i] == Type::UInt64)
        {
            allocateColumn(uint64, 0);
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_uint64[row] = strtoull(values[i].first, 0, 10);
            if (fieldValues[i]->value_uint64[row] == 0 && isTrueStr(values[i]))
                fieldValues[i]->value_uint64[row] = 1;
        }
        else if (fieldType[i] == Type::Float32)
        {
            allocateColumn(float, 0);
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_float[row] = strtof(values[i].first, 0);
        }
        else if (fieldType[i] == Type::Float64)
        {
            allocateColumn(double, 0);
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_double[row] = strtod(values[i].first, 0);
        }
        else if (fieldType[i] == Type::String)
        {
            if (!fieldValues[i])
            {
                fieldValues[i] = new ColumnData;
                fieldValues[i]->nulls = vector<uint8>(blockSize, 1);
                fieldValues[i]->value_string = vector<ColumnString*>(blockParts, NULL);
                for (int k = 0; k < blockParts; ++k)
                {
                    fieldValues[i]->value_string[k] = new ColumnString;
                }
                fieldValues[i]->last_value_ind = -1;
            }
            fieldValues[i]->nulls[row] = 0;
            fillEmptyStrings(fieldValues[i], partSize);
            fieldValues[i]->value_string[row / partSize]->Append(string_view(values[i].first, values[i].second));
        }
    }

    row++;
    if (row == blockSize)
    {
        flush();
    }
}

void ClickhouseSink::flush()
{
    if (row != 0)
    {
        for (size_t i = 0; i < fieldValues.size(); ++i)
        {
            if (!fieldValues[i]) continue;

            if (fieldValues[i]->nulls.size()) fieldValues[i]->nulls.resize(row);
            if (fieldValues[i]->value_uint8.size()) fieldValues[i]->value_uint8.resize(row);
            if (fieldValues[i]->value_uint32.size()) fieldValues[i]->value_uint32.resize(row);
            if (fieldValues[i]->value_uint64.size()) fieldValues[i]->value_uint64.resize(row);
            if (fieldValues[i]->value_float.size()) fieldValues[i]->value_float.resize(row);
            if (fieldValues[i]->value_double.size()) fieldValues[i]->value_double.resize(row);

            if (fieldType[i] == Type::String)
            {
                int partSize = blockSize / blockParts;
                fillEmptyStrings(fieldValues[i], partSize);
            }
        }
        writeBlock();
        row = 0;
    }
}

bool ClickhouseSink::isFlushed() const 
{
    return row == 0;
}

void ClickhouseSink::fillEmptyStrings(ColumnData *values, int partSize)
{
    for (int j = values->last_value_ind + 1; j < row; ++j)
    {
        values->value_string[j / partSize]->Append(string_view());
    }
    values->last_value_ind = row;
}

void ClickhouseSink::writeBlock()
{
    int rowCount = row;
    int partSize = blockSize / blockParts;
    int threadCount = (blockParts * rowCount + blockSize - 1) / blockSize;

    vector<thread> threads(threadCount);
    for (int k = 0; k < threadCount; ++k)
    {
        int st = partSize * k;
        int sz = min(rowCount, partSize * (k + 1)) - st;

        fprintf(stderr, "write block part %d: %d, %d\n", k, st, sz);
        threads[k] = thread(&ClickhouseSink::writeBlockPart, this, k, st, sz);
    }

    for (int k = 0; k < threadCount; ++k)
    {
        threads[k].join();
    }

    for (size_t i = 0; i < fieldValues.size(); ++i)
    {
        delete fieldValues[i];
        fieldValues[i] = NULL;
    }
}

#define slice(typ, vec, st, sz) \
    vector<typ>(vec.begin() + st, vec.begin() + st + sz)

void ClickhouseSink::writeBlockPart(int partNo, int st, int sz)
{
    Client *client = clients[partNo];
    Block block;

    for (size_t i = 0; i < fieldValues.size(); ++i)
    {
        if (!fieldValues[i])
        {
            continue;
        }

        if (fieldType[i] == Type::UInt8)
        {
            Column *col = new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->value_uint8, st, sz)));
            if (hasNulls)
            {
                col = new ColumnNullable(shared_ptr<Column>(col), shared_ptr<ColumnUInt8>(
                                         new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->nulls, st, sz)))));
            }
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::UInt32)
        {
            Column *col = new ColumnUInt32(std::move(slice(uint32, fieldValues[i]->value_uint32, st, sz)));
            if (hasNulls)
            {
                col = new ColumnNullable(shared_ptr<Column>(col), shared_ptr<ColumnUInt8>(
                                         new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->nulls, st, sz)))));
            }
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::UInt64)
        {
            Column *col = new ColumnUInt64(std::move(slice(uint64, fieldValues[i]->value_uint64, st, sz)));
            if (hasNulls)
            {
                col = new ColumnNullable(shared_ptr<Column>(col), shared_ptr<ColumnUInt8>(
                                         new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->nulls, st, sz)))));
            }
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::Float32)
        {
            Column *col = new ColumnFloat32(std::move(slice(float, fieldValues[i]->value_float, st, sz)));
            if (hasNulls)
            {
                col = new ColumnNullable(shared_ptr<Column>(col), shared_ptr<ColumnUInt8>(
                                         new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->nulls, st, sz)))));
            }
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::Float64)
        {
            Column *col = new ColumnFloat64(std::move(slice(double, fieldValues[i]->value_double, st, sz)));
            if (hasNulls)
            {
                col = new ColumnNullable(shared_ptr<Column>(col), shared_ptr<ColumnUInt8>(
                                         new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->nulls, st, sz)))));
            }
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::String)
        {
            Column *col = fieldValues[i]->value_string[partNo];
            if (hasNulls)
            {
                col = new ColumnNullable(shared_ptr<Column>(col), shared_ptr<ColumnUInt8>(
                                         new ColumnUInt8(std::move(slice(uint8, fieldValues[i]->nulls, st, sz)))));
            }
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
    }

    fprintf(stderr, "send block part %d: %d, %d\n", partNo, st, sz);
    client->Insert(this->tableName, block);
    fprintf(stderr, "block inserted %d\n", partNo);
}
