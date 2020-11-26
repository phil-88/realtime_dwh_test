
#include "clickhouse.h"
#include "vhash.h"
#include "utils.h"

using namespace std;
using namespace cppkafka;


ClickhouseSink::ClickhouseSink(string tableName, string columnTypeFmt,
                               string host, int port, string database, string user, string password)
    : blockSize(100000), row(0)
{
    this->tableName = tableName;

    ClientOptions opt;
    opt.SetHost(host);
    opt.SetPort(port);
    opt.SetDefaultDatabase(database);
    opt.SetUser(user);
    opt.SetPassword(password);
//    opt.SetCompressionMethod(CompressionMethod::LZ4);

    client = new Client(opt);

    unordered_map<string, int> typeId;
    typeId["UInt8"] = Type::UInt8;
    typeId["UInt32"] = Type::UInt32;
    typeId["UInt64"] = Type::UInt64;
    typeId["Float32"] = Type::Float32;
    typeId["Float64"] = Type::Float64;
    typeId["String"] = Type::String;

    for (string colType : splitString(columnTypeFmt, ','))
    {
        vector<string> tokens = splitString(colType, ':');
        string name = tokens[0];
        string type = tokens[1];

        fieldIndex[name] = fieldName.size();
        fieldName.push_back(name);
        fieldType.push_back(typeId[type]);
    }

    fieldValues.resize(fieldName.size());
}


#define allocateColumn(Type, Default) \
    if (!fieldValues[i]) \
    { \
        fieldValues[i] = new ColumnData(); \
        fieldValues[i]->nulls = vector<uint8>(blockSize, 1); \
        fieldValues[i]->value_ ## Type = vector<Type>(blockSize, Default); \
    }


void ClickhouseSink::put(Message &doc)
{
    int totalSize = 0;
    const int fieldCount = fieldName.size();
    static std::vector<std::pair<const char*, int> > values(fieldCount, make_pair("", 0));
    memset(values.data(), 0, sizeof make_pair("", 0) * fieldCount);

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
        }
        else if (fieldType[i] == Type::UInt32)
        {
            allocateColumn(uint32, 0)
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_uint32[row] = strtoul(values[i].first, 0, 10);
        }
        else if (fieldType[i] == Type::UInt64)
        {
            allocateColumn(uint64, 0);
            fieldValues[i]->nulls[row] = 0;
            fieldValues[i]->value_uint64[row] = strtoull(values[i].first, 0, 10);
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
                fieldValues[i]->value_string = new ColumnString();
                fieldValues[i]->last_value_ind = -1;
            }
            fieldValues[i]->nulls[row] = 0;
            for (int j = fieldValues[i]->last_value_ind + 1; j < row; ++j)
            {
                fieldValues[i]->value_string->Append(string_view());
            }
            fieldValues[i]->last_value_ind = row;
            fieldValues[i]->value_string->Append(string_view(values[i].first, values[i].second));
        }
    }

    row++;
    if (row == blockSize)
    {
        writeBlock();
        row = 0;
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
        }
        writeBlock();
        row = 0;
    }
}

void ClickhouseSink::writeBlock()
{
    Block block;

    for (size_t i = 0; i < fieldValues.size(); ++i)
    {
        if (!fieldValues[i])
        {
            continue;
        }

        if (fieldType[i] == Type::UInt8)
        {
            ColumnNullable *col = new ColumnNullable(
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->value_uint8))),
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->nulls))));
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::UInt32)
        {
            ColumnNullable *col = new ColumnNullable(
                shared_ptr<ColumnUInt32>(new ColumnUInt32(std::move(fieldValues[i]->value_uint32))),
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->nulls))));
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::UInt64)
        {
            ColumnNullable *col = new ColumnNullable(
                shared_ptr<ColumnUInt64>(new ColumnUInt64(std::move(fieldValues[i]->value_uint64))),
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->nulls))));
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::Float32)
        {
            ColumnNullable *col = new ColumnNullable(
                shared_ptr<ColumnFloat32>(new ColumnFloat32(std::move(fieldValues[i]->value_float))),
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->nulls))));
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::Float64)
        {
            ColumnNullable *col = new ColumnNullable(
                shared_ptr<ColumnFloat64>(new ColumnFloat64(std::move(fieldValues[i]->value_double))),
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->nulls))));
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }
        else if (fieldType[i] == Type::String)
        {
            for (int j = fieldValues[i]->last_value_ind + 1; j < row; ++j)
            {
                fieldValues[i]->value_string->Append(string_view());
            }
            ColumnNullable *col = new ColumnNullable(
                shared_ptr<ColumnString>(fieldValues[i]->value_string),
                shared_ptr<ColumnUInt8>(new ColumnUInt8(std::move(fieldValues[i]->nulls))));
            block.AppendColumn(fieldName[i], shared_ptr<Column>(col));
        }

        delete fieldValues[i];
        fieldValues[i] = NULL;
    }

    //client->Insert(this->tableName, block);
}

