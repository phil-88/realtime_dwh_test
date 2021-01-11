
#include "orc.h"
#include "vhash.h"
#include "utils.h"


using namespace std;
using namespace cppkafka;


ORCSink::ORCSink(std::string columnTypeFmt, std::string filename) :
    batchSize(1024), rows(0),
    buffer(*orc::getDefaultPool(), 350 * 100 * 1024), offset(0)
{
    outStream = writeLocalFile(filename);
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

void ORCSink::put(Message &doc)
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
    for (size_t i = 0; i < values.size(); ++i)
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
        else if (fieldType[i] == orc::STRING)
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
        rows = 0;
        offset = 0;
    }
}

void ORCSink::flush()
{
    if (rows != 0)
    {
        writeBatch();
        rows = 0;
        offset = 0;
    }
    writer->close();
}

void ORCSink::writeBatch()
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
}

void ORCSink::preserve(size_t valueSize, StringVectorBatch *stringBatch)
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

