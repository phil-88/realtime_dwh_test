#ifndef _SINK_ARES_H
#define _SINK_ARES_H

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


#endif
