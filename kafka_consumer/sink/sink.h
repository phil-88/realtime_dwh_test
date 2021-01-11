#ifndef _SINK_H
#define _SINK_H

#include "cppkafka/message.h"

enum HeaderFields
{
    HEADER_PARTITION,
    HEADER_OFFSET,
    HEADER_KEY,
    HEADER_TIMESTAMP
};

class Sink
{
public:
    virtual void put(cppkafka::Message &doc) = 0;
    virtual void flush() = 0;
    virtual bool isFlushed() const {return false; }
};

#endif
