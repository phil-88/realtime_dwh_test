#ifndef _SINK_H
#define _SINK_H

#include "cppkafka/message.h"

class Sink
{
public:
    virtual void put(cppkafka::Message &doc) = 0;
    virtual void flush() = 0;
};

#endif
