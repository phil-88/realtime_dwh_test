#!/bin/bash

if [ ! -e /usr/local/include/cppkafka ] ; then
	sh prereq.sh
fi

g++ $CXXFLAGS -fPIC -std=gnu++1y \
    -I /opt/vertica/sdk/include -I. -Itsl/ \
    -I /usr/local/include/ \
    -O3 -march=native -mtune=native \
    -Wall -Wno-unused-value -Wno-narrowing \
    -o kafka_consumer kafka_consumer.cpp vhash.cpp \
        /usr/local/lib/libcppkafka.a /usr/local/lib/librdkafka.a \
        /usr/local/lib/liborc.a \
        /usr/local/lib/libsnappy.a \
        /usr/local/lib/libz.a \
        /usr/local/lib/libzstd.a \
        /usr/local/lib/liblz4.a \
        /usr/local/lib/libprotobuf.a \
        /usr/local/lib/libprotoc.a \
        /usr/local/lib/libhdfspp_static.a \
    -Wl,--whole-archive -Wl,-Bstatic -lstdc++ -lgcc -Wl,-Bdynamic \
    -Wl,--whole-archive -Wl,-Bstatic -lssl -lcrypto -lsasl2 -Wl,-Bdynamic \
    -Wl,--no-whole-archive \
    -lpthread -lrt -ldl -lz 

#-lpthread -lrt -ldl -lz -lssl -lcrypto -lsasl2
