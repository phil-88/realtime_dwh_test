#!/bin/bash

if [ ! -e /usr/local/include/cppkafka ] ; then
	sh prereq.sh
fi

WITH_ORC=false
WITH_CH=true


ORCDEPS="/usr/local/lib/liborc.a
/usr/local/lib/libsnappy.a
/usr/local/lib/libz.a
/usr/local/lib/libzstd.a
/usr/local/lib/liblz4.a
/usr/local/lib/libprotobuf.a
/usr/local/lib/libprotoc.a
/usr/local/lib/libhdfspp_static.a"

CHDEPS="/usr/local/lib/libclickhouse-cpp-lib-static.a
/usr/local/lib/libcityhash-lib.a
/usr/local/lib/liblz4-lib.a"

EXTRA_SOURCES=""

if $WITH_ORC ; then
	EXTRA_SOURCES="$EXTRA_SOURCES -DWITH_ORC sink/orc.cpp $ORCDEPS"
fi

if $WITH_CH ; then
	EXTRA_SOURCES="$EXTRA_SOURCES -DWITH_CH sink/clickhouse.cpp $CHDEPS"
fi

gcc -c -o math_compat.o math_compat.c

g++ $CXXFLAGS -fPIC -std=gnu++1z  \
    -I. -Itsl/ -I /usr/local/include/ \
    -O3 -march=native -mtune=native \
    -Wall -Wno-unused-function -Wno-unused-value -Wno-narrowing \
    -o kafka_consumer \
    kafka_consumer.cpp utils.cpp vhash.cpp math_compat.o -Wl,--wrap=log2 -Wl,--wrap=log -Wl,--wrap=pow \
    sink/csv.cpp \
    $EXTRA_SOURCES \
    /usr/local/lib/libcppkafka.a /usr/local/lib/librdkafka.a \
    -Wl,--whole-archive -Wl,-Bstatic -lstdc++ -lgcc -Wl,-Bdynamic -Wl,--no-whole-archive \
    -lssl -lcrypto -lsasl2 -lrt -ldl -lz 

#    /lib/x86_64-linux-gnu/libc_nonshared.a \
#    -DGLIBC_COMPAT_LEVEL=0x020205 \
#    -Wl,compat=GLIBC_2.2.5 \

