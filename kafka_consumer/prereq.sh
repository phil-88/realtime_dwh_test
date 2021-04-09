#!/bin/bash

#export CXXFLAGS="-fabi-version=2 -D_GLIBCXX_USE_CXX11_ABI=0 -fPIC"

ARCLOCATION=https://github.com/apache/orc/archive/master.zip
wget -O /tmp/orc.zip $ARCLOCATION 
unzip /tmp/orc.zip -d /tmp/
cd /tmp/orc-master
mkdir cmake-build
cd cmake-build
cmake .. -DBUILD_CPP_TESTS=OFF -DBUILD_JAVA=OFF 
make 
make install

ARCLOCATION=https://github.com/edenhill/librdkafka/archive/v1.1.0.tar.gz
wget -O - $ARCLOCATION | tar xz -C /tmp/
cd /tmp/librdkafka-1.1.0
mkdir cmake-build
cd cmake-build
cmake -DWITH_C11THREADS=OFF -DCMAKE_C_FLAGS="$CFLAGS -D__STDC_VERSION__=201112 -fPIC" -DCMAKE_CXX_FLAGS="$CXXFLAGS -std=gnu++1y -D__STDC_VERSION__=201112 -fPIC" -DWITH_ZSTD=OFF -DENABLE_LZ4_EXT=OFF -DRDKAFKA_BUILD_STATIC=ON -DRDKAFKA_BUILD_EXAMPLES=OFF -DRDKAFKA_BUILD_TESTS=OFF .. \
 && make \
 && make install

ARCLOCATION=https://github.com/mfontanini/cppkafka/archive/master.tar.gz
wget -O - $ARCLOCATION | tar xz -C /tmp/
cd /tmp/cppkafka-master
mkdir cmake-build
cd cmake-build
cmake -DCPPKAFKA_BUILD_SHARED=OFF -DCPPKAFKA_DISABLE_EXAMPLES=ON -DCMAKE_CXX_FLAGS="$CXXFLAGS -fPIC -std=c++11" .. \
 && make \
 && make install

ARCLOCATION=https://github.com/abseil/abseil-cpp/archive/refs/heads/master.zip
wget -O /tmp/abseil-cpp.zip $ARCLOCATION
unzip /tmp/abseil-cpp.zip -d /tmp/
cd /tmp/abseil-cpp-master
mkdir cmake-build
cd cmake-build
cmake -DCPPKAFKA_BUILD_SHARED=OFF -DCMAKE_C_FLAGS="$CFLAGS -std=c++17 -fPIC" -DCMAKE_CXX_FLAGS="$CXXFLAGS -std=c++17 -fPIC" .. \
 && make \
 && make install

ARCLOCATION=https://github.com/ClickHouse/clickhouse-cpp/archive/master.zip
wget -O /tmp/clickhouse-cpp.zip $ARCLOCATION
unzip /tmp/clickhouse-cpp.zip -d /tmp/
cd /tmp/clickhouse-cpp-master
mkdir cmake-build
cd cmake-build
cmake -DCMAKE_VERBOSE_MAKEFILE=ON \
        -DCMAKE_C_FLAGS="$CFLAGS -std=c++17 -fPIC" \
        -DCMAKE_CXX_FLAGS="$CXXFLAGS -std=c++17 -fPIC" .. \
 && make \
 && make install \
 && find contrib/ -name '*.a' -exec cp {} /usr/local/lib \;

