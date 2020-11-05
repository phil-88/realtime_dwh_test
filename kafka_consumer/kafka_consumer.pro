#-------------------------------------------------
#
# Project created by QtCreator 2017-10-02T18:27:27
#
#-------------------------------------------------

QT       -= core gui

QMAKE_CXX = g++
QMAKE_CC = gcc

TARGET = kafka_consumer
TEMPLATE = app
CONFIG += c++17
QMAKE_CXXFLAGS_RELEASE = -fabi-version=2 -D_GLIBCXX_USE_CXX11_ABI=0 -Wno-unused-parameter -fPIC -std=gnu++1z -march=native -mtune=native -O3
QMAKE_CFLAGS_RELEASE = -Wno-unused-parameter -fPIC -std=gnu++1z -march=native -mtune=native -O3
SOURCES += kafka_consumer.cpp vhash.cpp 
INCLUDEPATH += /usr/local/include ./tsl .

LIBS += /usr/local/lib/libcppkafka.a /usr/local/lib/librdkafka.a \
        /usr/local/lib/liborc.a \
        /usr/local/lib/libsnappy.a \
        /usr/local/lib/libz.a \
        /usr/local/lib/libzstd.a \
        /usr/local/lib/liblz4.a \
        /usr/local/lib/libprotobuf.a \
        /usr/local/lib/libprotoc.a \
        /usr/local/lib/libhdfspp_static.a \
	-lpthread -lrt -ldl -lz -lssl -lcrypto -lsasl2

#        -Wl,-Bstatic -lstdc++ -lgcc -Wl,-Bdynamic \
