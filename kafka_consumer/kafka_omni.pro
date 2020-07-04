#-------------------------------------------------
#
# Project created by QtCreator 2017-10-02T18:27:27
#
#-------------------------------------------------

QT       -= core gui

QMAKE_CXX = g++-8
QMAKE_CC = gcc-8

TARGET = kafka_omni
TEMPLATE = app
CONFIG += c++11
QMAKE_CXXFLAGS_RELEASE = -Wno-unused-parameter -fPIC -std=gnu++1y -march=native -mtune=native -O3
QMAKE_CFLAGS_RELEASE = -Wno-unused-parameter -fPIC -std=gnu++1y -march=native -mtune=native -O3
SOURCES += kafka_omni.cpp vhash.cpp 
INCLUDEPATH += /usr/local/include ./tsl .

LIBS += /usr/local/lib64/libcppkafka.a /usr/local/lib/librdkafka.a \
	-lpthread -lrt -ldl -lz -lssl -lcrypto -lsasl2
