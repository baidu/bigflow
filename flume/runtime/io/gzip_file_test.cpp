/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include <string>
#include <vector>

#include "flume/runtime/io/compression_file_factory.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_array.h"
#include "toft/base/scoped_ptr.h"
#include "toft/compress/block/block_compression.h"
#include "toft/storage/file/file.h"
#include "boost/random/random_device.hpp"
#include "boost/random.hpp"

namespace baidu {
namespace flume {
namespace runtime {

TEST(GZipFile, Write) {
    toft::File* raw_file = toft::File::Open("output.dat", "w");
    toft::scoped_ptr<toft::File> comp_file(CompressionFileFactory::CompressionFile(raw_file, "gz"));

    std::string record1("Hello, world!");
    std::string record2("Hello, toft!");
    std::string record3("Hello, flume!");

    comp_file->Write(record1.data(), record1.size());
    comp_file->Write(record2.data(), record2.size());
    comp_file->Write(record3.data(), record3.size());

    comp_file->Close();

    toft::scoped_ptr<toft::File> read_file(toft::File::Open("output.dat", "r"));
    size_t buffer_size = 128u;
    toft::scoped_array<char> buffer(new char[buffer_size]);

    int size = read_file->Read(buffer.get(), buffer_size);

    toft::BlockCompression* compression = TOFT_CREATE_BLOCK_COMPRESSION("gzip");
    std::string uncompressed_data;
    CHECK(compression->Uncompress(buffer.get(), size, &uncompressed_data));

    ASSERT_EQ(uncompressed_data, "Hello, world!Hello, toft!Hello, flume!");
}

TEST(GZipFile, Write_Large_File) {
    toft::File* raw_file = toft::File::Open("output.dat", "w");
    toft::scoped_ptr<toft::File> comp_file(CompressionFileFactory::CompressionFile(raw_file, "gz"));


    boost::random::random_device rng;
    boost::uniform_int< > rand_num(33, 126);
    std::string record;

    size_t origin_size =  1024 * 1024 * 4;
    record.reserve(origin_size);
    int npot = 0;

    while(npot < origin_size) {
        char r = char(rand_num(rng));
        record += r;
        ++ npot;
    }

    comp_file->Write(record.data(), record.size());

    comp_file->Close();

    toft::File *file = toft::File::Open("output.dat", "r");
    CHECK_NOTNULL(file);
    toft::scoped_ptr<toft::File> decomp_file(CompressionFileFactory::CompressionFile(file, "gz"));
    toft::scoped_array<char> buffer(new char[origin_size]);
    int size = decomp_file->Read(buffer.get(), origin_size);
    std::string uncompressed_data(buffer.get(), size);
    ASSERT_EQ(uncompressed_data,record);
}

TEST(GZipFile, Read) {
    toft::File *file = toft::File::Open("testdata/gzip_file.data", "r");
    CHECK_NOTNULL(file);
    toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
    size_t buffer_size = 128u;
    toft::scoped_array<char> buffer(new char[buffer_size]);

    int size = read_file->Read(buffer.get(), buffer_size);
    std::string uncompressed_data(buffer.get(), size);
    ASSERT_EQ(uncompressed_data, "Hello, world!\nHello, toft!\nHello, flume!");
}

TEST(GZipFile, ReadLine) {
    {
        toft::File *file = toft::File::Open("testdata/gzip_file.data", "r");
        CHECK_NOTNULL(file);
        toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
        std::string result;
        ASSERT_FALSE(read_file->IsEof());
        ASSERT_TRUE(read_file->ReadLine(&result, 5));
        ASSERT_EQ("Hello", result);
        ASSERT_TRUE(read_file->ReadLine(&result));
        ASSERT_EQ(", world!", result);
        ASSERT_TRUE(read_file->ReadLine(&result, 12));
        ASSERT_EQ("Hello, toft!", result);
        ASSERT_TRUE(read_file->ReadLine(&result, 1));
        ASSERT_EQ("", result);
        ASSERT_TRUE(read_file->ReadLine(&result, 1));
        ASSERT_EQ("H", result);
        ASSERT_TRUE(read_file->ReadLine(&result, 12));
        ASSERT_EQ("ello, flume!", result);
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLineWithLineEnding(&result));
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLineWithLineEnding(&result));
    }
    {
        toft::File *file = toft::File::Open("testdata/gzip_eol.data", "r");
        CHECK_NOTNULL(file);
        toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
        std::string result;
        ASSERT_FALSE(read_file->IsEof());
        ASSERT_TRUE(read_file->ReadLine(&result, 1));
        ASSERT_EQ("", result);
    }
    {
        toft::File *file = toft::File::Open("testdata/gzip_file.data", "r");
        CHECK_NOTNULL(file);
        toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
        toft::StringPiece result;
        ASSERT_FALSE(read_file->IsEof());
        ASSERT_TRUE(read_file->ReadLine(&result, 5));
        ASSERT_EQ("Hello", result);
        ASSERT_TRUE(read_file->ReadLine(&result));
        ASSERT_EQ(", world!", result);
        ASSERT_TRUE(read_file->ReadLine(&result));
        ASSERT_EQ("Hello, toft!", result);
        ASSERT_TRUE(read_file->ReadLine(&result));
        ASSERT_EQ("Hello, flume!", result);
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLine(&result));
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLine(&result));
    }
}

TEST(GZipFile, ReadLineWithLineEnding) {
    {
        toft::File *file = toft::File::Open("testdata/gzip_file.data", "r");
        CHECK_NOTNULL(file);
        toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
        std::string result;
        ASSERT_FALSE(read_file->IsEof());
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 5));
        ASSERT_EQ("Hello", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result));
        ASSERT_EQ(", world!\n", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 12));
        ASSERT_EQ("Hello, toft!", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 1));
        ASSERT_EQ("\n", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 1));
        ASSERT_EQ("H", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 12));
        ASSERT_EQ("ello, flume!", result);
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLineWithLineEnding(&result));
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLineWithLineEnding(&result));
    }
    {
        toft::File *file = toft::File::Open("testdata/gzip_eol.data", "r");
        CHECK_NOTNULL(file);
        toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
        std::string result;
        ASSERT_FALSE(read_file->IsEof());
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 1));
        ASSERT_EQ("\n", result);
    }
    {
        toft::File *file = toft::File::Open("testdata/gzip_file.data", "r");
        CHECK_NOTNULL(file);
        toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
        toft::StringPiece result;
        ASSERT_FALSE(read_file->IsEof());
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 5));
        ASSERT_EQ("Hello", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result));
        ASSERT_EQ(", world!\n", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 12));
        ASSERT_EQ("Hello, toft!", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 1));
        ASSERT_EQ("\n", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 1));
        ASSERT_EQ("H", result);
        ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 12));
        ASSERT_EQ("ello, flume!", result);
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLineWithLineEnding(&result));
        ASSERT_TRUE(read_file->IsEof());
        ASSERT_FALSE(read_file->ReadLineWithLineEnding(&result));
    }
}

TEST(GZipFile, Tell) {
    toft::File *file = toft::File::Open("testdata/gzip_file.data", "r");
    CHECK_NOTNULL(file);
    toft::scoped_ptr<toft::File> read_file(CompressionFileFactory::CompressionFile(file, "gz"));
    std::string result;
    ASSERT_FALSE(read_file->IsEof());
    ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result, 5));
    ASSERT_EQ(5, read_file->Tell());
    ASSERT_TRUE(read_file->ReadLine(&result));
    ASSERT_EQ(14, read_file->Tell());
    ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result));
    ASSERT_EQ(27, read_file->Tell());
    ASSERT_TRUE(read_file->ReadLineWithLineEnding(&result));
    ASSERT_EQ(40, read_file->Tell());
    ASSERT_TRUE(read_file->IsEof());
    ASSERT_EQ(40, read_file->Tell());
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
