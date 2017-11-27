/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//         Zhou Kai <bigflow-opensource@baidu.com>

#include "flume/runtime/io/io_format.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_array.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/partitioner.h"
#include "flume/proto/entity.pb.h"

using ::testing::_;
using ::testing::Return;

namespace baidu {
namespace flume {
namespace runtime {

class FakeCoreEmitter : public core::Emitter {
public:
    virtual bool Emit(void *object) {
        return true;
    }

    virtual void Done() {}
};

class LongLineReadTestEmitter : public core::Emitter {
public:
    LongLineReadTestEmitter() : lines_num(0u) {}

    virtual bool Emit(void *object) {
        toft::StringPiece value = static_cast<Record*>(object)->value;
        EXPECT_EQ(value.length(), 103604u);
        ++lines_num;
        return true;
    }

    virtual void Done() {
        EXPECT_EQ(lines_num, 1u);
    }

private:
    size_t lines_num;
};

TEST(IoFormat, InputFormat) {
    FakeCoreEmitter emitter;
    LongLineReadTestEmitter test_long_line_emitter;
    TextInputFormat text;
    std::vector<std::string> splits;
    text.Setup("");
    text.Split("testdata", &splits);
    text.Load("testdata/text_input_format.data", &emitter);
    text.Load("testdata/text_input_format_1_long_line.data", &test_long_line_emitter);

    SequenceFileAsBinaryInputFormat sequence_file;
    sequence_file.Setup("");
    sequence_file.Split("testdata", &splits);
    sequence_file.Load("testdata/sequence_input_format.data", &emitter);
}

TEST(IoFormat, TextOutputFormat) {
    // Only test local
    // std::string file_path = "hdfs://fake_path/fake";
    std::string file_path = "fake_path/fake";
    system(("mkdir -p " + file_path).c_str());

    Record record;
    record.key = "key";
    record.value = "value";

    std::string encoded_partition = core::EncodePartition(2);
    std::vector<toft::StringPiece> keys;
    keys.push_back("worker");
    keys.push_back(encoded_partition);

    std::string config;
    PbOutputFormatEntityConfig config_pb;
    config_pb.set_overwrite(true);
    config_pb.set_path(file_path);
    ASSERT_TRUE(config_pb.SerializeToString(&config));

    TextOutputFormat output;
    output.Setup(config);
    output.Open(keys);
    output.Sink(&record);
    output.Close();
}

TEST(IoFormat, RecordObjector) {
    RecordObjector objector;
    objector.Setup("");

    Record record;
    record.key = "key";
    record.value = "value";

    size_t buffer_size = objector.Serialize(&record, NULL, 0);
    toft::scoped_array<char> buffer(new char[buffer_size]);
    ASSERT_EQ(buffer_size, objector.Serialize(&record, buffer.get(), buffer_size));

    Record* output = static_cast<Record*>(objector.Deserialize(buffer.get(), buffer_size));
    ASSERT_EQ(record.key, output->key);
    ASSERT_EQ(record.value, output->value);

    objector.Release(output);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
