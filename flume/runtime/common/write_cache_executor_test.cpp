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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "toft/base/array_size.h"
#include "toft/base/scoped_array.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/hash/city.h"
#include "wing/common/comparable.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/core/testing/string_objector.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/common/merge_shuffle_executor.h"
#include "libhce/Hce.hh"
#include "flume/runtime/executor.h"
#include "flume/runtime/source.h"

#include "flume/runtime/testing/mock_source.h"


namespace baidu {
namespace flume {
namespace runtime {

void PushBack(std::vector<std::string>* collection,
              void* object) {
    collection->push_back(*static_cast<std::string*>(object));
}

void Set(bool* done) {
    *done = true;
}

std::vector<std::string> Vector(const char* s1 = NULL,
                                const char* s2 = NULL,
                                const char* s3 = NULL,
                                const char* s4 = NULL) {
    std::vector<std::string> ret;
    const char* arr[] = {s1, s2, s3, s4};
    for (size_t i = 0; i != TOFT_ARRAY_SIZE(arr); ++i) {
        if (arr[i]) {
            ret.push_back(arr[i]);
        }
    }
    return ret;
}

std::vector<toft::StringPiece> ToPiece(const std::vector<std::string>& strs) {
    std::vector<toft::StringPiece> ret(strs.begin(), strs.end());
    return ret;
}

MergeShuffleRecord Record(const std::vector<toft::StringPiece>& keys,
                          const toft::StringPiece& value) {
    MergeShuffleRecord record;
    record.key_ptr = &(*keys.begin());
    record.key_end = record.key_ptr + keys.size();
    record.value = value;
    return record;
}

std::string Serialize(std::string value) {
    StringObjector objector;
    string buffer(1000, 0);
    buffer.resize(objector.Serialize(&value, &*buffer.begin(), buffer.size()));
    return buffer;
}

TEST(TestShuffleDecoderr, TestCase1) {
    ShuffleDecoder decoder;
    PbDceTask::ShuffleDecoder pb_decoder;
    pb_decoder.set_id("decoder_id");
    StringObjector objector;
    pb_decoder.mutable_objector()->CopyFrom(
            core::Entity<core::Objector>::Of<StringObjector>("").ToProtoMessage());
    pb_decoder.set_source("decode_source");

    MemoryDatasetManager memory_dataset_manager;
    PbExecutor pb_message;
    pb_message.set_scope_level(3);
    decoder.Initialize(pb_decoder, &memory_dataset_manager,
                       pb_message, std::vector<Executor*>());
    std::map<std::string, Source*> sources;
    MockSource input_source;
    sources["input_source"] = &input_source;
    decoder.Setup(sources);

    std::vector<std::string> result;
    bool done = false;
    Source* source = decoder.GetSource("decode_source", 1);
    source->RequireObject(toft::NewPermanentClosure(&PushBack, &result),
                          toft::NewPermanentClosure(&Set, &done));
    decoder.BeginGroup("");
    decoder.BeginGroup("");
    decoder.BeginGroup("");
    decoder.BeginGroup("");
    decoder.FinishGroup();
    input_source.Dispatch(Record(ToPiece(Vector("key1", "key2")), Serialize("v1")));
    input_source.Dispatch(Record(ToPiece(Vector("key1", "key2")), Serialize("v2")));
    input_source.Dispatch(Record(ToPiece(Vector("key1", "key2")), Serialize("v3")));
    EXPECT_FALSE(done);
    input_source.DispatchDone();
    EXPECT_FALSE(done);
    decoder.FinishGroup();
    EXPECT_FALSE(done);
    decoder.FinishGroup();
    EXPECT_TRUE(done);
    decoder.FinishGroup();
    EXPECT_TRUE(done);
    EXPECT_EQ(Vector("v1", "v2", "v3"), result);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
