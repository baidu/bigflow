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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>

#include "flume/runtime/common/fix_empty_group_executor.h"

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

#include "flume/proto/physical_plan.pb.h"

#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/mock_dispatcher.h"
#include "flume/runtime/testing/mock_executor_context.h"
#include "flume/runtime/testing/mock_source.h"

namespace baidu {
namespace flume {
namespace runtime {

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

TEST(TestFixEmptyGroupExecutor, TestCreate) {
    using testing::Return;
    using testing::_;
    MockExecutorContext context;
    MockSource source;
    MockDispatcher dispatcher;
    std::string input = "1";
    std::string output = "2";
    EXPECT_CALL(context, input(input))
           .WillOnce(Return(&source));
    EXPECT_CALL(context, output(output))
            .WillOnce(Return(&dispatcher));

    EXPECT_CALL(source, RequireStream(Source::REQUIRE_BINARY, _, _));

    CreateEmptyRecordExecutor creator;
    PbExecutor msg;
    msg.add_input(input);
    msg.mutable_create_empty_record_executor()->set_output(output);
    creator.setup(msg, &context);
    {
    testing::InSequence seq;
    EXPECT_CALL(dispatcher, EmitBinary("1"));
    EXPECT_CALL(dispatcher, EmitBinary("2"));
    creator.begin_group(ToPiece(Vector("ka", "kb")));
    source.DispatchBinary("1");
    source.DispatchBinary("2");
    creator.end_group();
    }

    {
    testing::InSequence seq;
    toft::StringPiece empty;
    EXPECT_CALL(dispatcher, EmitEmpty());
    creator.begin_group(ToPiece(Vector("ka", "kc")));
    creator.end_group();
    }

    {
    testing::InSequence seq;
    EXPECT_CALL(dispatcher, EmitBinary("1"));
    EXPECT_CALL(dispatcher, EmitBinary("2"));
    creator.begin_group(ToPiece(Vector("kb", "kb")));
    source.DispatchBinary("1");
    source.DispatchBinary("2");
    creator.end_group();
    }
}

TEST(TestFixEmptyGroupExecutor, TestFilter) {
    using testing::Return;
    using testing::_;
    MockExecutorContext context;
    MockSource source;
    MockDispatcher dispatcher;
    std::string input = "1";
    std::string output = "2";
    EXPECT_CALL(context, input(input))
           .WillOnce(Return(&source));
    EXPECT_CALL(context, output(output))
            .WillOnce(Return(&dispatcher));

    EXPECT_CALL(source, RequireStream(Source::REQUIRE_BINARY, _, _));

    FilterEmptyRecordExecutor filter;
    PbExecutor msg;
    msg.add_input(input);
    msg.mutable_filter_empty_record_executor()->set_output(output);
    filter.setup(msg, &context);
    {
    testing::InSequence seq;
    EXPECT_CALL(dispatcher, EmitBinary("1"));
    EXPECT_CALL(dispatcher, EmitBinary("2"));
    filter.begin_group(ToPiece(Vector("ka", "kb")));
    source.DispatchBinary("1");
    source.DispatchBinary("2");
    filter.end_group();
    }

    {
    testing::InSequence seq;
    toft::StringPiece empty;
    EXPECT_CALL(dispatcher, EmitBinary("1"));
    EXPECT_CALL(dispatcher, EmitBinary("2"));
    filter.begin_group(ToPiece(Vector("ka", "kc")));

    source.DispatchBinary(empty);
    source.DispatchBinary(empty);
    source.DispatchBinary("1");
    source.DispatchBinary("2");

    filter.end_group();
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
