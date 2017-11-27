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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/local/local_input_executor.h"

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Loader;

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::Sequence;

class MockExecutor: public Executor {
public:
    virtual ~MockExecutor() {}

    MOCK_METHOD1(Setup, void (const std::map<std::string, Source*>&)); // NOLINT
    MOCK_METHOD2(GetSource, Source* (const std::string&, unsigned));   // NOLINT
    MOCK_METHOD1(BeginGroup, void (const toft::StringPiece&));         // NOLINT
    MOCK_METHOD0(FinishGroup, void ());                                // NOLINT
};

PbLocalInput ConstructLocalInputMessage() {
    PbLocalInput message;
    message.set_id("test_local_input");
    *message.mutable_spliter() = Entity<Loader>::Of<MockLoader>("spliter").ToProtoMessage();
    message.add_uri("uri1");
    message.add_uri("uri2");
    return message;
}

PbExecutor ConstructExecutorMessage() {
    PbExecutor message;
    message.set_type(PbExecutor::EXTERNAL);
    message.set_scope_level(1);
    message.add_output("output");

    PbExecutor* child = message.add_child();
    child->set_type(PbExecutor::TASK);
    child->set_scope_level(1);
    child->add_output("output");

    return message;
}

TEST(LocalInputExecutorTest, Test) {
    Source* const kSourcePtr = reinterpret_cast<Source*>(0x123456);

    MockLoader& spliter = MockLoader::Mock("spliter");
    {
        std::vector<std::string> splits1;
        splits1.push_back("1");
        splits1.push_back("2");
        EXPECT_CALL(spliter, Split("uri1", _))
            .WillOnce(SetArgPointee<1>(splits1));

        std::vector<std::string> splits2;
        splits2.push_back("3");
        EXPECT_CALL(spliter, Split("uri2", _))
            .WillOnce(SetArgPointee<1>(splits2));
    }

    MockExecutor child;
    {
        Sequence s1, s2, s3;

        EXPECT_CALL(child, Setup(_))
            .InSequence(s1, s2, s3);
        EXPECT_CALL(child, GetSource("output", 0))
            .InSequence(s1, s2, s3)
            .WillOnce(Return(kSourcePtr));
        EXPECT_CALL(child, BeginGroup(toft::StringPiece("")))
            .InSequence(s1, s2, s3);

        EXPECT_CALL(child, BeginGroup(toft::StringPiece("1")))
            .InSequence(s1);
        EXPECT_CALL(child, FinishGroup())
            .InSequence(s1)
            .RetiresOnSaturation();

        EXPECT_CALL(child, BeginGroup(toft::StringPiece("2")))
            .InSequence(s2);
        EXPECT_CALL(child, FinishGroup())
            .InSequence(s2)
            .RetiresOnSaturation();

        EXPECT_CALL(child, BeginGroup(toft::StringPiece("3")))
            .InSequence(s3);
        EXPECT_CALL(child, FinishGroup())
            .InSequence(s3)
            .RetiresOnSaturation();

        EXPECT_CALL(child, FinishGroup())
            .InSequence(s1, s2, s3);
    }

    LocalInputExecutor executor;
    executor.Initialize(ConstructLocalInputMessage(),
                        ConstructExecutorMessage(),
                        std::vector<Executor*>(1, &child));

    executor.Setup(std::map<std::string, Source*>());
    ASSERT_EQ(kSourcePtr, executor.GetSource("output", 0));
    executor.BeginGroup("");
    executor.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
