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

#include "flume/runtime/common/sinker_executor.h"

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/sinker.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_pointer.h"
#include "flume/runtime/testing/mock_source.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::InSequence;

using core::Entity;
using core::Sinker;

PbExecutor ConstructExecutorMessage() {
    PbExecutor message;
    message.set_type(PbExecutor::LOGICAL);
    message.set_scope_level(0u);
    message.add_input("fakeid");

    PbLogicalPlanNode* node = message.mutable_logical_executor()->mutable_node();
    node->set_id("");
    node->set_type(PbLogicalPlanNode::SINK_NODE);
    node->set_scope("");
    node->mutable_sink_node()->set_from("fakeid");
    *node->mutable_sink_node()->mutable_sinker() =
            Entity<Sinker>::Of<MockSinker>("sinker").ToProtoMessage();

    return message;
}

TEST(SinkExecutorTest, Test) {
    MockSource source;
    MockSinker& sinker = MockSinker::Mock("sinker");
    {
        EXPECT_CALL(source, RequireObject(_, _));

        EXPECT_CALL(sinker, Open(ElementsAre("")));
        EXPECT_CALL(sinker, Sink(ObjectPtr(1)));
        EXPECT_CALL(sinker, Sink(ObjectPtr(2)));
        EXPECT_CALL(sinker, Close());
    }

    SinkerExecutor executor;
    executor.Initialize(ConstructExecutorMessage());

    std::map<std::string, Source*> sources;
    sources["fakeid"] = &source;
    executor.Setup(sources);

    executor.BeginGroup("");
    executor.FinishGroup();
    source.DispatchObject(ObjectPtr(1));
    source.DispatchObject(ObjectPtr(2));
    source.DispatchDone();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
