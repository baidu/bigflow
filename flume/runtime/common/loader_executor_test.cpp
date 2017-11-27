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

#include "flume/runtime/common/loader_executor.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/emitter.h"
#include "flume/core/entity.h"
#include "flume/core/loader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/proto/split.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/fake_pointer.h"
#include "flume/runtime/testing/mock_listener.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

using core::Entity;
using core::Objector;
using core::Loader;

const std::map<std::string, Source*> kEmptySources;
static const char* kOutput = "fake_output_id";

PbExecutor ConstructExecutorMessage() {
    PbExecutor message;
    message.set_type(PbExecutor::LOGICAL);
    message.set_scope_level(1u);
    message.add_output(kOutput);
    message.add_dispatcher()->set_identity(kOutput);

    PbLogicalPlanNode* node = message.mutable_logical_executor()->mutable_node();
    node->set_id(kOutput);
    node->set_type(PbLogicalPlanNode::LOAD_NODE);
    node->set_scope("fake_scope");
    *node->mutable_objector() =
            Entity<Objector>::Of<FakeObjector>("").ToProtoMessage();
    *node->mutable_load_node()->mutable_loader() =
            Entity<Loader>::Of<MockLoader>("loader").ToProtoMessage();

    return message;
}

TEST(LoaderExecutorTest, Load) {
    MemoryDatasetManager dataset_manager;

    LoaderExecutor executor;
    executor.Initialize(ConstructExecutorMessage(), &dataset_manager);
    executor.Setup(kEmptySources);

    MockLoader& loader = MockLoader::Mock("loader");
    MockListener listener;
    Source::Handle* handle = listener.RequireObject(executor.GetSource(kOutput, 1));
    {
        InSequence in_sequence;

        EXPECT_CALL(loader, Load("s1"))
            .WillOnce(DoAll(EmitAndExpect(&loader, ObjectPtr(1), true),
                            EmitAndExpect(&loader, ObjectPtr(2), false)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(1)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(2)))
            .WillOnce(InvokeWithoutArgs(handle, &Source::Handle::Done));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(loader, Load("s2"))
            .WillOnce(DoAll(EmitAndExpect(&loader, ObjectPtr(3), true),
                            EmitDone(&loader),
                            EmitAndExpect(&loader, ObjectPtr(4), false)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(3)));
        EXPECT_CALL(listener, GotDone());
    }

    executor.BeginGroup("");
    std::string splits[] = {"s1", Loader::EncodeSplit("s2").SerializeAsString() };
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(splits); ++i) {
        executor.BeginGroup(splits[i]);
        executor.FinishGroup();
    }
    executor.FinishGroup();
}

TEST(LoaderExecutorTest, SkipUnusedSplit) {
    MemoryDatasetManager dataset_manager;

    LoaderExecutor executor;
    executor.Initialize(ConstructExecutorMessage(), &dataset_manager);
    executor.Setup(kEmptySources);

    MockLoader& loader = MockLoader::Mock("loader");
    MockListener listener;
    Source::Handle* handle = listener.RequireObject(executor.GetSource(kOutput, 0));
    {
        InSequence in_sequence;

        EXPECT_CALL(loader, Load("s1"))
            .WillOnce(DoAll(EmitAndExpect(&loader, ObjectPtr(1), true),
                            EmitAndExpect(&loader, ObjectPtr(2), false)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(1)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(2)))
            .WillOnce(InvokeWithoutArgs(handle, &Source::Handle::Done));
        EXPECT_CALL(listener, GotDone());
    }

    executor.BeginGroup("");
    std::string splits[] = {"s1", "s2" };
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(splits); ++i) {
        executor.BeginGroup(splits[i]);
        executor.FinishGroup();
    }
    executor.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
