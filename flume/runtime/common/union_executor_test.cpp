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

#include "flume/runtime/common/union_executor.h"

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/fake_pointer.h"
#include "flume/runtime/testing/mock_listener.h"
#include "flume/runtime/testing/mock_source.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::InSequence;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

using core::Entity;
using core::Objector;

static const char* kOutput = "fake_output_id";
static const unsigned kScopeLevel = 1;

class UnionExecutorTest : public ::testing::Test {
protected:
    typedef ExecutorRunnerWrapper<UnionRunner> UnionExecutor;

    virtual void SetUp() {
        m_message = ConstructExecutorMessage();
    }

    MockSource& AddInput(const std::string& id) {
        m_message.add_input(id);

        PbUnionNode* node =
                m_message.mutable_logical_executor()->mutable_node()->mutable_union_node();
        node->add_from(id);

        m_deleter.push_back(new MockSource());
        m_inputs[id] = &m_deleter.back();
        return m_deleter.back();
    }

    UnionExecutor& StartTest() {
        m_executor.reset(new_executor_by_runner<UnionRunner>(
                kScopeLevel, m_message, std::vector<Executor*>(), &m_dataset_manager
        ));
        m_executor->Setup(m_inputs);
        return *m_executor;
    }

private:
    PbExecutor ConstructExecutorMessage() {
        PbExecutor message;
        message.set_type(PbExecutor::LOGICAL);
        message.set_scope_level(kScopeLevel);
        message.add_output(kOutput);
        message.add_dispatcher()->set_identity(kOutput);

        PbLogicalPlanNode* node = message.mutable_logical_executor()->mutable_node();
        node->set_id(kOutput);
        node->set_type(PbLogicalPlanNode::UNION_NODE);
        node->set_scope("");
        *node->mutable_objector() = Entity<Objector>::Of<FakeObjector>("").ToProtoMessage();

        return message;
    }

protected:
    boost::ptr_vector<MockSource> m_deleter;

    PbExecutor m_message;
    MemoryDatasetManager m_dataset_manager;
    std::map<std::string, Source*> m_inputs;
    boost::scoped_ptr<UnionExecutor> m_executor;
};

TEST_F(UnionExecutorTest, Union) {
    MockSource& input_1 = AddInput("input_1");
    EXPECT_CALL(input_1, RequireObject(_, _));

    MockSource& input_2 = AddInput("input_2");
    EXPECT_CALL(input_2, RequireObject(_, _));

    UnionExecutor& executor = StartTest();

    MockListener listener;
    listener.RequireObject(executor.GetSource(kOutput, 1));
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, GotObject(ObjectPtr(1)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(2)));
        EXPECT_CALL(listener, GotObject(ObjectPtr(3)));
        EXPECT_CALL(listener, GotDone());

        EXPECT_CALL(listener, GotDone());
    }

    {
        executor.BeginGroup("");

        executor.BeginGroup("key1");
        executor.FinishGroup();
        input_1.DispatchObject(ObjectPtr(1));
        input_2.DispatchObject(ObjectPtr(2));
        input_2.DispatchDone();
        input_1.DispatchObject(ObjectPtr(3));
        input_1.DispatchDone();

        executor.BeginGroup("key2");
        executor.FinishGroup();
        input_1.DispatchDone();
        input_2.DispatchDone();

        executor.FinishGroup();
    }
}

TEST_F(UnionExecutorTest, Cancel) {
    MockSource& input_1 = AddInput("input_1");
    EXPECT_CALL(input_1, RequireObject(_, _));

    MockSource& input_2 = AddInput("input_2");
    EXPECT_CALL(input_2, RequireObject(_, _));

    UnionExecutor& executor = StartTest();

    MockListener listener;
    Source::Handle* handle = listener.RequireObject(executor.GetSource(kOutput, 1));
    {
        InSequence in_sequence;

        // key1
        EXPECT_CALL(listener, GotDone());

        // // key2
        EXPECT_CALL(input_1, Done());
        EXPECT_CALL(input_2, Done());
        EXPECT_CALL(listener, GotDone());

        // key3
        EXPECT_CALL(listener, GotObject(ObjectPtr(2)))
            .WillOnce(InvokeWithoutArgs(handle, &Source::Handle::Done));
        EXPECT_CALL(input_1, Done());
        EXPECT_CALL(input_2, Done());
        EXPECT_CALL(listener, GotDone());
    }

    {
        executor.BeginGroup("");

        executor.BeginGroup("key1");
        handle->Done();
        executor.FinishGroup();
        input_1.DispatchDone();
        input_2.DispatchDone();

        executor.BeginGroup("key2");
        executor.FinishGroup();
        input_1.DispatchDone();
        handle->Done();
        input_2.DispatchObject(ObjectPtr(1));
        input_2.DispatchDone();

        executor.BeginGroup("key2");
        executor.FinishGroup();
        input_1.DispatchObject(ObjectPtr(2));
        input_1.DispatchDone();
        input_2.DispatchDone();

        executor.FinishGroup();
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
