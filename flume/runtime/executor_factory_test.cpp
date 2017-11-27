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

#include "flume/runtime/executor_factory.h"

#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"
#include "flume/core/logical_plan.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/loader_executor.h"
#include "flume/runtime/common/processor_executor.h"
#include "flume/runtime/common/sinker_executor.h"
#include "flume/runtime/common/task_executor.h"
#include "flume/runtime/common/union_executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Loader;
using core::Sinker;
using core::Processor;
using core::Objector;
using core::KeyReader;

const std::vector<Executor*> kEmptyChilds;

template<typename T>
void CreateAndCheck(const PbExecutor& message, const std::vector<Executor*>& childs) {
    toft::scoped_ptr<Executor> executor(
            ExecutorFactory::CreateCommonExecutor(message, 0, childs, NULL));
    EXPECT_TRUE(dynamic_cast<T*>(executor.get()) != NULL);
}

TEST(ExecutorFactory, DefaultExecutor) {
    PbExecutor message;
    message.set_type(PbExecutor::TASK);
    message.set_scope_level(0);

    CreateAndCheck<TaskExecutor>(message, kEmptyChilds);
}

PbExecutor BasicMessageForLogicalExecutor(PbLogicalPlanNode::Type type) {
    PbExecutor message;
    message.set_type(PbExecutor::LOGICAL);
    message.set_scope_level(0);

    PbLogicalPlanNode* node = message.mutable_logical_executor()->mutable_node();
    node->set_id("fake_id");
    node->set_type(type);
    node->set_scope("");
    *node->mutable_objector() = Entity<Objector>::Of<MockObjector>("").ToProtoMessage();

    return message;
}

PbExecutor BasicMessageForProcessorExecutor() {
    PbExecutor message;
    message.set_type(PbExecutor::PROCESSOR);
    message.set_scope_level(0);
    message.mutable_processor_executor();
    return message;
}

TEST(ExecutorFactory, UnionExecutor) {
    typedef ExecutorRunnerWrapper<UnionRunner> UnionExecutor;

    PbExecutor message = BasicMessageForLogicalExecutor(PbLogicalPlanNode::UNION_NODE);
    CreateAndCheck<UnionExecutor>(message, kEmptyChilds);
}

TEST(ExecutorFactory, LoadExecutor) {
    PbExecutor message = BasicMessageForLogicalExecutor(PbLogicalPlanNode::LOAD_NODE);
    PbLoadNode* load_node =
            message.mutable_logical_executor()->mutable_node()->mutable_load_node();
    *load_node->mutable_loader() = Entity<Loader>::Of<MockLoader>("").ToProtoMessage();

    CreateAndCheck<LoaderExecutor>(message, kEmptyChilds);
}

TEST(ExecutorFactory, SinkerExecutor) {
    PbExecutor message = BasicMessageForLogicalExecutor(PbLogicalPlanNode::SINK_NODE);
    PbSinkNode* sink_node =
            message.mutable_logical_executor()->mutable_node()->mutable_sink_node();
    *sink_node->mutable_sinker() = Entity<Sinker>::Of<MockSinker>("").ToProtoMessage();

    CreateAndCheck<SinkerExecutor>(message, kEmptyChilds);
}

TEST(ExecutorFactory, NormalProcessorExecutor) {
    typedef ExecutorRunnerWrapper<NormalProcessorRunner> NormalProcessorExecutor;

    PbExecutor message = BasicMessageForProcessorExecutor();
    CreateAndCheck<NormalProcessorExecutor>(message, kEmptyChilds);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
