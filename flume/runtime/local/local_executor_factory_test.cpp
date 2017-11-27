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

#include "flume/runtime/local/local_executor_factory.h"

#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"
#include "flume/core/loader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/task_executor.h"
#include "flume/runtime/local/local_backend.h"
#include "flume/runtime/local/local_input_executor.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace local {

using core::Entity;
using core::Loader;

const std::vector<Executor*> kEmptyChilds;

TEST(LocalExecutorFactoryTest, LocalInputExecutor) {
    PbLocalJob job;
    PbLocalInput* input = job.add_input();
    input->set_id("fake_id");
    *input->mutable_spliter() = Entity<Loader>::Of<MockLoader>("").ToProtoMessage();
    LocalBackend backend;
    LocalExecutorFactory factory(&backend);
    factory.Initialize(job, NULL);

    PbExecutor message;
    message.set_type(PbExecutor::EXTERNAL);
    message.mutable_external_executor()->set_id("fake_id");
    toft::scoped_ptr<Executor> executor(factory.CreateExecutor(message, 0, kEmptyChilds));
    EXPECT_TRUE(dynamic_cast<LocalInputExecutor*>(executor.get()) != NULL);
}

TEST(LocalExecutorFactoryTest, TaskExecutor) {
    PbLocalJob job;
    PbLocalInput* input = job.add_input();
    input->set_id("fake_id");
    *input->mutable_spliter() = Entity<Loader>::Of<MockLoader>("").ToProtoMessage();
    LocalBackend backend;
    LocalExecutorFactory factory(&backend);
    factory.Initialize(job, NULL);

    PbExecutor message;
    message.set_type(PbExecutor::TASK);
    message.mutable_external_executor()->set_id("fake_id");
    toft::scoped_ptr<Executor> executor(factory.CreateExecutor(message, 0, kEmptyChilds));
    EXPECT_TRUE(dynamic_cast<TaskExecutor*>(executor.get()) != NULL);
}

}  // namespace local
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
