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

#include "flume/runtime/executor_factory.h"

#include <vector>

#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/loader_executor.h"
#include "flume/runtime/common/partial_executor.h"
#include "flume/runtime/common/processor_executor.h"
#include "flume/runtime/common/shuffle_executor.h"
#include "flume/runtime/common/sinker_executor.h"
#include "flume/runtime/common/task_executor.h"
#include "flume/runtime/common/union_executor.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

Executor* CreateProcessorExecutor(
        const PbExecutor& message,
        uint32_t input_scope_level,
        const std::vector<Executor*>& childs,
        DatasetManager* dataset_manager) {
    return new_executor_by_runner<NormalProcessorRunner>(
            input_scope_level, message, childs, dataset_manager
    );
}

Executor* CreateLogicalExecutor(
        const PbExecutor& message,
        uint32_t input_scope_level,
        const std::vector<Executor*>& childs,
        DatasetManager* dataset_manager) {
    const PbLogicalPlanNode& node = message.logical_executor().node();
    switch (node.type()) {
        case PbLogicalPlanNode::UNION_NODE: {
            return new_executor_by_runner<UnionRunner>(
                    input_scope_level, message, childs, dataset_manager
            );
        }
        case PbLogicalPlanNode::LOAD_NODE: {
            toft::scoped_ptr<LoaderExecutor> executor(new LoaderExecutor());
            executor->Initialize(message, dataset_manager);
            return executor.release();
        }
        case PbLogicalPlanNode::SINK_NODE: {
            toft::scoped_ptr<SinkerExecutor> executor(new SinkerExecutor());
            executor->Initialize(message);
            return executor.release();
        }
        case PbLogicalPlanNode::PROCESS_NODE:
        default: {
            LOG(FATAL) << "illegal executor: " << message.DebugString();
            return NULL;
        }
    }
}

}  // namespace

Executor* ExecutorFactory::CreateCommonExecutor(const PbExecutor& message, uint32_t scope_level,
                                                const std::vector<Executor*>& childs,
                                                DatasetManager* dataset_manager) {
    switch (message.type()) {
        case PbExecutor::TASK: {
            toft::scoped_ptr<TaskExecutor> executor(new TaskExecutor());
            executor->Initialize(message, childs);
            return executor.release();
        }
        case PbExecutor::PROCESSOR: {
            LOG_IF(FATAL, !childs.empty())
                    << "illegal message (Executor for Processor must not have child): "
                    << message.DebugString();
            return CreateProcessorExecutor(message, scope_level, childs, dataset_manager);
        }
        case PbExecutor::LOGICAL: {
            LOG_IF(FATAL, !childs.empty())
                    << "illegal message (Executor for logicial plan node must not have child): "
                    << message.DebugString();
            return CreateLogicalExecutor(message, scope_level, childs, dataset_manager);
        }
        case PbExecutor::SHUFFLE: {
            return new_shuffle_executor(0, scope_level, message, childs, dataset_manager);
        }
        case PbExecutor::PARTIAL: {
            toft::scoped_ptr<PartialExecutor> executor(new PartialExecutor());
            executor->Initialize(message, childs, scope_level, dataset_manager);
            return executor.release();
        }
        default: {
            LOG(FATAL) << "illegal executor: " << message.DebugString();
            return NULL;
        }
    }
}

}   // namespace runtime
}   // namespace flume
}   // namespace baidu
