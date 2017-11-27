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
//
// Interface, basic excecution unit in flume runtime.

#ifndef FLUME_RUNTIME_EXECUTOR_H_
#define FLUME_RUNTIME_EXECUTOR_H_

#include <map>
#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

namespace baidu {
namespace flume {

class PbExecutor;

namespace runtime {

class DatasetManager;
class Dispatcher;
class ExecutorContext;
class Source;

struct PriorityDispatcher;

// Executor is used to drive the execution of logical plan, for both user and framework
// logics.
class Executor {
public:
    virtual ~Executor() {}

    // called father executor.
    // used to pass external inputs of this Executor.
    virtual void Setup(const std::map<std::string, Source*>& sources) = 0;

    // queryed by father executor.
    // return an Source object, which represents output of this Executor
    virtual Source* GetSource(const std::string& identity, unsigned scope_level) = 0;

    // informed by father executor.
    // When BeginGroup is called, all depending sources are in the same group.
    // All resources related to previous group can be released
    virtual void BeginGroup(const toft::StringPiece& key) = 0;

    // informed by father executor.
    // When FinishGroup is call, all depended source-listeners are in the same group.
    virtual void FinishGroup() = 0;
};

// An simplized interface for implementing executors. The calling convention of
// BeginGroup/EndGroup are same as in Processor.
// Details such as driving child executors are handled by different ExecutorImpl. We can
// call NewExecutorByRunner to create an instance of Executor for a specific
// ExecutorRunner.
class ExecutorRunner {
public:
    virtual ~ExecutorRunner() {}

    virtual void setup(const PbExecutor& message, ExecutorContext* context) = 0;

    // called when all inputs is ready
    virtual void begin_group(const std::vector<toft::StringPiece>& keys) = 0;

    // called after all inputs is done.
    virtual void end_group() = 0;
};


class ExecutorContext {
public:
    virtual ~ExecutorContext() {}

    virtual Source* input(const std::string& identity) = 0;

    virtual Dispatcher* output(const std::string& identity) = 0;

    virtual PriorityDispatcher priority_output(const std::string& identity) = 0;

    virtual void close_prior_outputs(int priority) = 0;

    virtual void begin_sub_group(const toft::StringPiece& key) = 0;

    virtual void end_sub_group() = 0;
};

template<typename RunnerType>
class ExecutorRunnerWrapper : public Executor {
public:
    virtual RunnerType* runner() = 0;
};

// include "flume/runtime/common/executor_impl.h" for definition.
// use template for performance considerations
template<typename RunnerType> ExecutorRunnerWrapper<RunnerType>*
new_executor_by_runner(uint32_t input_scope_level, const PbExecutor& message,
                       const std::vector<Executor*>& childs, DatasetManager* dataset_manager);

typedef toft::Closure<
    Executor* (uint32_t input_scope_level, const PbExecutor& message,
               const std::vector<Executor*>& childs, DatasetManager* dataset_manager)
> ExecutorBuilder;

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_EXECUTOR_H_
