/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#ifndef FLUME_RUNTIME_SPARK_SPARK_EXECUTOR_FACTORY_H_
#define FLUME_RUNTIME_SPARK_SPARK_EXECUTOR_FACTORY_H_

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/shared_ptr.hpp"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/executor_factory.h"
#include "flume/runtime/spark/spark_task.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class HadoopInputExecutor;
class ShuffleInputExecutor;
class ShuffleOutputExecutor;
class CacheInputExecutor;

class SparkExecutorFactory : public ExecutorFactory {
public:
    explicit SparkExecutorFactory(
            const PbSparkTask& message,
            uint32_t partition,
            CacheManager* cache_manager,
            DatasetManager* dataset_manager,
            SparkTask::EmitCallback* emitter);
    virtual ~SparkExecutorFactory();

    virtual Executor* CreateExecutor(const PbExecutor& message, uint32_t scope_level,
                                     const std::vector<Executor*>& childs);
public:
    InputExecutor* initialize_for_input_rdd();

    ShuffleInputExecutor* initialize_for_general_rdd();

    void check_invalid();

private:
    typedef toft::Closure<
        void (const PbExecutor& message,  // NOLINT(whitespace/parens)
              const std::vector<Executor*>& childs,
              DatasetManager* manager)
    > ExecutorInitializer;

    static Executor* run_and_return(ExecutorInitializer* closure, Executor* executor,
                                    uint32_t input_scope_level, const PbExecutor& message,
                                    const std::vector<Executor*>& childs,
                                    DatasetManager* dataset_manager) {
        closure->Run(message, childs, dataset_manager);
        return executor;
    }

    CacheInputExecutor* create_cache_input_executor();

    HadoopInputExecutor* create_hadoop_input_executor();

    ShuffleInputExecutor* create_shuffle_input_executor();

    ShuffleOutputExecutor* create_shuffle_output_executor();

    Executor* create_write_cache_executor(const PbExecutor& message, uint32_t scope_level,
                                          const std::vector<Executor*>& childs);

    void register_executor(const std::string& identity,
                           Executor* executor, ExecutorInitializer* initializer);

    void register_executor(const std::string& identity, ExecutorBuilder* builder);

    uint32_t _partition;
    CacheManager* _cache_manager;
    DatasetManager* _dataset_manager;
    SparkTask::EmitCallback* _emitter;
    PbSparkTask _task;

    std::map<std::string, ExecutorBuilder*> _executor_builders;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_SPARK_SPARK_EXECUTOR_FACTORY_H_
