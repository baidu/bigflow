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

#include "flume/runtime/spark/spark_executor_factory.h"

#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/cache_manager.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/common/fix_empty_group_executor.h"
#include "flume/runtime/common/shuffle_executor.h"
#include "flume/runtime/common/write_cache_executor.h"
#include "flume/runtime/spark/cache_input_executor.h"
#include "flume/runtime/spark/hadoop_input_executor.h"
#include "flume/runtime/spark/shuffle_input_executor.h"
#include "flume/runtime/spark/shuffle_output_executor.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

SparkExecutorFactory::SparkExecutorFactory(
        const PbSparkTask& message,
        uint32_t partition,
        CacheManager* cache_manager,
        DatasetManager* dataset_manager,
        SparkTask::EmitCallback* emitter):
    _partition(partition),
    _cache_manager(cache_manager),
    _dataset_manager(dataset_manager),
    _emitter(emitter),
    _task(message) {
}

SparkExecutorFactory::~SparkExecutorFactory() {}

InputExecutor* SparkExecutorFactory::initialize_for_input_rdd() {
    create_shuffle_output_executor();
    if(_task.has_hadoop_input()) {
        return create_hadoop_input_executor();
    } else if (_task.has_cache_input()) {
        return create_cache_input_executor();
    }
    return NULL;
}

ShuffleInputExecutor* SparkExecutorFactory::initialize_for_general_rdd() {
    create_shuffle_output_executor();
    return create_shuffle_input_executor();
}

HadoopInputExecutor* SparkExecutorFactory::create_hadoop_input_executor() {
    const PbSparkTask::PbHadoopInput& message = _task.hadoop_input();
    CHECK(message.has_input_format());
    std::auto_ptr<HadoopInputExecutor> executor(new HadoopInputExecutor(message));
    register_executor(
            message.id(),
            executor.get(),
            toft::NewClosure(executor.get(), &HadoopInputExecutor::initialize));
    return executor.release();
}

CacheInputExecutor* SparkExecutorFactory::create_cache_input_executor() {
    const PbSparkTask::PbCacheInput& message = _task.cache_input();
    std::auto_ptr<CacheInputExecutor> executor(new CacheInputExecutor(message));
    register_executor(
            message.id(),
            executor.get(),
            toft::NewClosure(executor.get(), &CacheInputExecutor::initialize));
    return executor.release();
}

ShuffleInputExecutor* SparkExecutorFactory::create_shuffle_input_executor() {
    if (_task.has_shuffle_input()) {
        const PbSparkTask::PbShuffleInput& message = _task.shuffle_input();
        std::auto_ptr<ShuffleInputExecutor> executor(new ShuffleInputExecutor(message, 0));
        register_executor(
                message.id(),
                executor.get(),
                toft::NewClosure(executor.get(), &ShuffleInputExecutor::initialize));
        return executor.release();
    }
}

ShuffleOutputExecutor* SparkExecutorFactory::create_shuffle_output_executor() {
    if (_task.has_shuffle_output()) {
        const PbSparkTask::PbShuffleOutput& message = _task.shuffle_output();
        register_executor(message.id(), toft::NewClosure(
                new_shuffle_output_executor, _task.shuffle_output(), _emitter
        ));
    } else {
        LOG(INFO) << "No ShuffleOutput";
    }
}

Executor* SparkExecutorFactory::create_write_cache_executor(const PbExecutor& message, uint32_t scope_level,
                                                            const std::vector<Executor*>& childs) {
    toft::scoped_ptr<WriteCacheExecutor> executor_core(new WriteCacheExecutor(_cache_manager));
    CHECK(message.has_write_cache_executor());

    typedef ExecutorImpl<WriteCacheExecutor> Materlizer;
    toft::scoped_ptr<Materlizer> executor(new Materlizer(executor_core.release()));
    executor->Initialize(message, childs, scope_level, _dataset_manager);
    return executor.release();
}

void SparkExecutorFactory::check_invalid() {
    CHECK_EQ(0U, _executor_builders.size());
}

void SparkExecutorFactory::register_executor(const std::string& identity,
                                          Executor* executor,
                                          ExecutorInitializer* initializer) {
    register_executor(identity, toft::NewClosure(run_and_return, initializer, executor));
}

void SparkExecutorFactory::register_executor(const std::string& identity,
                                          ExecutorBuilder* builder) {
    CHECK_EQ(0u, _executor_builders.count(identity));
    _executor_builders[identity] = builder;
}

Executor* SparkExecutorFactory::CreateExecutor(const PbExecutor& message, uint32_t scope_level,
                                             const std::vector<Executor*>& childs) {
    Executor* result = NULL;
    switch (message.type()) {
        case PbExecutor::EXTERNAL:
            {
                const std::string& identity = message.external_executor().id();
                CHECK_EQ(_executor_builders.count(identity), 1);

                ExecutorBuilder* builder = _executor_builders[identity];
                _executor_builders.erase(identity);
                result = builder->Run(scope_level, message, childs, _dataset_manager);
            }
            break;
        case PbExecutor::SHUFFLE:
            // we need partition for create LocalDistributeExecutor
            LOG(INFO) << "Partition: " << _partition << ", partition_offset: " << _task.partition_offset();
            result = new_shuffle_executor(_partition - _task.partition_offset(), scope_level, message, childs, _dataset_manager);
            break;
        case PbExecutor::WRITE_CACHE:
            result = create_write_cache_executor(message, scope_level, childs);
            break;
        case PbExecutor::CREATE_EMPTY_RECORD:
            result = new_executor_by_runner<CreateEmptyRecordExecutor>(
                    scope_level,
                    message,
                    childs,
                    _dataset_manager);
            break;
        case PbExecutor::FILTER_EMPTY_RECORD:
            result = new_executor_by_runner<FilterEmptyRecordExecutor>(
                    scope_level,
                    message,
                    childs,
                    _dataset_manager);
            break;
        default:
            result = ExecutorFactory::CreateCommonExecutor(message, scope_level, childs, _dataset_manager);
    }
    CHECK_NOTNULL(result);
    return result;
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
