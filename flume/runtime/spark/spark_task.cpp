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
// Author: Wang Cong <wangcong09@baidu.com>

#include "flume/runtime/spark/spark_task.h"

#include <functional>
#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "gflags/gflags.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/runtime/spark/input_executor.h"
#include "flume/runtime/spark/kv_buffer.h"
#include "flume/runtime/spark/shuffle_input_executor.h"
#include "flume/runtime/spark/shuffle_output_executor.h"
#include "flume/runtime/spark/shuffle_protocol.h"
#include "flume/runtime/spark/spark_executor_factory.h"
#include "flume/runtime/spark/spark_cache_manager.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

DEFINE_int32(flume_spark_task_index, -1, "id of task which to be executed");
DEFINE_int32(flume_spark_max_memory_metabytes, 256, "max used memory size in worker side");
DEFINE_int32(flume_spark_kvbuffer_size, 8 * 1024 * 1024, "Initial memory size of KVBuffer");
DEFINE_string(flume_spark_partition_prefix, "", "prefix of spark partition info");

SparkTask::SparkTask(
        const PbSparkJob::PbSparkJobInfo& pb_job_info,
        const PbSparkTask& pb_spark_task,
        bool is_use_pipe,
        uint32_t partition):
    _is_use_pipe(is_use_pipe),
    _type(pb_spark_task.type()),
    _do_cpu_profile(pb_spark_task.do_cpu_profile()),
    _do_heap_profile(pb_spark_task.do_heap_profile()) {

    _buffer.reset(new KVBuffer(FLAGS_flume_spark_kvbuffer_size));

    _emitter.reset(toft::NewPermanentClosure(this, &SparkTask::emit));
    // noted: Spark could execute multiple tasks in one executor under the same workspace.
    // partition would be collided with merged tasks or speculative tasks. Use attempt_id instead
    std::string attempt_id = pb_spark_task.runtime_task_ctx().task_attempt_id();
    // Multiple flume tasks could be executed in the same spark task. For example, cached rdd has
    // new transformation defined, normally the cached rdd would not be computed as we have cached
    // the result. However if the cache result is evicted, then spark will try to re-compute the
    // cached rdd which has flume task involved, plus new transformation(another flume task
    // involved). To avoid collision, we append uuid to .swap.$attempt_id.
    std::string uuid = toft::CreateCanonicalUUIDString();
    LOG(INFO) << "Initialize with .swap, attempt id: " << attempt_id << ", uuid: " << uuid;
    _dataset_manager.Initialize(
            ".swap." + attempt_id + "_" + uuid,
            FLAGS_flume_spark_max_memory_metabytes * 1024 * 1024);
    using std::placeholders::_1;
    using std::placeholders::_2;
    SparkCacheManager::Emitter emit_fn = std::bind(&SparkTask::emit, this, _1, _2);
    _cache_manager.reset(new SparkCacheManager(emit_fn, pb_job_info));

    SparkExecutorFactory factory(
            pb_spark_task,
            partition,
            _cache_manager.get(),
            &_dataset_manager,
            _emitter.get());

    switch (_type) {
        case PbSparkTask::INPUT:
            _input_executor = factory.initialize_for_input_rdd();
            break;
        case PbSparkTask::GENERAL:
            _shuffle_input_executor = factory.initialize_for_general_rdd();
            break;
        default:
            LOG(FATAL) << "Wrong type";
    }

    _task.Initialize(pb_spark_task.root(), &factory);
    factory.check_invalid();
}

SparkTask::~SparkTask() {
    LOG(INFO) << "Deconstructing SparkTask";
}

bool SparkTask::do_cpu_profile() {
    return _do_cpu_profile;
}

bool SparkTask::do_heap_profile() {
    return _do_heap_profile;
}

bool SparkTask::run(const std::string& info) {
    _task.Run(info);
    return true;
}

InputExecutor* SparkTask::input_executor() {
    if (_type == PbSparkTask::INPUT) {
        return _input_executor;
    } else {
        LOG(FATAL) << "Wrong type, only INPUT task can have HadoopInputExecutor";
    }
}

ShuffleInputExecutor* SparkTask::shuffle_input_executor() {
    if (_type == PbSparkTask::GENERAL) {
        return _shuffle_input_executor;
    } else {
        LOG(FATAL) << "Wrong type, only GENERAL task can have HadoopInputExecutor";
    }
}

void SparkTask::input_done() {
    switch (_type) {
        case PbSparkTask::INPUT:
            _input_executor->input_done();
            break;
        case PbSparkTask::GENERAL:
            _shuffle_input_executor->input_done();
            break;
        default:
            LOG(FATAL) << "Wrong type";
    }
}

KVBuffer* SparkTask::get_output_buffer() {
    return _buffer.get();
}

void SparkTask::emit(const toft::StringPiece& key, const toft::StringPiece& value) {
    if (_is_use_pipe) {
        size_t size = value.size();
        CHECK_EQ(size, fwrite(value.data(), 1, size, stderr));
    } else {
        _buffer->put(key, value);
    }
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

