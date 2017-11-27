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

#ifndef FLUME_RUNTIME_SPARK_SPARK_TASK_H
#define FLUME_RUNTIME_SPARK_SPARK_TASK_H

#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/local_dataset.h"
#include "flume/runtime/spark/spark_cache_manager.h"
#include "flume/runtime/task.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class InputExecutor;
class ShuffleInputExecutor;
class SparkExecutorFactory;
class KVBuffer;

DECLARE_int32(flume_spark_kvbuffer_size);
DECLARE_int32(flume_spark_max_memory_metabytes);
DECLARE_int32(flume_spark_task_index);
DECLARE_string(flume_spark_partition_prefix);

class SparkTask {
public:
    typedef
        toft::Closure<void (const toft::StringPiece&, const toft::StringPiece&)>
        EmitCallback;

public:
    SparkTask(const PbSparkJob::PbSparkJobInfo& pb_job_info,
            const PbSparkTask& pb_spark_task,
            bool is_use_pipe,
            uint32_t partition);
    ~SparkTask();

    bool run(const std::string& info);

    InputExecutor* input_executor();

    ShuffleInputExecutor* shuffle_input_executor();

    void input_done();

    KVBuffer* get_output_buffer();

    bool do_cpu_profile();
    bool do_heap_profile();

private:
    void emit(const toft::StringPiece& key, const toft::StringPiece& value);

private:
    bool _is_use_pipe;
    LocalDatasetManager _dataset_manager;
    Task _task;
    PbSparkTask::Type _type;
    toft::scoped_ptr<EmitCallback> _emitter;
    toft::scoped_ptr<KVBuffer> _buffer;
    InputExecutor* _input_executor;
    ShuffleInputExecutor* _shuffle_input_executor;
    bool _do_cpu_profile;
    bool _do_heap_profile;
    toft::scoped_ptr<CacheManager> _cache_manager;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SPARK_TASK_H
