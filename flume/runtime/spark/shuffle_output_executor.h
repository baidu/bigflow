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
//         Zhang Yuncong <zhangyuncong@baidu.com>
//         Pan Yuchang <panyuchang@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>
//

#ifndef FLUME_RUNTIME_SPARK_SHUFFLE_OUTPUT_EXECUTOR_H
#define FLUME_RUNTIME_SPARK_SHUFFLE_OUTPUT_EXECUTOR_H

#include <map>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/scoped_ptr.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/spark/spark_task.h"
#include "flume/runtime/spark/shuffle_protocol.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

class TransferEncoder;

namespace spark {

class ShuffleOutputRunner : public ExecutorRunner {
public:
    ShuffleOutputRunner();
    virtual ~ShuffleOutputRunner();

    void initialize(const PbSparkTask::PbShuffleOutput& message, SparkTask::EmitCallback* emitter);

    virtual void setup(const PbExecutor& message, ExecutorContext* context);

    // called when all inputs is ready
    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    // called after all inputs is done.
    virtual void end_group();

private:
    typedef PbSparkTask::PbShuffleOutput::Channel PbChannel;
    static const uint32_t kAvailableBufferSize = 32 * 1024;  /* 32K is enough for most keys */
    static const int32_t kMaxSignedInteger32 = 0x7fffffff;
    static const int64_t kBitMasker = 0x00000000ffffffffL;

    class RecordWriter;
    class GroupRecordWriter;
    class BucketRecordWriter;
    class BroadcastRecordWriter;
    class DefaultRecordWriter;

    void write_record(TransferEncoder* encoder, RecordWriter* writer,
                      const std::vector<toft::StringPiece>& keys,
                      void* object, const toft::StringPiece& binary);

    char _default_buffer[kAvailableBufferSize + sizeof(ShuffleHeader)];

    PbSparkTask::PbShuffleOutput _message;
    SparkTask::EmitCallback* _emitter;
    boost::ptr_vector<TransferEncoder> _encoders;
    boost::ptr_vector<RecordWriter> _writers;
};

Executor* new_shuffle_output_executor(
        const PbSparkTask::PbShuffleOutput& config,
        SparkTask::EmitCallback* emitter,
        uint32_t input_scope_level,
        const PbExecutor& executor,
        const std::vector<Executor*>& childs,
        DatasetManager* dataset_manager);

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SHUFFLE_OUTPUT_EXECUTOR_H
