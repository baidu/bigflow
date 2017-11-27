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
//         Zhang Yuncong <bigflow-opensource@baidu.com>
//         Pan Yuchang <bigflow-opensource@baidu.com>
//         Wen Xiang <bigflow-opensource@baidu.com>
//

#include "flume/runtime/spark/shuffle_output_executor.h"

#include <algorithm>
#include <iterator>
#include <vector>

#include "boost/scoped_array.hpp"
#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"
#include "toft/hash/city.h"

#include "flume/core/partitioner.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/common/transfer_encoding.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class ShuffleOutputRunner::RecordWriter {
public:
    RecordWriter(const PbChannel& config, SparkTask::EmitCallback* emitter) :
            _task_index(config.task_index()),
            _task_concurrency(config.task_concurrency()),
            _task_offset(config.task_offset()),
            _emitter(emitter) {}

    virtual ~RecordWriter() {}
    virtual void write(const std::vector<toft::StringPiece>& keys,
                       char* key_buffer, uint32_t key_buffer_size,
                       const toft::StringPiece& value) = 0;

protected:
    const uint32_t _task_index;
    const uint32_t _task_concurrency;
    const uint32_t _task_offset;
    SparkTask::EmitCallback* _emitter;
};

class ShuffleOutputRunner::GroupRecordWriter : public RecordWriter {
public:
    GroupRecordWriter(const PbChannel& config,
                      SparkTask::EmitCallback* emitter) : RecordWriter(config, emitter) {}

    virtual uint64_t hashcode(const std::vector<toft::StringPiece>& keys) {
        return toft::CityHash64(keys[1].data(), keys[1].size());
    }

    virtual void write(const std::vector<toft::StringPiece>& keys,
                       char* key_buffer, uint32_t key_buffer_size,
                       const toft::StringPiece& value) {
        uint64_t hash = hashcode(keys);
        uint32_t partition = hash % _task_concurrency + _task_offset;

        ShuffleHeader* header = reinterpret_cast<ShuffleHeader*>(key_buffer);
        header->set(_task_index, partition, false);

        _emitter->Run(toft::StringPiece(key_buffer, key_buffer_size), value);
    }
};

class ShuffleOutputRunner::BucketRecordWriter : public RecordWriter {
public:
    BucketRecordWriter(const PbChannel& config,
                       SparkTask::EmitCallback* emitter) : RecordWriter(config, emitter) {}

    virtual void write(const std::vector<toft::StringPiece>& keys,
                       char* key_buffer, uint32_t key_buffer_size,
                       const toft::StringPiece& value) {
        uint32_t partition = core::DecodePartition(keys[1]) % _task_concurrency + _task_offset;

        ShuffleHeader* header = reinterpret_cast<ShuffleHeader*>(key_buffer);
        header->set(_task_index, partition, false);
        _emitter->Run(toft::StringPiece(key_buffer, key_buffer_size), value);
    }
};

class ShuffleOutputRunner::DefaultRecordWriter : public RecordWriter {
public:
    DefaultRecordWriter(const PbChannel& config,
                       SparkTask::EmitCallback* emitter) : RecordWriter(config, emitter) {}

    virtual uint64_t hashcode(const std::vector<toft::StringPiece>& keys) {
        return toft::CityHash64(keys[1].data(), keys[1].size());
    }

    virtual void write(const std::vector<toft::StringPiece>& keys,
                       char* key_buffer, uint32_t key_buffer_size,
                       const toft::StringPiece& value) {
        uint32_t partition = hashcode(keys) % _task_concurrency + _task_offset;

        ShuffleHeader* header = reinterpret_cast<ShuffleHeader*>(key_buffer);
        header->set(_task_index, partition, false);
        _emitter->Run(toft::StringPiece(key_buffer, key_buffer_size), value);
    }
};

class ShuffleOutputRunner::BroadcastRecordWriter : public RecordWriter {
public:
    BroadcastRecordWriter(const PbChannel& config,
                          SparkTask::EmitCallback* emitter) : RecordWriter(config, emitter) {}

    virtual void write(const std::vector<toft::StringPiece>& keys,
                       char* key_buffer, uint32_t key_buffer_size,
                       const toft::StringPiece& value) {
        ShuffleHeader* header = reinterpret_cast<ShuffleHeader*>(key_buffer);
        header->set(_task_index, _task_offset, false);

        for (uint32_t i = 0; i < _task_concurrency; ++i) {
            header->set(_task_index, _task_offset + i, false);
            _emitter->Run(toft::StringPiece(key_buffer, key_buffer_size), value);
        }
    }
};

void ShuffleOutputRunner::initialize(const PbSparkTask::PbShuffleOutput& message,
                                    SparkTask::EmitCallback* emitter) {
    _message = message;
    _emitter = emitter;
}

void ShuffleOutputRunner::setup(const PbExecutor& message, ExecutorContext* context) {
    for (int i = 0; i != _message.channel_size(); ++i) {
        const PbChannel& config = _message.channel(i);

        std::auto_ptr<TransferEncoder> encoder(new TransferEncoder(config.encoder()));
        std::auto_ptr<RecordWriter> writer;

        if (config.has_transfer_type()) {
            switch (config.transfer_type()) {
                case PbChannel::KEY: {
                    writer.reset(new GroupRecordWriter(config, _emitter));
                    break;
                }
                case PbChannel::SEQUENCE: {
                    writer.reset(new BucketRecordWriter(config, _emitter));
                    break;
                }
                case PbChannel::BROADCAST: {
                    writer.reset(new BroadcastRecordWriter(config, _emitter));
                    break;
                }
                case PbChannel::WINDOW: {
                    LOG(FATAL) << "Window scope is not supported yet in Spark: " << config.DebugString();
                }
                default: {
                    LOG(FATAL) << "un-expected transfer config: " << config.DebugString();
                }
            }
        } else {
            switch (config.transfer_scope().type()) {
                case PbScope::GROUP: {
                    writer.reset(new GroupRecordWriter(config, _emitter));
                    break;
                }
                case PbScope::BUCKET: {
                    writer.reset(new BucketRecordWriter(config, _emitter));
                    break;
                }
                case PbScope::DEFAULT: {
                    writer.reset(new BroadcastRecordWriter(config, _emitter));
                    break;
                }
                case PbScope::WINDOW: {
                    LOG(FATAL) << "Window scope is not supported yet in Spark: " << config.DebugString();
                }
                default: {
                    LOG(FATAL) << "un-expected transfer config: " << config.DebugString();
                }
            }
        }

        context->input(config.from())->RequireStream(
                Source::REQUIRE_KEY | Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(
                        this, &ShuffleOutputRunner::write_record, encoder.get(), writer.get()
                ),
                NULL
        );

        _encoders.push_back(encoder);
        _writers.push_back(writer);
    }
}

void ShuffleOutputRunner::write_record(TransferEncoder* encoder, RecordWriter* writer,
                                      const std::vector<toft::StringPiece>& keys,
                                      void* object, const toft::StringPiece& binary) {
    const uint32_t kKeyStartOffset = sizeof(ShuffleHeader); // NOLINT

    boost::scoped_array<char> buffer_guard;

    char* buffer = _default_buffer;
    uint32_t encoded_bytes = encoder->encode(keys, buffer + kKeyStartOffset, kAvailableBufferSize);
    if (encoded_bytes > kAvailableBufferSize) {
        buffer = new char[kKeyStartOffset + encoded_bytes];
        buffer_guard.reset(buffer);
        encoded_bytes = encoder->encode(keys, buffer + kKeyStartOffset, encoded_bytes);
    }
    writer->write(keys, buffer, kKeyStartOffset + encoded_bytes, binary);
}

void ShuffleOutputRunner::begin_group(const std::vector<toft::StringPiece>& keys) {}

void ShuffleOutputRunner::end_group() {}

ShuffleOutputRunner::ShuffleOutputRunner() {}

ShuffleOutputRunner::~ShuffleOutputRunner() {}

Executor* new_shuffle_output_executor(
        const PbSparkTask::PbShuffleOutput& config,
        SparkTask::EmitCallback* emitter,
        uint32_t input_scope_level,
        const PbExecutor& executor,
        const std::vector<Executor*>& childs,
        DatasetManager* dataset_manager) {
    typedef ExecutorRunnerWrapper<ShuffleOutputRunner> ShuffleOutputExecutor;
    std::auto_ptr<ShuffleOutputExecutor> instance(
            new_executor_by_runner<ShuffleOutputRunner>(
                    input_scope_level, executor, childs, dataset_manager
            )
    );
    instance->runner()->initialize(config, emitter);
    return instance.release();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
