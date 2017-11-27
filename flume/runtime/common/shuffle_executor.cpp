/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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

#include "flume/runtime/common/shuffle_executor.h"

#include <string>
#include <utility>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"
#include "toft/system/memory/unaligned.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Objector;

namespace shuffle {

template class LocalRunner<std::string, true>;
template class LocalRunner<std::string, false>;
template class LocalRunner<uint32_t>;

MergeRunner::MergeRunner() :
        _scope_level(0), _context(NULL), _is_first_record(false) {}

void MergeRunner::setup(const PbExecutor& message, ExecutorContext* context) {
    _scope_level = message.scope_level();
    _context = context;

    CHECK(message.has_shuffle_executor());
    const PbShuffleExecutor& sub_message = message.shuffle_executor();
    for (int i = 0; i < sub_message.source_size(); ++i) {
        const PbShuffleExecutor::MergeSource& source = sub_message.source(i);
        CHECK(source.has_priority());

        Source* input = _context->input(source.input());
        Dispatcher* output = _context->output(source.output());
        input->RequireStream(
                Source::REQUIRE_KEY | Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(
                        this, &MergeRunner::on_input_come, source.priority(), output
                ),
                NULL
        );
    }
}

void MergeRunner::begin_group(const std::vector<toft::StringPiece>& keys) {
    _is_first_record = true;
}

void MergeRunner::end_group() {
    if (!_is_first_record) {
        _context->end_sub_group();
    }
}

void MergeRunner::on_input_come(uint32_t priority, Dispatcher* output,
                                const std::vector<toft::StringPiece>& keys,
                                void* object, const toft::StringPiece& binary) {
    const toft::StringPiece& key = keys[_scope_level];
    if (_is_first_record || key != _last_key) {
        if (!_is_first_record) {
            _context->end_sub_group();
        }

        key.copy_to_string(&_last_key);
        _context->begin_sub_group(_last_key);
    }
    _is_first_record = false;

    _context->close_prior_outputs(priority);
    output->EmitBinary(keys, binary);
}

void LocalDistributeRunner::set_partition(uint32_t partition) {
    _partition =  core::EncodePartition(partition);
}

void LocalDistributeRunner::setup(const PbExecutor& message, ExecutorContext* context) {
    CHECK(message.has_shuffle_executor());
    const PbShuffleExecutor& sub_message = message.shuffle_executor();

    _context = context;

    for (int i = 0; i != sub_message.node_size(); ++i) {
        const PbLogicalPlanNode& node = sub_message.node(i);
        Source* input = context->input(node.shuffle_node().from());
        Dispatcher* output = context->output(node.id());

        int index = _handles.size();
        _handles.push_back(input->RequireStream(
                Source::REQUIRE_OBJECT,
                toft::NewPermanentClosure(
                        this, &LocalDistributeRunner::on_shuffle_input, index, output
                ),
                toft::NewPermanentClosure(
                        this, &LocalDistributeRunner::on_input_done, output
                )
        ));
    }

    for (int i = 0; i != sub_message.source_size(); ++i) {
        const PbShuffleExecutor::MergeSource& source = sub_message.source(i);
        Source* input = context->input(source.input());
        Dispatcher* output = context->output(source.output());

        int index = _handles.size();
        _handles.push_back(input->RequireStream(
                Source::REQUIRE_KEY | Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(
                        this, &LocalDistributeRunner::on_merge_input, index, output
                ),
                toft::NewPermanentClosure(
                        this, &LocalDistributeRunner::on_input_done, output
                )
        ));
    }
}

void LocalDistributeRunner::begin_group(const std::vector<toft::StringPiece>& keys) {
    _context->begin_sub_group(_partition);
}

void LocalDistributeRunner::end_group() {
    _context->end_sub_group();
}

void LocalDistributeRunner::on_shuffle_input(int index, Dispatcher* output,
                                             const std::vector<toft::StringPiece>& keys,
                                             void* object, const toft::StringPiece& binary) {
    // for ShuffleNode, origin input is likely to be raw object
    if (!output->EmitObject(object)) {
        _handles[index]->Done();
    }
}

void LocalDistributeRunner::on_merge_input(int index, Dispatcher* output,
                                           const std::vector<toft::StringPiece>& keys,
                                           void* object, const toft::StringPiece& binary) {
    // for Channel, origin input is likely to be binary (shuffled from other task)
    if (!output->EmitBinary(keys, binary)) {
        _handles[index]->Done();
    }
}

void LocalDistributeRunner::on_input_done(Dispatcher* output) {
    output->Done();
}

void DistributeByEveryRecordRunner::setup(const PbExecutor& message, ExecutorContext* context) {
    _context = context;
    const PbShuffleExecutor& sub_message = message.shuffle_executor();
    CHECK_EQ(PbShuffleExecutor::BY_RECORD, sub_message.type());

    _global_index = 0;

    size_t sn_num = sub_message.node_size();
    size_t channel_num = sub_message.source_size();
    CHECK_EQ(message.dispatcher_size(), sn_num + channel_num);

    _broadcast_num = 0;
    for (size_t i = 0; i < sn_num; ++i) {
        const PbLogicalPlanNode& node = sub_message.node(i);
        CHECK_EQ(PbLogicalPlanNode::SHUFFLE_NODE, node.type());
        const PbShuffleNode& sn = node.shuffle_node();
        Source* input = context->input(sn.from());
        PriorityDispatcher output = context->priority_output(node.id());

        if (sn.type() == PbShuffleNode::BROADCAST) {
            input->RequireStream(
                Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(
                    this, &DistributeByEveryRecordRunner::on_broadcast_input_come, i
                ),
                toft::NewPermanentClosure(
                    this, &DistributeByEveryRecordRunner::on_broadcast_input_done, i
                )
            );
            _broadcast_dispatcher[i] = output.dispatcher;
            _broadcast_num++;
        } else {
            input->RequireStream(
                Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(
                    this, &DistributeByEveryRecordRunner::on_cogroup_input_come, i
                ),
                NULL
            );
        }
        _dispatchers.push_back(output);
    }

    for (size_t i = 0; i < channel_num; ++i) {
        const PbShuffleExecutor::MergeSource& source = sub_message.source(i);
        Source* input = context->input(source.input());
        PriorityDispatcher output = context->priority_output(source.output());

        input->RequireStream(
            Source::REQUIRE_BINARY,
            toft::NewPermanentClosure(
                    this, &DistributeByEveryRecordRunner::on_cogroup_input_come, i + sn_num
            ),
            NULL
        );
        _dispatchers.push_back(output);
    }
}

void DistributeByEveryRecordRunner::on_cogroup_input_come(int index,
                                   const std::vector<toft::StringPiece>& keys,
                                   void* object,
                                   const toft::StringPiece& binary) {
    if (_need_cache) {
        _cogroup_data[index].push_back(_arena->Allocate(binary));
    } else {
        flush_record(index, binary);
    }
}

void DistributeByEveryRecordRunner::on_broadcast_input_come(int index,
                                   const std::vector<toft::StringPiece>& keys,
                                   void* object,
                                   const toft::StringPiece& binary) {
    internal::ShuffleRunnerBase::CopyTo(binary, _broadcast_data[index]);
}

void DistributeByEveryRecordRunner::on_broadcast_input_done(int index) {
    _broadcast_data[index]->Commit();
    _done_broadcast_num++;
    if (_done_broadcast_num == _broadcast_num) {
        flush_buffer();
        _need_cache = false;
    }
}

void DistributeByEveryRecordRunner::begin_group(const std::vector<toft::StringPiece>& keys) {
    _cogroup_data.clear();
    collect_datasets();

    if (_broadcast_num == 0) {
        _need_cache = false;
    } else {
        _need_cache = true;
    }
    _done_broadcast_num = 0;
    _arena.reset(new flume::util::Arena);
}

void DistributeByEveryRecordRunner::end_group() {
    for (std::map<int, Dataset*>::iterator it = _broadcast_data.begin();
         it != _broadcast_data.end();
         it++) {
        it->second->Release();
    }
}

void DistributeByEveryRecordRunner::flush_buffer() {
    std::map<int, std::vector<toft::StringPiece> >::iterator cogroup_iter = _cogroup_data.begin();
    std::vector<toft::StringPiece>::iterator cogroup_vector_iter;
    for (;cogroup_iter != _cogroup_data.end(); ++cogroup_iter) {
        int index = cogroup_iter->first;
        cogroup_vector_iter = cogroup_iter->second.begin();
        for (;cogroup_vector_iter != cogroup_iter->second.end(); ++cogroup_vector_iter) {
            flush_record(index, *cogroup_vector_iter);
        }
    }
    _cogroup_data.clear();
}

void DistributeByEveryRecordRunner::flush_record(int index, const toft::StringPiece& binary) {
    std::string key = boost::lexical_cast<std::string>(_global_index);
    toft::StringPiece binary_key = toft::StringPiece(key.data(), key.size());

    register_datasets(binary_key);

    _context->begin_sub_group(binary_key);

    _context->close_prior_outputs(_dispatchers[index].priority);

    _dispatchers[index].dispatcher->EmitBinary(binary);
    _context->end_sub_group();
    _global_index++;
}

void DistributeByEveryRecordRunner::register_datasets(const toft::StringPiece& key) {
    std::map<int, Dataset*>::iterator broacast_iter = _broadcast_data.begin();
    for (;broacast_iter != _broadcast_data.end(); ++broacast_iter) {
        broacast_iter->second->AddReference();
        _dispatchers[broacast_iter->first].dispatcher->BeginGroup(key, broacast_iter->second);
    }
}

void DistributeByEveryRecordRunner::collect_datasets() {
    _broadcast_data.clear();
    std::map<int, Dispatcher*>::iterator dispatcher_iter = _broadcast_dispatcher.begin();
    for (;dispatcher_iter != _broadcast_dispatcher.end(); ++dispatcher_iter) {
        _broadcast_data[dispatcher_iter->first] =
            dispatcher_iter->second->GetDataset(dispatcher_iter->second->GetScopeLevel())->GetChild("");
    }
}

}  // namespace shuffle

Executor* new_shuffle_executor(uint32_t partition,
                               uint32_t input_scope_level, const PbExecutor& message,
                               const std::vector<Executor*>& childs,
                               DatasetManager* dataset_manager) {
    CHECK(message.has_shuffle_executor());
    const PbShuffleExecutor& sub_message = message.shuffle_executor();
    const PbScope& scope = sub_message.scope();
    CHECK(scope.type() == PbScope::GROUP || scope.type() == PbScope::BUCKET);

    switch (sub_message.type()) {
        case PbShuffleExecutor::LOCAL: {
            if (scope.type() == PbScope::GROUP) {
                if (scope.is_sorted()) {
                    return new_executor_by_runner< shuffle::LocalRunner<std::string, true> >(
                        input_scope_level, message, childs, dataset_manager
                    );
                } else {
                    return new_executor_by_runner< shuffle::LocalRunner<std::string, false> >(
                        input_scope_level, message, childs, dataset_manager
                    );
                }
            }

            if (scope.type() == PbScope::BUCKET) {
                return new_executor_by_runner< shuffle::LocalRunner<uint32_t, true> >(
                    input_scope_level, message, childs, dataset_manager
                );
            }
        }
        case PbShuffleExecutor::MERGE: {
            return new_executor_by_runner<shuffle::MergeRunner>(
                input_scope_level, message, childs, dataset_manager
            );
        }
        case PbShuffleExecutor::COMBINE: {
            if (scope.type() == PbScope::GROUP) {
                return new_executor_by_runner< shuffle::CombineRunner<std::string> >(
                    input_scope_level, message, childs, dataset_manager
                );
            }

            if (scope.type() == PbScope::BUCKET) {
                return new_executor_by_runner< shuffle::CombineRunner<uint32_t> >(
                    input_scope_level, message, childs, dataset_manager
                );
            }
        }
        case PbShuffleExecutor::LOCAL_DISTRIBUTE: {
            typedef ExecutorRunnerWrapper<shuffle::LocalDistributeRunner> ExecutorType;
            std::auto_ptr<ExecutorType> executor(
                new_executor_by_runner<shuffle::LocalDistributeRunner>(
                    input_scope_level, message, childs, dataset_manager
                )
            );
            executor->runner()->set_partition(partition);
            return executor.release();
        }
        case PbShuffleExecutor::BY_RECORD: {
            return new_executor_by_runner< shuffle::DistributeByEveryRecordRunner >(
                input_scope_level, message, childs, dataset_manager
            );
        }
        default: {
            LOG(FATAL) << "un-expected shuffle executor type: " << sub_message.type();
        }
    }

    return NULL;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
