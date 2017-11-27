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

#ifndef FLUME_RUNTIME_COMMON_SHUFFLE_EXECUTOR_H_
#define FLUME_RUNTIME_COMMON_SHUFFLE_EXECUTOR_H_

#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/optional.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "gflags/gflags.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/hash/city.h"

#include "flume/core/objector.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/common/shuffle_runner.h"
#include "flume/runtime/common/sub_executor_manager.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/util/serialize_buffer.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace shuffle {

template<typename T> struct RefType;

template<>
struct RefType<std::string> {
    typedef toft::StringPiece type;
};

template<>
struct RefType<uint32_t> {
    typedef uint32_t type;
};

class ShuffleContextBase {
protected:
    ShuffleContextBase() : _key_buffer(32 * 1024 /*32k*/) {}

    const std::vector<Dataset*>& datasets(const std::vector<Dispatcher*>& outputs) {
        _datasets.clear();
        for (size_t i = 0; i < outputs.size(); ++i) {
            _datasets.push_back(outputs[i]->GetDataset(outputs[i]->GetScopeLevel()));
        }
        return _datasets;
    }

    void register_datasets(const toft::StringPiece& key,
                           const std::vector<Dataset*>& datasets,
                           const std::vector<Dispatcher*>& outputs) {
        CHECK_EQ(datasets.size(), outputs.size());
        for (size_t i = 0; i < datasets.size(); ++i) {
            outputs[i]->BeginGroup(key, datasets[i]);
        }
    }

    void release_datasets(const std::vector<Dataset*>& datasets) {
        for (size_t i = 0; i < datasets.size(); ++i) {
            datasets[i]->Release();
        }
    }

    toft::StringPiece save_key(uint32_t key, boost::optional<uint32_t>* ptr) {
        *ptr = key;
        _saved_key = _key_buffer.Copy(core::EncodePartition(key));
        return _saved_key.ref();
    }

    toft::StringPiece save_key(const toft::StringPiece key,
                               boost::optional<toft::StringPiece>* ptr) {
        _saved_key = _key_buffer.Copy(key);
        *ptr = _saved_key.ref();
        return _saved_key.ref();
    }

private:
    std::vector<Dataset*> _datasets;
    SerializeBuffer _key_buffer;
    SerializeBuffer::Guard _saved_key;
};

template<typename Key, bool kIsSorted = true>
class ShuffleContext : public ShuffleContextBase {
public:
    typedef typename RefType<Key>::type KeyRef;

    void setup(const PbShuffleExecutor& message, ExecutorContext* context) {
        _broadcast_runner.Initialize(message.scope());
        _cogroup_runner.Initialize(message.scope());

        for (int i = 0; i < message.node_size(); ++i) {
            const PbLogicalPlanNode& node = message.node(i);
            Source* input = context->input(node.shuffle_node().from());
            Dispatcher* output = context->output(node.id());
            output->SetObjector(node.objector());

            if (node.shuffle_node().type() == PbShuffleNode::BROADCAST) {
                _broadcast_outputs.push_back(output);
                _broadcast_runner.AddShuffleInput(node, input);
            } else {
                _cogroup_outputs.push_back(output);
                _cogroup_runner.AddShuffleInput(node, input);
            }
        }

        _context = context;
    }

    void start_shuffle() {
        _broadcast_runner.StartShuffle(datasets(_broadcast_outputs));
        _cogroup_runner.StartShuffle(datasets(_cogroup_outputs));
        _current_key = boost::none;
    }

    void finish_shuffle() {
        CHECK(is_shuffle_done());

        if (_current_key == boost::none) {
            _ptr = _cogroup_runner.begin();
        } else {
            _context->end_sub_group();
        }

        while (_ptr != _cogroup_runner.end()) {
            const std::string& key_name = _cogroup_runner.key_name(_ptr);

            register_datasets(key_name, _broadcast_runner.datasets(key_name), _broadcast_outputs);
            register_datasets(key_name, _cogroup_runner.datasets(_ptr), _cogroup_outputs);
            _context->begin_sub_group(key_name);
            _context->end_sub_group();

            ++_ptr;
        }

        _broadcast_runner.FinishShuffle();
        _cogroup_runner.FinishShuffle();
    }

    bool is_shuffle_done() {
        return _broadcast_runner.IsInputDone() && _cogroup_runner.IsInputDone();
    }

    // return true if we has it, and save it ot *key;
    bool get_next_key(KeyRef* key) {
        if (_ptr == _cogroup_runner.end()) {
            return false;
        }

        *key = _cogroup_runner.key(_ptr);
        return true;
    }

    void move_to_next() {
        if (_current_key != boost::none) {
            _context->end_sub_group();
        }

        CHECK(_ptr != _cogroup_runner.end());
        toft::StringPiece key = save_key(_cogroup_runner.key(_ptr), &_current_key);

        register_datasets(key, _broadcast_runner.datasets(key), _broadcast_outputs);
        register_datasets(key, _cogroup_runner.datasets(_ptr), _cogroup_outputs);
        _context->begin_sub_group(key);

        ++_ptr;
    }

    // flush all datasets with preceding key, and push datasets with same key
    // return true if enter new group
    bool move_to(KeyRef key) {
        CHECK(is_shuffle_done());

        if (_current_key != boost::none) {
            if (*_current_key == key) {
                return false;  // same group
            }
        } else {
            _ptr = _cogroup_runner.begin();
        }

        KeyRef next_key;
        while (get_next_key(&next_key) && next_key <= key) {
            move_to_next();
        }

        if (_current_key != boost::none && *_current_key != key) {
            _context->end_sub_group();
            _current_key = boost::none;
        }

        if (_current_key == boost::none) {
            toft::StringPiece group_key = save_key(key, &_current_key);
            register_datasets(group_key,
                              _broadcast_runner.datasets(group_key), _broadcast_outputs);
            _context->begin_sub_group(group_key);
        }

        return true;  // new group
    }

    void close_prior_outputs(uint32_t priority) {
        _context->close_prior_outputs(priority);
    }

private:
    std::vector<Dispatcher*> _broadcast_outputs;
    internal::BroadcastRunner _broadcast_runner;

    std::vector<Dispatcher*> _cogroup_outputs;
    internal::CoGroupRunner<Key, kIsSorted> _cogroup_runner;

    ExecutorContext* _context;
    // _current_key is always less than key(_ptr)
    boost::optional<KeyRef> _current_key;
    typename internal::CoGroupRunner<Key, kIsSorted>::Container::iterator _ptr;
};

template<typename Key, bool kIsSorted = true> class LocalRunner;

template<bool _>
class LocalRunner<uint32_t, _> : public ExecutorRunner {
public:
    virtual void setup(const PbExecutor& message, ExecutorContext* context) {
        _context.setup(message.shuffle_executor(), context);
    }

    virtual void begin_group(const std::vector<toft::StringPiece>& keys) {
        _context.start_shuffle();
    }

    virtual void end_group() {
        _context.finish_shuffle();
    }

private:
    ShuffleContext<uint32_t, true> _context;
};

template<bool NeedSorted = false, typename IndexType = unsigned char>
struct ShuffleRecord;

template<typename IndexType>
struct ShuffleRecord<true, IndexType> {
    const static int MAX_SUPPORT_SHUFFLE_NODE_NUM = (1 << sizeof(IndexType) * 8) - 1;

    ShuffleRecord(int index,
                  const toft::StringPiece& key,
                  toft::StringPiece data) : index((IndexType)index),
                                          key(key),
                                          data(data) {
    }

    ShuffleRecord() {}

    int key_compare(const ShuffleRecord<true, IndexType>& that) const{
        return key.compare(that.key);
    }

    IndexType index;  // ShuffleNode index
    toft::StringPiece key;
    toft::StringPiece data;
};

template<typename IndexType>
struct ShuffleRecord<false, IndexType> {
    const static int MAX_SUPPORT_SHUFFLE_NODE_NUM = (1 << sizeof(IndexType) * 8) - 1;
    ShuffleRecord(int index,
                  const toft::StringPiece& key,
                  toft::StringPiece data) : index((IndexType)index),
                                          key(key),
                                          data(data),
                                          hashcode(toft::CityHash32(key.data(), key.size())){
    }

    ShuffleRecord() {}

    int key_compare(const ShuffleRecord<false, IndexType>& that) const {
        if (hashcode != that.hashcode) {
            return hashcode < that.hashcode ? -1 : 1;
        }
        return key.compare(that.key);
    }

    IndexType index;  // ShuffleNode index
    toft::StringPiece key;
    toft::StringPiece data;
    uint32_t hashcode;
};

class RecordLess {
public:
    RecordLess(const std::vector<int>& priority) : _priority(priority) {
    }

    template<bool NeedSorted, typename IndexType>
    bool operator()(const ShuffleRecord<NeedSorted, IndexType>& left,
                    const ShuffleRecord<NeedSorted, IndexType>& right) {
        int cmp = left.key_compare(right);
        if (cmp != 0) {
            return cmp < 0;
        }
        return _priority[left.index] < _priority[right.index];
    }
private:
    const std::vector<int>& _priority;
};

template<bool kIsSorted>
class LocalRunner<std::string, kIsSorted> : public ExecutorRunner {
public:
    typedef ShuffleRecord<kIsSorted> Record;

    virtual void setup(const PbExecutor& message, ExecutorContext* context) {
        _context = context;
        const PbShuffleExecutor& sub_message = message.shuffle_executor();
        CHECK_EQ(PbShuffleExecutor::LOCAL, sub_message.type());

        size_t sn_num = sub_message.node_size();
        CHECK_LE(sn_num, ShuffleRecord<kIsSorted>::MAX_SUPPORT_SHUFFLE_NODE_NUM);
        CHECK_EQ(message.dispatcher_size(), sn_num);

        std::map<std::string, int> id2priority;
        for (size_t i = 0; i != sn_num; ++i) {
            const PbExecutor::Dispatcher& dispatcher = message.dispatcher(i);
            if (dispatcher.has_priority()) {
                id2priority[dispatcher.identity()] = dispatcher.priority();
            } else {
                id2priority[dispatcher.identity()] = -1;
            }
        }
        _functor.reset(new internal::KeyReaderFunctor[sn_num]);

        for (int i = 0; i < sn_num; ++i) {
            const PbLogicalPlanNode& node = sub_message.node(i);
            CHECK_EQ(1u, id2priority.count(node.id()));
            _priority.push_back(id2priority[node.id()]);
            CHECK_EQ(PbLogicalPlanNode::SHUFFLE_NODE, node.type());
            const PbShuffleNode& sn = node.shuffle_node();
            Source* input = context->input(sn.from());
            Dispatcher* output = context->output(node.id());

            if (sn.type() == PbShuffleNode::BROADCAST) {
                input->RequireStream(
                    Source::REQUIRE_BINARY,
                    toft::NewPermanentClosure(
                        this, &LocalRunner::on_broadcast_input_come, i
                    ),
                    NULL
                );
                _broadcast_priority.insert(std::make_pair(_priority.back(), i));
            } else {
                input->RequireStream(
                    Source::REQUIRE_BINARY | Source::REQUIRE_OBJECT,
                    toft::NewPermanentClosure(
                        this, &LocalRunner::on_cogroup_input_come, i
                    ),
                    NULL
                );
                _functor[i].Initialize(sub_message.scope(), node);
            }
            _dispatchers.push_back(output);
        }
    }

    virtual void on_cogroup_input_come(int index,
                                       const std::vector<toft::StringPiece>& keys,
                                       void* object,
                                       const toft::StringPiece& binary) {
        toft::StringPiece key = _functor[index](object, _arena.get());
        toft::StringPiece value = _arena->Allocate(binary);
        _cogroup_data.push_back(ShuffleRecord<kIsSorted>(index, key, value));
    }

    virtual void on_broadcast_input_come(int index,
                                         const std::vector<toft::StringPiece>& keys,
                                         void* object,
                                         const toft::StringPiece& binary) {
        _broadcast_data[index].push_back(_arena->Allocate(binary));
    }

    virtual void begin_group(const std::vector<toft::StringPiece>& keys) {
        _is_first = true;
        _cogroup_data.clear();
        _broadcast_data.clear();
        _broadcast_iterator = _broadcast_priority.begin();
        _arena.reset(new flume::util::Arena);
    }

    virtual void end_group() {
        typedef std::vector<Record> Container;
        std::sort(_cogroup_data.begin(), _cogroup_data.end(), RecordLess(_priority));
        typename Container::iterator cogroup_iter = _cogroup_data.begin();
        std::multimap<int, int>::iterator broadcast_iter = _broadcast_priority.begin();

        for (;cogroup_iter != _cogroup_data.end(); ++cogroup_iter) {
            move_to(*cogroup_iter);
            emit(cogroup_iter->index, cogroup_iter->data);
        }
        flush_and_reset_broadcast();
        _context->end_sub_group();
    }

protected:

    void unchecked_move_to(const Record& record) {
        if (!_is_first) {
            flush_and_reset_broadcast();
            _context->end_sub_group();
        }
        _context->begin_sub_group(record.key);
        _is_first = false;
    }

    void move_to(const Record& record) {
        if (_is_first || record.key_compare(_last_record) != 0) {
            unchecked_move_to(record);
        }
        move_broadcast(record);
        _last_record = record;
    }

    void broadcast(int index) {
        const std::vector<toft::StringPiece>& data = _broadcast_data[index];
        for (size_t i = 0; i != data.size(); ++i) {
            emit(index, data[i]);
        }
    }

    void flush_and_reset_broadcast() {
        while(_broadcast_iterator != _broadcast_priority.end()) {
            int index = _broadcast_iterator->second;
            broadcast(index);
            ++_broadcast_iterator;
        }
        _broadcast_iterator = _broadcast_priority.begin();
    }

    void move_broadcast(const Record& record) {
        while(_broadcast_iterator != _broadcast_priority.end()) {
            int priority = _broadcast_iterator->first;
            int index = _broadcast_iterator->second;
            if (priority > get_priority(record)) {
                break;
            }
            broadcast(index);
            ++_broadcast_iterator;
        }
    }

    int get_priority(const Record& record) const {
        return _priority[record.index];
    }

    void emit(int index, toft::StringPiece binary) {
        if (_priority[index] >= 0) {
            _context->close_prior_outputs(_priority[index]);
        }
        _dispatchers[index]->EmitBinary(binary);
    }

private:
    std::vector<Record> _cogroup_data;
    std::map<int, std::vector<toft::StringPiece> > _broadcast_data;
    toft::scoped_ptr<flume::util::Arena> _arena;
    ExecutorContext* _context;
    std::vector<Dispatcher*> _dispatchers;
    toft::scoped_array<internal::KeyReaderFunctor> _functor;
    bool _is_first;
    toft::StringPiece _last_key;
    Record _last_record;
    std::vector<int> _priority;
    std::multimap<int, int> _broadcast_priority;  // priority -> output_index
    std::multimap<int, int>::iterator _broadcast_iterator;
};

class MergeRunner : public ExecutorRunner {
public:
    MergeRunner();

    virtual void setup(const PbExecutor& message, ExecutorContext* context);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    virtual void end_group();

private:
    static const int kMaxPriority = 3;

    void on_input_come(uint32_t priority, Dispatcher* output,
                       const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary);

    uint32_t _scope_level;
    ExecutorContext* _context;

    bool _is_first_record;
    std::string _last_key;
};

template<typename KeyType>
class CombineRunner : public ExecutorRunner {
public:
    virtual void setup(const PbExecutor& message, ExecutorContext* context) {
        const PbShuffleExecutor& sub_message = message.shuffle_executor();

        _scope_level = message.scope_level();
        _context.setup(sub_message, context);

        for (int i = 0; i < sub_message.source_size(); ++i) {
            const PbShuffleExecutor::MergeSource& source = sub_message.source(i);
            CHECK(source.has_priority());

            Source* input = context->input(source.input());
            Dispatcher* output = context->output(source.output());
            input->RequireStream(
                Source::REQUIRE_KEY | Source::REQUIRE_BINARY,
                toft::NewPermanentClosure(
                        this, &CombineRunner::on_input_come, source.priority(), output
                ),
                NULL
            );
        }
    }

    virtual void begin_group(const std::vector<toft::StringPiece>& keys) {
        _context.start_shuffle();
    }

    virtual void end_group() {
        _context.finish_shuffle();
    }

private:
    void on_input_come(uint32_t priority, Dispatcher* output,
                       const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary) {
        typename RefType<KeyType>::type key;
        read_key(keys[_scope_level], &key);
        _context.move_to(key);
        _context.close_prior_outputs(priority);
        output->EmitBinary(keys, binary);
    }

    void read_key(const toft::StringPiece& data, toft::StringPiece* key) {
        *key = data;
    }

    void read_key(const toft::StringPiece& data, uint32_t* key) {
        *key = core::DecodePartition(data);
    }

    uint32_t _scope_level;
    ShuffleContext<KeyType, true> _context;
};

class LocalDistributeRunner : public ExecutorRunner {
public:
    void set_partition(uint32_t partition);

    virtual void setup(const PbExecutor& message, ExecutorContext* context);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    virtual void end_group();

private:
    void on_shuffle_input(int index, Dispatcher* output,
                          const std::vector<toft::StringPiece>& keys,
                          void* object, const toft::StringPiece& binary);

    void on_merge_input(int index, Dispatcher* output,
                        const std::vector<toft::StringPiece>& keys,
                        void* object, const toft::StringPiece& binary);

    void on_input_done(Dispatcher* output);

    std::string _partition;
    ExecutorContext* _context;
    std::vector<Source::Handle*> _handles;
};

class DistributeByEveryRecordRunner : public ExecutorRunner {
public:
    virtual void setup(const PbExecutor& message, ExecutorContext* context);

    virtual void on_cogroup_input_come(int index,
                                       const std::vector<toft::StringPiece>& keys,
                                       void* object,
                                       const toft::StringPiece& binary);

    virtual void on_broadcast_input_come(int index,
                                       const std::vector<toft::StringPiece>& keys,
                                       void* object,
                                       const toft::StringPiece& binary);

    virtual void on_broadcast_input_done(int index);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    virtual void end_group();

protected:
    void flush_buffer();

    void flush_record(int index, const toft::StringPiece& binary);

    void register_datasets(const toft::StringPiece& key);

    void collect_datasets();

private:
    std::map<int, std::vector<toft::StringPiece> > _cogroup_data;
    std::map<int, Dataset*> _broadcast_data;
    std::map<int, Dispatcher*> _broadcast_dispatcher;
    toft::scoped_ptr<flume::util::Arena> _arena;
    ExecutorContext* _context;
    std::vector<PriorityDispatcher> _dispatchers;
    bool _need_cache;
    int _broadcast_num;
    int _done_broadcast_num;
    uint64_t _global_index;
};

}  // namespace shuffle

Executor* new_shuffle_executor(uint32_t partition,
                               uint32_t input_scope_level, const PbExecutor& message,
                               const std::vector<Executor*>& childs,
                               DatasetManager* dataset_manager);

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SHUFFLE_EXECUTOR_H_
