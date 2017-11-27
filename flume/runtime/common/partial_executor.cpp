/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//

#include "flume/runtime/common/partial_executor.h"

#include <cstring>
#include <memory>

#include "boost/foreach.hpp"
#include "boost/intrusive/slist.hpp"
#include "boost/range/algorithm.hpp"
#include "boost/assign/ptr_map_inserter.hpp"
#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"
#include "toft/hash/city.h"

#include "flume/core/entity.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_base.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

void DoNothing() { }

class PartialSource : public Source {
public:
    PartialSource(Source* original_source)
            : m_origin(original_source) {}

    virtual Handle* RequireStream(uint32_t flag,
                                  StreamCallback* callback, DoneCallback* done) {
        CHECK_EQ(Source::REQUIRE_BINARY | Source::REQUIRE_KEY, flag)
                << "Only ReduceOutput is allowed to be downstream of PartialExecutor";
        m_done_callbacks.push_back(done);
        return m_origin->RequireStream(flag, callback, toft::NewPermanentClosure(&DoNothing));
    }

    void Done() {
        BOOST_FOREACH(DoneCallback& callback, m_done_callbacks) {
            callback.Run();
        }
    }

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback, DoneCallback* done) {
        CHECK(false) << "Only ReduceOutput is allowed to be downstream of PartialExecutor";
        return NULL;
    }

private:
    boost::ptr_vector<DoneCallback> m_done_callbacks;
    Source* m_origin;
};

}  // namespace

PartialExecutor::PartialExecutor() :
        m_buffer_size(kDefaultBufferSize), m_buffer(new char[m_buffer_size]), m_instance(NULL),
        m_buffer_ptr(NULL), m_buffer_end(NULL), m_records(NULL), m_record_size(0) {}

PartialExecutor::PartialExecutor(int metabytes) :
        m_buffer_size(metabytes * 1024 * 1024),  m_buffer(new char[m_buffer_size]),
        m_instance(NULL), m_buffer_ptr(NULL), m_buffer_end(NULL),
        m_records(NULL), m_record_size(0) {}

void PartialExecutor::Initialize(const PbExecutor& message, const std::vector<Executor*>& childs,
                                 uint32_t input_scope_level, DatasetManager* dataset_manager) {
    CHECK_EQ(0, input_scope_level);
    CHECK_EQ(0, message.scope_level());

    m_message = message;
    m_childs.Initialize(message, childs);
    m_dispatchers.reset(new internal::DispatcherManager(message, dataset_manager));

    const PbPartialExecutor& partial_executor = message.partial_executor();
    boost::ptr_multimap<std::string, EntityDag::Listener> listeners;
    for (int i = 0; i < partial_executor.output_size(); ++i) {
        const PbPartialExecutor::Output& config = partial_executor.output(i);

        std::auto_ptr<core::Objector> objector(
                core::Entity<core::Objector>(config.objector()).CreateAndSetup()
        );

        std::auto_ptr<Output> output(new Output);
        output->index = i;
        output->need_hash = config.need_hash();
        boost::copy(config.priority(), std::back_inserter(output->priorities));
        output->dispatcher = m_dispatchers->get(config.identity());
        output->objector = objector.get();

        std::auto_ptr<EntityDag::Listener> listener;
        if (config.need_buffer()) {
            listener.reset(
                    toft::NewPermanentClosure(this, &PartialExecutor::OnSortedOutput, output.get())
            );
        } else {
            listener.reset(
                    toft::NewPermanentClosure(this, &PartialExecutor::OnDirectOutput, output.get())
            );
        }

        m_objectors.push_back(objector);
        m_outputs.push_back(output);
        listeners.insert(config.identity(), listener);
    }

    m_dag.Initialize(partial_executor.scope().begin(), partial_executor.scope().end(),
                     partial_executor.node().begin(), partial_executor.node().end(),
                     partial_executor.scope_level().begin(), partial_executor.scope_level().end(),
                     message.input().begin(), message.input().end(),
                     &listeners);
    m_active_bitset.resize(message.input_size());
}

void PartialExecutor::Setup(const std::map<std::string, Source*>& sources) {
    for (int i = 0; i < m_message.input_size(); ++i) {
        std::map<std::string, Source*>::const_iterator ptr = sources.find(m_message.input(i));
        CHECK(ptr != sources.end());
        ptr->second->RequireStream(Source::REQUIRE_OBJECT,
            toft::NewPermanentClosure(this, &PartialExecutor::OnInputCome, i),
            toft::NewPermanentClosure(this, &PartialExecutor::OnInputDone, i)
        );
    }

    std::map<std::string, Source*> child_inputs;
    const PbPartialExecutor& partial_executor = m_message.partial_executor();
    for (int i = 0; i < partial_executor.output_size(); ++i) {
        const PbPartialExecutor::Output& config = partial_executor.output(i);
        child_inputs[config.identity()] = m_dispatchers->get(config.identity())->GetSource(0);
    }
    m_childs.Setup(child_inputs);

    for (int i = 0; i < m_message.output_size(); ++i) {
        const std::string& id = m_message.output(i);
        Dispatcher* dispatcher = m_dispatchers->get(id);
        Source* source = NULL;
        if (dispatcher != NULL) {
            source = dispatcher->GetSource(0);
        } else {
            source = m_childs.GetSource(id, 0);
        }
        boost::assign::ptr_map_insert<PartialSource>(m_partial_source)(id, source);
    }
}

Source* PartialExecutor::GetSource(const std::string& id, unsigned scope_level) {
    CHECK_EQ(0u, scope_level);
    CHECK_EQ(1u, m_partial_source.count(id));
    return m_partial_source.find(id)->second;
}

void PartialExecutor::BeginGroup(const toft::StringPiece& key) {
    CHECK(m_active_bitset.none());
    m_keys.push_back(key);
}

void PartialExecutor::FinishGroup() {
    m_active_bitset.set();
    ResetBuffer();
    m_instance = m_dag.GetInstance(m_keys);
}

void PartialExecutor::OnInputCome(int input, const std::vector<toft::StringPiece>& keys,
                                  void* object, const toft::StringPiece& binary) {
    m_instance->Run(input, object);
}

void PartialExecutor::OnInputDone(int input) {
    m_active_bitset.reset(input);
    if (m_active_bitset.none()) {
        m_instance->Done();
        m_instance = NULL;
        FlushBuffer();
        m_keys.pop_back();

        typedef boost::ptr_map<std::string, Source>::value_type Item;
        BOOST_FOREACH(const Item& item, m_partial_source) {
            PartialSource *source = dynamic_cast<PartialSource*>(item.second);
            source->Done();
        }
    }
}

void PartialExecutor::OnSortedOutput(Output* output,
                                     const std::vector<toft::StringPiece>& keys, void* object) {
    if (PushBack(output, keys, object)) {
        return;
    }

    // retry once
    FlushBuffer();
    ResetBuffer();

    if (!PushBack(output, keys, object)) {
        // for big record
        output->dispatcher->EmitObject(keys, object);
        FlushBuffer();
        ResetBuffer();
    }
}

void PartialExecutor::OnDirectOutput(Output* output,
                                     const std::vector<toft::StringPiece>& keys, void* object) {
    output->dispatcher->EmitObject(keys, object);
}

struct PartialExecutor::Key : public boost::intrusive::slist_base_hook<
    boost::intrusive::link_mode< boost::intrusive::normal_link >
> {
    uint32_t priority;
    uint32_t size;
    char data[0];

    Key() : size(0) {}

    toft::StringPiece ref() const { return toft::StringPiece(data, size); }

    friend bool operator<(const Key& k0, const Key& k1) {
        int r = k0.ref().compare(k1.ref());
        if (r == 0) {
            return k0.priority < k1.priority;
        } else {
            return r < 0;
        }
    }
};

struct PartialExecutor::Record {
    typedef boost::intrusive::slist<
        Key,
        boost::intrusive::constant_time_size<false>  // for minimum size
    > KeyList;

    uint32_t output;
    uint32_t priority;
    uint64_t hash;
    KeyList keys;

    uint32_t size;
    char data[0];

    Record() : output(-1), size(0) {}

    toft::StringPiece ref() { return toft::StringPiece(data, size); }
};

template<typename T>
T* PartialExecutor::New() {
    using namespace boost::alignment;

    std::size_t space = m_buffer_end - m_buffer_ptr;
    void* ptr = m_buffer_ptr;
    if (align(alignment_of<T>::value, sizeof(T), ptr, space) == NULL) {
        return NULL;
    }
    m_buffer_ptr = static_cast<char*>(ptr) + sizeof(T);

    return new(ptr) T();
}

void PartialExecutor::ResetBuffer() {
    m_buffer_ptr = m_buffer.get();
    m_buffer_end = m_buffer_ptr + m_buffer_size;
    m_records = reinterpret_cast<Record**>(m_buffer_end);
    m_record_size = 0;

    m_dispatchers->begin_group(m_keys.back(), 0);
    m_childs.BeginGroup(m_keys.back());

    m_childs.FinishGroup();
    m_dispatchers->finish_group();
}

inline bool PartialExecutor::PushBack(Output* output,
                                      const std::vector<toft::StringPiece>& keys, void* object) {
    DCHECK_EQ(keys.size(), output->priorities.size());

    // reserve space for record
    if (m_buffer_ptr + sizeof(Record*) > m_buffer_end) {  // NOLINT(runtime/sizeof)
        return false;
    }
    m_buffer_end -= sizeof(Record*);  // NOLINT(runtime/sizeof)

    Record* record = New<Record>();
    if (record == NULL) {
        return false;
    }

    record->output = output->index;
    record->priority = output->priorities[0];
    record->hash = 0;
    record->size = output->objector->Serialize(object, record->data, m_buffer_end - record->data);
    if (record->size > m_buffer_end - m_buffer.get()) {
        LOG(INFO) << "Big record size: " << record->size;
    }

    m_buffer_ptr = record->data + record->size;
    if (m_buffer_ptr > m_buffer_end) {
        return false;
    }

    DCHECK(!keys.empty());  // keys at least contains global key
    for (size_t i = keys.size() - 1; i > 0; --i) {
        Key* key = New<Key>();
        if (key == NULL || key->data + keys[i].size() > m_buffer_end) {
            return false;
        }

        key->priority = output->priorities[i];
        key->size = keys[i].size();
        std::memcpy(key->data, keys[i].data(), key->size);
        m_buffer_ptr = key->data + key->size;

        if (output->need_hash) {
            record->hash = toft::CityHash64WithSeed(key->data, key->size, record->hash);
        }

        record->keys.push_front(*key);
    }

    m_records = reinterpret_cast<Record**>(m_buffer_end);
    *m_records = record;
    ++m_record_size;

    return true;
}

void PartialExecutor::FlushBuffer() {
    std::sort(m_records, m_records + m_record_size, CompareRecord);

    std::vector<toft::StringPiece> keys = m_keys;
    for (size_t i = 0; i < m_record_size; ++i) {
        Record* record = m_records[i];

        keys.resize(1);  // keep global key
        typedef Record::KeyList::iterator KeyIterator;
        for (KeyIterator key = record->keys.begin(); key != record->keys.end(); ++key) {
            keys.push_back(key->ref());
        }

        m_dispatchers->close_prior_dispatchers(record->priority);
        m_outputs[record->output].dispatcher->EmitBinary(keys, record->ref());
    }
    m_dispatchers->done();
}

inline bool PartialExecutor::CompareRecord(const Record* r0, const Record* r1) {
    if (r0->priority != r1->priority) {
        return r0->priority < r1->priority;
    }

    if (r0->hash != r1->hash) {
        return r0->hash < r1->hash;
    }

    return r0->keys < r1->keys;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

