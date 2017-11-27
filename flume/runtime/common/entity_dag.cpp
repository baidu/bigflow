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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/common/entity_dag.h"

#include <map>
#include <memory>

#include "boost/any.hpp"
#include "boost/foreach.hpp"
#include "boost/scoped_array.hpp"
#include "boost/tuple/tuple.hpp"
#include "glog/logging.h"
#include "toft/hash/city.h"

#include "flume/core/entity.h"
#include "flume/core/iterator.h"
#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/core/processor.h"
#include "flume/core/sinker.h"
#include "flume/runtime/counter.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Emitter;
using core::Entity;
using core::Loader;
using core::Iterator;
using core::KeyReader;
using core::Objector;
using core::Partitioner;
using core::Processor;
using core::Sinker;

class EntityDag::InstanceImpl : public Instance {
public:
    template<typename BitsetIterator, typename FactoryIterator>
    explicit InstanceImpl(BitsetIterator input_begin, BitsetIterator input_end,
                          FactoryIterator factory_begin, FactoryIterator factory_end) :
            m_input_downstreams(input_begin, input_end) {
        for (FactoryIterator ptr = factory_begin; ptr != factory_end; ++ptr) {
            m_runners.push_back(ptr->NewRunner(this));
        }
        m_active_bitset.resize(m_runners.size());
    }

    virtual void Prepare(const std::vector<toft::StringPiece>& keys) {
        m_record.keys = keys;
        for (size_t i = 0; i < m_runners.size(); ++i) {
            m_active_bitset.set(i);
            m_runners[i].DoPrepare(keys);
        }
    }

    virtual bool Run(int input, void* object) {
        m_record.object = object;
        return Dispatch(m_input_downstreams[input], &m_record);
    }

    virtual void Done() {
        for (size_t i = m_runners.size(); i > 0; --i) {
            m_runners[i - 1].Finish();
            m_active_bitset.reset(i - 1);
        }
        m_record.keys.clear();

        // TODO(wenxiang): use object pool instead of delete
        delete this;
    }

    bool Dispatch(const Bitset& target, Record* record) {
        Bitset::size_type index = target.find_first();
        while (index < m_runners.size()) {
            if (m_active_bitset[index]) {
                m_runners[index].DoRun(record);
            }
            index = target.find_next(index);
        }
        return target.intersects(m_active_bitset);
    }

    bool Test(const Bitset& mask) {
        return mask.intersects(m_active_bitset);
    }

    void Close(const Bitset& mask) {
        m_active_bitset &= ~mask;
    }

    Record* record() { return &m_record; }

private:
    std::vector<Bitset> m_input_downstreams;
    boost::ptr_vector<Runner> m_runners;

    Bitset m_active_bitset;
    Record m_record;
};

void EntityDag::Runner::DoRun(Record* record) {
    set_current_keys(&record->keys);
    this->Run(record);
}

void EntityDag::Runner::DoPrepare(const std::vector<toft::StringPiece>& keys) {
    set_current_keys(&keys);
    this->Prepare(keys);
}

inline bool EntityDag::Runner::Dispatch(Record* record) {
    if (!m_instance->Dispatch(downstream(), record)) {
        m_instance->Close(mask());
        return false;
    }
    return true;
}

bool EntityDag::Runner::Emit(void *object) {
    CHECK_NOTNULL(current_keys());
    Record& tmp = *tmp_record();
    tmp.keys = *current_keys();
    tmp.object = object;
    bool need_more = m_instance->Dispatch(downstream(), &tmp);
    return need_more;
}

void EntityDag::Runner::Done() {
    m_instance->Close(mask());
}

EntityDag::Instance* EntityDag::GetInstance(const std::vector<toft::StringPiece>& keys) {
    std::auto_ptr<InstanceImpl> instance(NewInstance());
    instance->Prepare(keys);
    return instance.release();
}

EntityDag::InstanceImpl* EntityDag::NewInstance() {
    return new InstanceImpl(m_input_downstreams.begin(), m_input_downstreams.end(),
                            m_runner_factories.begin(), m_runner_factories.end());
}

void EntityDag::Initialize(const std::map<std::string, PbScope>& scopes,
                           const std::vector<PbLogicalPlanNode>& nodes,
                           const std::vector<int32_t>& scope_levels,
                           const std::vector<std::string>& inputs,
                           boost::ptr_multimap<std::string, Listener>* listeners) {
    std::vector<std::string> outputs;
    std::multimap<std::string, uint32_t> downstreams;

    while (!listeners->empty()) {
        SetupForListener(listeners->begin()->first, listeners->begin()->second, &downstreams);
        m_listeners.transfer(listeners->begin(), *listeners);
        outputs.push_back("");  // the output identity for listeners has no effects
    }

    for (size_t i = 0; i < nodes.size(); ++i) {  // nodes is already in reverse topological order
        PbLogicalPlanNode node = nodes[i];
        switch (node.type()) {
            case PbLogicalPlanNode::LOAD_NODE: {
                SetupForLoadNode(node.load_node());
                break;
            }
            case PbLogicalPlanNode::PROCESS_NODE: {
                SetupForProcessNode(node.process_node(), &downstreams);
                break;
            }
            case PbLogicalPlanNode::SHUFFLE_NODE: {
                SetupForShuffleNode(scopes.find(node.scope())->second,
                                    node.shuffle_node(),
                                    node.objector(),
                                    &downstreams,
                                    scope_levels[i]);
                break;
            }
            case PbLogicalPlanNode::UNION_NODE: {
                SetupForUnionNode(node.id(), node.union_node(), &downstreams);
                break;
            }
            case PbLogicalPlanNode::SINK_NODE: {
                SetupForSinkNode(node.sink_node(), &downstreams);
                break;
            }
            default: {
                LOG(FATAL) << "EntityDag does not support " << nodes[i].type();
            }
        }
        outputs.resize(m_runner_factories.size(), nodes[i].id());
    }

    for (size_t i = 0; i < inputs.size(); ++i) {
        m_input_downstreams.push_back(ToDownstream(downstreams, inputs[i]));
    }

    for (size_t i = 0; i < m_runner_factories.size(); ++i) {
        m_runner_factories[i].Initialize(i, outputs.at(i),
                                         ToMask(outputs, outputs.at(i)),
                                         ToDownstream(downstreams, outputs.at(i)));
    }
}

EntityDag::Bitset EntityDag::ToMask(const std::vector<std::string>& outputs,
                                    const std::string& identity) {
    Bitset bitset(m_runner_factories.size());

    for (size_t i = 0; i < outputs.size(); ++i) {
        if (outputs[i] == identity) {
            bitset.set(i);
        }
    }

    return bitset;
}

EntityDag::Bitset EntityDag::ToDownstream(const std::multimap<std::string, uint32_t>& downstreams,
                                          const std::string& identity) {
    Bitset bitset(m_runner_factories.size());

    std::pair<std::string, uint32_t> pair;
    BOOST_FOREACH(pair, downstreams.equal_range(identity)) {
        bitset.set(pair.second);
    }

    return bitset;
}

class EntityDag::LoaderRunnerFactory : public RunnerFactory {
public:
    explicit LoaderRunnerFactory(const PbEntity& entity) : m_entity(entity) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(this, instance, m_entity.CreateAndSetup());
    }

private:
    class RunnerImpl : public Runner {
    public:
        RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance, Loader* loader) :
                Runner(factory, instance), m_loader(loader) {}

        virtual void Prepare(const std::vector<toft::StringPiece>& keys) {
            CHECK_EQ(keys.size(), 2);
            m_loader->Load(keys.back().as_string(), this);
        }

    private:
        boost::scoped_ptr<Loader> m_loader;
    };

    Entity<Loader> m_entity;
};

void EntityDag::SetupForLoadNode(const PbLoadNode& node) {
    m_runner_factories.push_back(new LoaderRunnerFactory(node.loader()));
}

// each input of one process_node has a dedicated runner
// this is for the first factory of a process_node
class EntityDag::PrimaryProcessorRunnerFactory : public RunnerFactory {
public:
    explicit PrimaryProcessorRunnerFactory(const PbEntity& entity) :
            m_entity(entity), m_processor(NULL) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        m_processor = m_entity.CreateAndSetup();
        return new RunnerImpl(this, instance, m_processor);
    }

    Processor* processor() {
        DCHECK_NOTNULL(m_processor);
        return m_processor;
    }

private:
    class RunnerImpl : public Runner {
    public:
        RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance, Processor* processor) :
                Runner(factory, instance), m_processor(processor), m_counter(NULL),
                m_inputs(mask().count(), NULL) {
            #ifndef NDEBUG
            std::string counter_name =
                CounterSession::COUNTER_DEFAULT_PREFIX + "|" + factory->identity();
            m_counter = CounterSession::GlobalCounterSession()->GetCounter(counter_name);
            #endif
        }

        virtual void Prepare(const std::vector<toft::StringPiece>& keys) {
            m_processor->BeginGroup(keys, m_inputs, this);
        }

        virtual void Run(Record* record) {
            #ifndef NDEBUG
            m_counter->Update(1);
            #endif

            m_processor->Process(0, record->object);
        }

        virtual void Finish() {
            m_processor->EndGroup();
        }

    private:
        boost::scoped_ptr<Processor> m_processor;
        Counter* m_counter;
        std::vector<Iterator*> m_inputs;
    };

    Entity<Processor> m_entity;
    Processor* m_processor;
};

// each input of one process_node has a dedicated runner
// this is for the other inputs of process_node
class EntityDag::SideProcessorRunnerFactory : public RunnerFactory {
public:
    SideProcessorRunnerFactory(PrimaryProcessorRunnerFactory* leader, uint32_t index) :
            m_leader(leader), m_index(index) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(this, instance, m_leader->processor(), m_index);
    }

private:
    class RunnerImpl : public Runner {
    public:
        RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance,
                   Processor* processor, uint32_t index) :
                Runner(factory, instance), m_processor(processor), m_index(index) {}

        virtual void Run(Record* record) {
            m_processor->Process(m_index, record->object);
        }

    private:
        Processor* m_processor;
        uint32_t m_index;
    };

    PrimaryProcessorRunnerFactory* m_leader;
    uint32_t m_index;
};

void EntityDag::SetupForProcessNode(const PbProcessNode& node,
                                    std::multimap<std::string, uint32_t>* downstreams) {
    PrimaryProcessorRunnerFactory* leader =
            new PrimaryProcessorRunnerFactory(node.processor());

    for (int i = 0; i < node.input_size(); ++i) {
        downstreams->insert(std::make_pair(node.input(i).from(), m_runner_factories.size()));
        if (i == 0) {
            m_runner_factories.push_back(leader);
        } else {
            m_runner_factories.push_back(new SideProcessorRunnerFactory(leader, i));
        }
    }
}

class EntityDag::KeyReaderRunnerFactory : public RunnerFactory {
public:
    explicit KeyReaderRunnerFactory(const PbEntity& entity, int32_t scope_level)
            : m_entity(entity), m_scope_level(scope_level) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(this, instance, m_entity.CreateAndSetup(), m_scope_level);
    }

private:
    class RunnerImpl : public Runner {
    public:
        RunnerImpl(const RunnerFactory* factory,
                   InstanceImpl* instance,
                   KeyReader* key_reader,
                   int32_t scope_level) :
                Runner(factory, instance), m_key_reader(key_reader), m_scope_level(scope_level) {}

        virtual void Run(Record* record) {
            boost::scoped_array<char> tmp_buffer;

            char* key_buffer = m_buffer;
            size_t key_size = m_key_reader->ReadKey(record->object, key_buffer, kBufferSize);
            if (key_size > kBufferSize) {
                size_t buffer_size = key_size;
                tmp_buffer.reset(new char[buffer_size]);
                key_buffer = tmp_buffer.get();

                size_t new_size = m_key_reader->ReadKey(record->object, key_buffer, buffer_size);
                CHECK_LE(new_size, buffer_size);
                key_size = new_size;
            }

            CHECK_GE(record->keys.size(), m_scope_level);

            *tmp_record() = *record;
            tmp_record()->keys.resize(m_scope_level + 1);
            tmp_record()->keys[m_scope_level] = toft::StringPiece(key_buffer, key_size);
            Dispatch(tmp_record());
        }

    private:
        static const size_t kBufferSize = 32 * 1024;  // 32k should be enough

        char m_buffer[kBufferSize];
        boost::scoped_ptr<KeyReader> m_key_reader;
        int32_t m_scope_level;
    };

    Entity<KeyReader> m_entity;
    int32_t m_scope_level;
};

class EntityDag::PartitionerRunnerFactory : public RunnerFactory {
public:
    PartitionerRunnerFactory(uint32_t bucket_size, const PbEntity& entity, int32_t scope_level) :
            m_bucket_size(bucket_size), m_entity(entity), m_scope_level(scope_level) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(this, instance, m_bucket_size, m_entity.CreateAndSetup(),
                              m_scope_level);
    }

private:
    class RunnerImpl : public Runner {
    public:
        RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance,
                   uint32_t bucket_size, Partitioner* partitioner,
                   int32_t scope_level) :
                Runner(factory, instance),
                m_bucket_size(bucket_size), m_partitioner(partitioner),
                m_scope_level(scope_level){}

        virtual void Run(Record* record) {
            uint32_t partition = m_partitioner->Partition(record->object, m_bucket_size);
            DCHECK_LT(partition, m_bucket_size);
            m_buffer = core::EncodePartition(partition);

            CHECK_GE(record->keys.size(), m_scope_level);
            *tmp_record() = *record;
            tmp_record()->keys.resize(m_scope_level + 1);
            tmp_record()->keys[m_scope_level] = m_buffer;
            Dispatch(tmp_record());
        }

    private:
        uint32_t m_bucket_size;
        std::string m_buffer;
        boost::scoped_ptr<Partitioner> m_partitioner;
        int32_t m_scope_level;
    };

    uint32_t m_bucket_size;
    Entity<Partitioner> m_entity;
    int32_t m_scope_level;
};

class EntityDag::HashRunnerFactory : public RunnerFactory {
public:
    HashRunnerFactory(uint32_t bucket_size, const PbEntity& entity, int32_t scope_level) :
            m_bucket_size(bucket_size), m_entity(entity), m_scope_level(scope_level) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(
            this, instance, m_bucket_size, m_entity.CreateAndSetup(), m_scope_level);
    }

private:
    class RunnerImpl : public Runner {
    public:
        RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance,
                   uint32_t bucket_size, Objector* objector,
                   int32_t scope_level) :
            Runner(factory, instance), m_bucket_size(bucket_size), m_objector(objector),
            m_scope_level(scope_level) {}

        virtual void Run(Record* record) {
            boost::scoped_array<char> tmp_buffer;

            char* buffer = m_buffer;
            size_t size = m_objector->Serialize(record->object, buffer, kBufferSize);
            if (size > kBufferSize) {
                size_t buffer_size = size;
                tmp_buffer.reset(new char[buffer_size]);
                buffer = tmp_buffer.get();

                size_t new_size = m_objector->Serialize(record->object, buffer, buffer_size);
                CHECK_LE(new_size, buffer_size);
                size = new_size;
            }

            uint32_t partition = toft::CityHash64(buffer, size) % m_bucket_size;
            m_key = core::EncodePartition(partition);

            CHECK_GE(record->keys.size(), m_scope_level);
            *tmp_record() = *record;
            tmp_record()->keys.resize(m_scope_level + 1);
            tmp_record()->keys[m_scope_level] = m_key;
            Dispatch(tmp_record());
        }

    private:
        static const size_t kBufferSize = 2 * 1024 * 1024;  // 2M should be enough for value

        uint32_t m_bucket_size;
        char m_buffer[kBufferSize];
        std::string m_key;
        boost::scoped_ptr<Objector> m_objector;
        int32_t m_scope_level;
    };

    uint32_t m_bucket_size;
    Entity<Objector> m_entity;
    int32_t m_scope_level;
};

void EntityDag::SetupForShuffleNode(const PbScope& scope,
                                    const PbShuffleNode& node, const PbEntity& objector,
                                    std::multimap<std::string, uint32_t>* downstreams,
                                    int32_t scope_level) {
    CHECK_NE(node.type(), PbShuffleNode::BROADCAST);
    downstreams->insert(std::make_pair(node.from(), m_runner_factories.size()));
    switch (scope.type()) {
        case PbScope::GROUP: {
            m_runner_factories.push_back(
                    new KeyReaderRunnerFactory(node.key_reader(), scope_level));
            break;
        }
        case PbScope::BUCKET: {
            if (node.has_partitioner()) {
                m_runner_factories.push_back(
                        new PartitionerRunnerFactory(scope.bucket_scope().bucket_size(),
                                                     node.partitioner(), scope_level));
            } else {
                m_runner_factories.push_back(
                        new HashRunnerFactory(scope.bucket_scope().bucket_size(), objector,
                            scope_level));
            }
            break;
        }
        default: {
            LOG(FATAL) << "unsupported scope " << scope.type();
        }
    }
}

void EntityDag::SetupForUnionNode(const std::string& identity, const PbUnionNode& node,
                                  std::multimap<std::string, uint32_t>* downstreams) {
    std::set<uint32_t> targets;

    std::pair<std::string, uint32_t> pair;
    BOOST_FOREACH(pair, downstreams->equal_range(identity)) {
        targets.insert(pair.second);
    }
    downstreams->erase(identity);

    for (int i = 0; i < node.from_size(); ++i) {
        BOOST_FOREACH(uint32_t target, targets) {
            downstreams->insert(std::make_pair(node.from(i), target));
        }
    }
}

class EntityDag::SinkerRunnerFactory : public RunnerFactory {
public:
    explicit SinkerRunnerFactory(const PbEntity& entity) : m_entity(entity) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(this, instance, m_entity.CreateAndSetup());
    }

private:
    class RunnerImpl : public Runner {
    public:
        explicit RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance,
                            Sinker* sinker) : Runner(factory, instance), m_sinker(sinker) {}

        virtual void Prepare(const std::vector<toft::StringPiece>& keys) {
            m_sinker->Open(keys);
        }

        virtual void Run(Record* record) {
            m_sinker->Sink(record->object);
        }

        virtual void Finish() {
            m_sinker->Close();
        }

    private:
        boost::scoped_ptr<Sinker> m_sinker;
    };

    Entity<Sinker> m_entity;
};

void EntityDag::SetupForSinkNode(const PbSinkNode& node,
                                 std::multimap<std::string, uint32_t>* downstreams) {
    downstreams->insert(std::make_pair(node.from(), m_runner_factories.size()));
    m_runner_factories.push_back(new SinkerRunnerFactory(node.sinker()));
}

class EntityDag::ListenerRunnerFactory : public RunnerFactory {
public:
    explicit ListenerRunnerFactory(Listener* listener) : m_listener(listener) {}

    virtual Runner* NewRunner(InstanceImpl* instance) {
        return new RunnerImpl(this, instance, m_listener);
    }

private:
    class RunnerImpl : public Runner {
    public:
        explicit RunnerImpl(const RunnerFactory* factory, InstanceImpl* instance,
                            Listener* listener) :
                Runner(factory, instance), m_listener(listener) {}

        virtual void Run(Record* record) {
            m_listener->Run(record->keys, record->object);
        }

    private:
        Listener* m_listener;
    };

    Listener* m_listener;
};

void EntityDag::SetupForListener(const std::string& identity, Listener* listener,
                                 std::multimap<std::string, uint32_t>* downstreams) {
    downstreams->insert(std::make_pair(identity, m_runner_factories.size()));
    m_runner_factories.push_back(new ListenerRunnerFactory(listener));
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
