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

#include "flume/runtime/common/entity_dag.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/logical_plan.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/core/testing/string_objector.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/fake_pointer.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::LogicalPlan;

using testing::_;
using testing::DoAll;
using testing::ElementsAre;
using testing::Lt;
using testing::IsNull;
using testing::InSequence;
using testing::ResultOf;

static const std::vector<toft::StringPiece> kEmptyKeys;

class Listener {
public:
    MOCK_METHOD2(Got, void (const std::vector<std::string>&, void*));  // NOLINT

    EntityDag::Listener* NewListener() const {
        return toft::NewPermanentClosure(const_cast<Listener*>(this), &Listener::Listen);
    }

private:
    void Listen(const std::vector<toft::StringPiece>& keys, void* object) {
        std::vector<std::string> string_keys;
        for (size_t i = 0; i < keys.size(); ++i) {
            string_keys.push_back(keys[i].as_string());
        }
        Got(string_keys, object);
    }
};

class EntityDagTest : public ::testing::Test {
protected:
    typedef LogicalPlan::Node Node;

    LogicalPlan* plan() { return &m_plan; }

    int AsInput(LogicalPlan::Node* node) {
        m_inputs.push_back(node->identity());
        return m_inputs.size() - 1;
    }

    void Listen(const Listener& listener, LogicalPlan::Node* node) {
        m_listeners.insert(node->identity(),
                           std::auto_ptr<EntityDag::Listener>(listener.NewListener()));
    }

    EntityDag::Instance* StartTest(const char* k0 = NULL, const char* k1 = NULL,
            bool auto_scope_level = true) {
        PbLogicalPlan message = m_plan.ToProtoMessage();
        std::vector<PbLogicalPlanNode> non_input_nodes;
        std::set<std::string> total_inputs(m_inputs.begin(), m_inputs.end());
        int32_t last_scope_level = 0;
        for (int i = 0; i < message.node_size(); ++i) {
            if (total_inputs.count(message.node(i).id()) == 0) {
                non_input_nodes.push_back(message.node(i));
                if (auto_scope_level) {
                    if (message.node(i).type() == PbLogicalPlanNode::SHUFFLE_NODE) {
                        m_scope_levels.push_back(++last_scope_level);
                    } else {
                        m_scope_levels.push_back(last_scope_level);
                    }
                }
            }
        }
        m_dag.Initialize(message.scope().begin(), message.scope().end(),
                         non_input_nodes.begin(), non_input_nodes.end(),
                         m_scope_levels.begin(), m_scope_levels.end(),
                         m_inputs.begin(), m_inputs.end(),
                         &m_listeners);
        CHECK_EQ(m_listeners.size(), 0u);

        std::vector<toft::StringPiece> keys;
        const char* args[] = { k0, k1 };
        for (int i = 0; i < 2; ++i) {
            if (args[i] != NULL) {
                keys.push_back(args[i]);
            }
        }
        return m_dag.GetInstance(keys);
        m_scope_levels.clear();
    }

    void AddScopeLevel(int32_t scope_level) {
        m_scope_levels.push_back(scope_level);
    }

private:
    LogicalPlan m_plan;
    std::vector<std::string> m_inputs;
    boost::ptr_multimap<std::string, EntityDag::Listener> m_listeners;

    EntityDag m_dag;
    std::vector<int32_t> m_scope_levels;
};

TEST_F(EntityDagTest, Empty) {
    StartTest()->Done();
}

TEST_F(EntityDagTest, Loader) {
    Listener listener;
    MockLoader& loader = MockLoader::Mock();
    Listen(listener, plan()->Load("file")->By<MockLoader>(loader.config())->As<FakeObjector>());
    {
        InSequence in_sequence;

        EXPECT_CALL(loader, Load("file"))
            .WillOnce(DoAll(EmitAndExpect(&loader, ObjectPtr(1), true),
                            EmitAndExpect(&loader, ObjectPtr(2), true)));
        EXPECT_CALL(listener, Got(ElementsAre("test", "file"), ObjectPtr(1)));
        EXPECT_CALL(listener, Got(ElementsAre("test", "file"), ObjectPtr(2)));
    }

    StartTest("test", "file")->Done();
}

TEST_F(EntityDagTest, Sinker) {
    MockLoader& loader = MockLoader::Mock();
    Node* load_node = plan()->Load("")->By<MockLoader>(loader.config())->As<FakeObjector>();

    MockSinker& sinker = MockSinker::Mock();
    load_node->SinkBy<MockSinker>(sinker.config());
    {
        InSequence in_sequence;

        EXPECT_CALL(sinker, Open(ElementsAre("test")));
        EXPECT_CALL(sinker, Sink(ObjectPtr(0)));
        EXPECT_CALL(sinker, Sink(ObjectPtr(1)));
        EXPECT_CALL(sinker, Close());
    }

    int input = AsInput(load_node);
    EntityDag::Instance* instance = StartTest("test");
    instance->Run(input, ObjectPtr(0));
    instance->Run(input, ObjectPtr(1));
    instance->Done();
}

TEST_F(EntityDagTest, SingleInputProcessor) {
    MockLoader& loader = MockLoader::Mock();
    Node* load_node = plan()->Load("")->By<MockLoader>(loader.config())->As<FakeObjector>();
    int input = AsInput(load_node);

    Listener listener;
    MockProcessor& processor = MockProcessor::Mock();
    Listen(listener, load_node->ProcessBy<MockProcessor>(processor.config())->As<FakeObjector>());

    {
        InSequence in_sequence;

        EXPECT_CALL(processor, BeginGroup(ElementsAre("test"), ElementsAre(IsNull())))
            .WillOnce(EmitAndExpect(&processor, ObjectPtr(0x10), true));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(0x10)));

        EXPECT_CALL(processor, Process(0, ObjectPtr(0)))
            .WillOnce(EmitAndExpect(&processor, ObjectPtr(0x11), true));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(0x11)));

        EXPECT_CALL(processor, EndGroup())
            .WillOnce(EmitAndExpect(&processor, ObjectPtr(0x12), true));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(0x12)));
    }

    EntityDag::Instance* instance = StartTest("test");
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(0)));
    instance->Done();
}

TEST_F(EntityDagTest, MultiInputProcessor) {
    Node* source_0 = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    Node* source_1 = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    int input_0 = AsInput(source_0);
    int input_1 = AsInput(source_1);

    Listener listener;
    MockProcessor& processor = MockProcessor::Mock();
    Listen(listener,
           plan()->Process(source_0, source_1)
                ->By<MockProcessor>(processor.config())->As<FakeObjector>());

    {
        InSequence in_sequence;

        EXPECT_CALL(processor, BeginGroup(ElementsAre("test"),
                                          ElementsAre(IsNull(), IsNull())));

        EXPECT_CALL(processor, Process(0, ObjectPtr(0)))
            .WillOnce(EmitAndExpect(&processor, ObjectPtr(0x10), true));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(0x10)));

        EXPECT_CALL(processor, Process(1, ObjectPtr(1)))
            .WillOnce(DoAll(EmitAndExpect(&processor, ObjectPtr(0x11), true),
                            EmitDone(&processor)));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(0x11)));

        EXPECT_CALL(processor, EndGroup());
    }

    EntityDag::Instance* instance = StartTest("test");
    EXPECT_EQ(true, instance->Run(input_0, ObjectPtr(0)));
    EXPECT_EQ(false, instance->Run(input_1, ObjectPtr(1)));
    instance->Done();
}

TEST_F(EntityDagTest, KeyReader) {
    static const std::string A("a");
    static const std::string B(1024 * 1024, 'b');  // 1MB

    MockLoader& loader = MockLoader::Mock();
    Node* load_node = plan()->Load("")->By<MockLoader>(loader.config())->As<FakeObjector>();
    int input = AsInput(load_node);

    Listener listener;
    MockKeyReader& key_reader = MockKeyReader::Mock();
    Listen(listener, load_node->GroupBy<MockKeyReader>(key_reader.config()));
    {
        key_reader.KeyOf(ObjectPtr(0)) = A;
        key_reader.KeyOf(ObjectPtr(1)) = B;

        EXPECT_CALL(listener, Got(ElementsAre("test", A), ObjectPtr(0)));
        EXPECT_CALL(listener, Got(ElementsAre("test", B), ObjectPtr(1)));
    }

    EntityDag::Instance* instance = StartTest("test");
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(0)));
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(1)));
    instance->Done();
}

TEST_F(EntityDagTest, Partitioner) {
    MockLoader& loader = MockLoader::Mock();
    Node* load_node = plan()->Load("")->By<MockLoader>(loader.config())->As<FakeObjector>();
    int input = AsInput(load_node);

    Listener listener;
    MockPartitioner& partitioner = MockPartitioner::Mock();
    {
        partitioner.PartitionOf(ObjectPtr(0), 10) = 7;
        partitioner.PartitionOf(ObjectPtr(1), 10) = 3;

        EXPECT_CALL(listener, Got(ElementsAre("test", core::EncodePartition(7)), ObjectPtr(0)));
        EXPECT_CALL(listener, Got(ElementsAre("test", core::EncodePartition(3)), ObjectPtr(1)));
    }
    Listen(listener,
           plan()->Shuffle(load_node->scope(), load_node)->WithConcurrency(10)
                ->node(0)->DistributeBy<MockPartitioner>(partitioner.config()));

    EntityDag::Instance* instance = StartTest("test");
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(0)));
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(1)));
    instance->Done();
}

TEST_F(EntityDagTest, PartitionByHash) {
    std::string a("aaaa");
    std::string b(4 * 1024 * 1024, 'b');

    MockLoader& loader = MockLoader::Mock();
    Node* load_node = plan()->Load("")->By<MockLoader>(loader.config())->As<StringObjector>();
    int input = AsInput(load_node);

    Listener listener;
    Listen(listener, load_node->DistributeInto(10));
    {
        InSequence in_sequence;

        EXPECT_CALL(listener,
                    Got(ElementsAre("test", ResultOf(core::DecodePartition, Lt(10))), &a));
        EXPECT_CALL(listener,
                    Got(ElementsAre("test", ResultOf(core::DecodePartition, Lt(10))), &b));
    }

    EntityDag::Instance* instance = StartTest("test");
    EXPECT_EQ(true, instance->Run(input, &a));
    EXPECT_EQ(true, instance->Run(input, &b));
    instance->Done();
}

TEST_F(EntityDagTest, Union) {
    Node* source_0 = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    Node* source_1 = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    Node* source_2 = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    int input_0 = AsInput(source_0);
    int input_1 = AsInput(source_1);
    int input_2 = AsInput(source_2);

    Listener listener;
    Listen(listener, plan()->Union(source_0, plan()->Union(source_1, source_2)));

    {
        InSequence in_sequence;

        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(0)));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(1)));
        EXPECT_CALL(listener, Got(ElementsAre("test"), ObjectPtr(2)));
    }

    EntityDag::Instance* instance = StartTest("test");
    EXPECT_EQ(true, instance->Run(input_2, ObjectPtr(0)));
    EXPECT_EQ(true, instance->Run(input_0, ObjectPtr(1)));
    EXPECT_EQ(true, instance->Run(input_1, ObjectPtr(2)));
    instance->Done();
}

TEST_F(EntityDagTest, MultiLevel) {
    Node* source = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    int input = AsInput(source);

    MockPartitioner& partitioner = MockPartitioner::Mock();
    MockProcessor& processor_0 = MockProcessor::Mock();
    Node* common = plan()->Shuffle(plan()->global_scope(), source->RemoveScope())
            ->WithConcurrency(10)->node(0)
                ->DistributeBy<MockPartitioner>(partitioner.config())
                ->ProcessBy<MockProcessor>(processor_0.config())->As<FakeObjector>();
    AddScopeLevel(1);
    AddScopeLevel(1);
    AddScopeLevel(1);


    Listener listener_0;
    MockKeyReader& key_reader_0 = MockKeyReader::Mock();
    Listen(listener_0, common->GroupBy<MockKeyReader>(key_reader_0.config()));
    AddScopeLevel(2);

    Listener listener_1;
    MockKeyReader& key_reader_1 = MockKeyReader::Mock();
    MockProcessor& processor_1 = MockProcessor::Mock();
    Listen(listener_1, common
            ->GroupBy<MockKeyReader>(key_reader_1.config())
            ->ProcessBy<MockProcessor>(processor_1.config())->As<FakeObjector>());
    AddScopeLevel(2);
    AddScopeLevel(2);

    {
        InSequence in_sequence;

        EXPECT_CALL(processor_1, BeginGroup(ElementsAre("test"), ElementsAre(IsNull())));
        EXPECT_CALL(processor_0, BeginGroup(ElementsAre("test"), ElementsAre(IsNull())));

        EXPECT_CALL(processor_0, Process(0, ObjectPtr(0)))
            .WillOnce(EmitAndExpect(&processor_0, ObjectPtr(0x100), true));
        EXPECT_CALL(processor_1, Process(0, ObjectPtr(0x100)))
            .WillOnce(DoAll(EmitAndExpect(&processor_1, ObjectPtr(0x200), true),
                            EmitDone(&processor_1)));

        EXPECT_CALL(processor_0, Process(0, ObjectPtr(1)))
            .WillOnce(DoAll(EmitAndExpect(&processor_0, ObjectPtr(0x101), true),
                            EmitDone(&processor_0)));

        EXPECT_CALL(processor_0, EndGroup());
        EXPECT_CALL(processor_1, EndGroup());
    }
    {
        const std::string PARTITION(core::EncodePartition(6));
        partitioner.PartitionOf(ObjectPtr(0), 10) = 6;
        partitioner.PartitionOf(ObjectPtr(1), 10) = 6;

        key_reader_0.KeyOf(ObjectPtr(0x100)) = "a0";
        key_reader_0.KeyOf(ObjectPtr(0x101)) = "a1";
        EXPECT_CALL(listener_0, Got(ElementsAre("test", PARTITION, "a0"), ObjectPtr(0x100)));
        EXPECT_CALL(listener_0, Got(ElementsAre("test", PARTITION, "a1"), ObjectPtr(0x101)));

        key_reader_1.KeyOf(ObjectPtr(0x100)) = "b0";
        EXPECT_CALL(listener_1, Got(ElementsAre("test", PARTITION, "b0"), ObjectPtr(0x200)));
    }

    EntityDag::Instance* instance = StartTest("test", NULL, false);
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(0)));
    EXPECT_EQ(false, instance->Run(input, ObjectPtr(1)));
    EXPECT_EQ(false, instance->Run(input, ObjectPtr(2)));
    instance->Done();
}


TEST_F(EntityDagTest, MultiDownStream) {
    Node* source = plan()->Load("")->By<MockLoader>("")->As<FakeObjector>();
    int input = AsInput(source);

    MockPartitioner& partitioner_1 = MockPartitioner::Mock();
    MockPartitioner& partitioner_2 = MockPartitioner::Mock();

    Node* common = plan()->Shuffle(plan()->global_scope(), source->RemoveScope())
            ->WithConcurrency(10)->node(0)
                ->DistributeBy<MockPartitioner>(partitioner_1.config());
    AddScopeLevel(0);
    AddScopeLevel(1);

    Node* o2 = plan()->Shuffle(plan()->global_scope(), common->RemoveScope())
        ->WithConcurrency(10)->node(0)
            ->DistributeBy<MockPartitioner>(partitioner_2.config());
    AddScopeLevel(0);
    AddScopeLevel(1);

    MockProcessor& processor = MockProcessor::Mock();
    Node* o1 = common->ProcessBy<MockProcessor>(processor.config())->As<FakeObjector>();
    AddScopeLevel(1);

    Listener listener_1;
    Listener listener_2;
    Listen(listener_1, o1);
    Listen(listener_2, o2);

    {
        InSequence in_sequence;

        EXPECT_CALL(processor, BeginGroup(ElementsAre("test"), ElementsAre(IsNull())));

        EXPECT_CALL(processor, Process(0, ObjectPtr(0)))
            .WillOnce(EmitAndExpect(&processor, ObjectPtr(0x100), true));

        EXPECT_CALL(processor, Process(0, ObjectPtr(1)))
            .WillOnce(DoAll(EmitAndExpect(&processor, ObjectPtr(0x200), true),
                            EmitDone(&processor)));

        EXPECT_CALL(processor, EndGroup());
    }
    {
        const std::string PARTITION_6(core::EncodePartition(6));
        const std::string PARTITION_7(core::EncodePartition(7));
        partitioner_1.PartitionOf(ObjectPtr(0), 10) = 6;
        partitioner_1.PartitionOf(ObjectPtr(1), 10) = 7;

        partitioner_2.PartitionOf(ObjectPtr(0), 10) = 7;
        partitioner_2.PartitionOf(ObjectPtr(1), 10) = 6;


        EXPECT_CALL(listener_1, Got(ElementsAre("test", PARTITION_6), ObjectPtr(0x100)));
        EXPECT_CALL(listener_1, Got(ElementsAre("test", PARTITION_7), ObjectPtr(0x200)));
        EXPECT_CALL(listener_2, Got(ElementsAre("test", PARTITION_7), ObjectPtr(0)));
        EXPECT_CALL(listener_2, Got(ElementsAre("test", PARTITION_6), ObjectPtr(1)));
    }

    EntityDag::Instance* instance = StartTest("test", NULL, false);
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(0)));
    EXPECT_EQ(true, instance->Run(input, ObjectPtr(1)));
    instance->Done();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

