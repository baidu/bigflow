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

#include "flume/runtime/common/partial_executor.h"

#include <algorithm>
#include <list>
#include <string>
#include <vector>

#include "boost/assign.hpp"
#include "boost/foreach.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/logical_plan.h"
#include "flume/core/testing/string_key_reader.h"
#include "flume/core/testing/string_objector.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/testing/fake_objector.h"
#include "flume/runtime/testing/mock_source.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Expectation;
using ::testing::ExpectationSet;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::Sequence;

using ::boost::assign::list_of;

using core::Entity;
using core::Objector;
using core::LogicalPlan;

class PartialExecutorTest : public ::testing::Test {
public:
    typedef LogicalPlan::Node Node;

    class MockExecutor : public Executor {
    public:
        MOCK_METHOD1(Start, void(const std::string& key));
        MOCK_METHOD3(Got, void(int, const std::vector<std::string>&, const std::string&));
        MOCK_METHOD0(Done, void());
    };

    LogicalPlan* plan() { return &m_plan; }

    LogicalPlan::LoadNode* AddInput(MockSource* source) {
        LogicalPlan::LoadNode* node = plan()->Load("")->As<StringObjector>();
        m_message.add_input(node->identity());
        m_inputs[node->identity()] = source;

        source->SetObjector(node->objector().CreateAndSetup());
        EXPECT_CALL(*source, RequireObject(_, _));
        return node;
    }

    void AddDirectOutput(LogicalPlan::Node* node) {
        AddOutput(node)->set_need_buffer(false);
    }

    void AddSortedOutput(LogicalPlan::Node* node, const std::list<uint32_t>& priorities) {
        AddOutput(node, priorities)->set_need_buffer(true);
    }

    MockExecutor& StartTest(PartialExecutor* executor) {
        for (int i = 0; i < m_plan.node_size(); ++i) {
            if (m_plan.node(i)->type() == PbLogicalPlanNode::LOAD_NODE) {
                continue;
            }
            *m_message.mutable_partial_executor()->add_node() = m_plan.node(i)->ToProtoMessage();
        }

        for (int i = 0; i < m_plan.scope_size(); ++i) {
            *m_message.mutable_partial_executor()->add_scope() = m_plan.scope(i)->ToProtoMessage();
        }

        executor->Initialize(m_message, m_childs, 0, NULL);
        executor->Setup(m_inputs);
        return m_child;
    }

    void AddScopeLevel(int32_t scope_level) {
        m_message.mutable_partial_executor()->add_scope_level(scope_level);
    }

protected:
    virtual void SetUp() {
        m_message.set_type(PbExecutor::PARTIAL);
        m_message.set_scope_level(0);

        m_childs.push_back(&m_child);
        PbExecutor* child = m_message.add_child();
        child->set_type(PbExecutor::EXTERNAL);
        child->set_scope_level(0);
    }

    PbPartialExecutor::Output* AddOutput(LogicalPlan::Node* node,
                                         std::list<uint32_t> priorities = std::list<uint32_t>()) {
        m_child.ListenTo(node->identity());
        m_message.mutable_child(0)->add_input(node->identity());

        PbPartialExecutor::Output* output = m_message.mutable_partial_executor()->add_output();
        output->set_identity(node->identity());
        if (node->objector().empty()) {
            *output->mutable_objector() =
                    Entity<Objector>::Of<StringObjector>("").ToProtoMessage();
        } else {
            *output->mutable_objector() = node->objector().ToProtoMessage();
        }
        BOOST_FOREACH(uint32_t priority, priorities) {
            output->add_priority(priority);
        }

        PbExecutor::Dispatcher* dispatcher = m_message.add_dispatcher();
        dispatcher->set_identity(node->identity());
        dispatcher->mutable_objector()->CopyFrom(output->objector());
        dispatcher->set_usage_count(1);
        dispatcher->set_need_dataset(true);
        if (!priorities.empty()) {
            dispatcher->set_priority(priorities.front());
        }

        return output;
    }

private:
    class MockExecutorImpl : public MockExecutor {
    public:
        typedef boost::dynamic_bitset<> Bitset;

        void ListenTo(const std::string& input) {
            m_inputs.push_back(input);
            m_active_bitset.push_back(false);
        }

        virtual void Setup(const std::map<std::string, Source*>& sources) {
            for (size_t i = 0; i < m_inputs.size(); ++i) {
                std::map<std::string, Source*>::const_iterator ptr = sources.find(m_inputs[i]);
                ASSERT_TRUE(ptr != sources.end()) << "can not find " << m_inputs[i];
                ptr->second->RequireStream(
                    Source::REQUIRE_KEY | Source::REQUIRE_BINARY,
                    toft::NewPermanentClosure(this, &MockExecutorImpl::OnInputCome, i),
                    toft::NewPermanentClosure(this, &MockExecutorImpl::OnInputDone, i)
                );
            }
        }

        virtual Source* GetSource(const std::string& id, unsigned scope_level) {
            LOG(FATAL) << "GetSource should not be called.";
            return NULL;
        }

        virtual void BeginGroup(const toft::StringPiece& key) {
            m_keys.push_back(key);
        }

        virtual void FinishGroup() {
            m_active_bitset.set();
            CHECK_EQ(m_keys.size(), 1u);
            Start(m_keys.front().as_string());
        }

        void OnInputCome(int index, const std::vector<toft::StringPiece>& keys,
                         void* object, const toft::StringPiece& binary) {
            std::vector<std::string> copied_keys;
            for (size_t i = 0; i < keys.size(); ++i) {
                copied_keys.push_back(keys[i].as_string());
            }

            Got(index, copied_keys, binary.as_string());
        }

        void OnInputDone(int index) {
            m_active_bitset[index] = false;
            if (m_active_bitset.none()) {
                Done();
                m_keys.pop_back();
            }
        }

        std::vector<std::string> m_inputs;
        std::vector<toft::StringPiece> m_keys;
        Bitset m_active_bitset;
    };

    core::LogicalPlan m_plan;
    MockExecutorImpl m_child;

    PbExecutor m_message;
    std::map<std::string, Source*> m_inputs;
    std::vector<Executor*> m_childs;
};

TEST_F(PartialExecutorTest, DirectOutput) {
    MockSource source_0;
    AddDirectOutput(AddInput(&source_0));

    MockSource source_1;
    StringKeyReader& first_key = StringKeyReader::Mock();
    StringKeyReader& second_key = StringKeyReader::Mock();
    AddDirectOutput(AddInput(&source_1)
            ->GroupBy<StringKeyReader>(first_key.config())
            ->GroupBy<StringKeyReader>(second_key.config())
    );
    AddScopeLevel(1);
    AddScopeLevel(2);

    std::string r0_0 = "0_0";
    std::string r0_1 = "0_1";

    std::string r1_0 = "1_0";
    first_key.KeyOf(r1_0) = "c";
    second_key.KeyOf(r1_0) = "d";

    std::string r1_1 = "1_1";
    first_key.KeyOf(r1_1) = "a";
    second_key.KeyOf(r1_1) = "b";

    PartialExecutor executor;
    MockExecutor& listener = StartTest(&executor);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, Start("test"));
        EXPECT_CALL(listener, Got(0, ElementsAre("test"), r0_0));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "c", "d"), r1_0));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "a", "b"), r1_1));
        EXPECT_CALL(listener, Got(0, ElementsAre("test"), r0_1));
        EXPECT_CALL(listener, Done());
    }

    executor.BeginGroup("test");
    executor.FinishGroup();
    source_0.Dispatch(r0_0);
    source_1.Dispatch(r1_0);
    source_1.Dispatch(r1_1);
    source_1.DispatchDone();
    source_0.Dispatch(r0_1);
    source_0.DispatchDone();
}

TEST_F(PartialExecutorTest, SortedOutput) {
    MockSource source;
    Node* node_0 = AddInput(&source);
    AddSortedOutput(node_0, list_of(0));

    StringKeyReader& first_key = StringKeyReader::Mock();
    Node* node_1 = node_0->GroupBy<StringKeyReader>(first_key.config());
    AddSortedOutput(node_1, list_of(1)(0));
    AddScopeLevel(1);
    StringKeyReader& second_key = StringKeyReader::Mock();
    Node* node_2 = node_1->GroupBy<StringKeyReader>(second_key.config());
    AddSortedOutput(node_2, list_of(1)(1)(0));
    AddScopeLevel(2);

    std::string r0 = "google";
    first_key.KeyOf(r0) = "g";
    second_key.KeyOf(r0) = "6";

    std::string r1 = "amazon";
    first_key.KeyOf(r1) = "a";
    second_key.KeyOf(r1) = "6";

    std::string r2 = "apple";
    first_key.KeyOf(r2) = "a";
    second_key.KeyOf(r2) = "5";

    PartialExecutor executor;
    MockExecutor& listener = StartTest(&executor);
    {
        Expectation start = EXPECT_CALL(listener, Start("test"));

        ExpectationSet first;
        first += EXPECT_CALL(listener, Got(0, ElementsAre("test"), "google")).After(start);
        first += EXPECT_CALL(listener, Got(0, ElementsAre("test"), "amazon")).After(start);
        first += EXPECT_CALL(listener, Got(0, ElementsAre("test"), "apple")).After(start);

        ExpectationSet second;
        second += EXPECT_CALL(listener, Got(1, ElementsAre("test", "a"), "amazon")).After(first);
        second += EXPECT_CALL(listener, Got(1, ElementsAre("test", "a"), "apple")).After(first);

        Expectation third = EXPECT_CALL(
                listener, Got(2, ElementsAre("test", "a", "5"), "apple")
        ).After(second);

        Expectation forth = EXPECT_CALL(
                listener, Got(2, ElementsAre("test", "a", "6"), "amazon")
        ).After(third);

        Expectation fifth = EXPECT_CALL(
                listener, Got(1, ElementsAre("test", "g"), "google")
        ).After(forth);

        Expectation sixth = EXPECT_CALL(
                listener, Got(2, ElementsAre("test", "g", "6"), "google")
        ).After(fifth);

        EXPECT_CALL(listener, Done()).After(sixth);
    }

    executor.BeginGroup("test");
    executor.FinishGroup();
    source.Dispatch(r0);
    source.Dispatch(r1);
    source.Dispatch(r2);
    source.DispatchDone();
}

TEST_F(PartialExecutorTest, OutOfBuffer) {
    static const size_t BIG_SIZE = 768 * 1024;  // 0.75MB, double of which will exceeds 1MB
    static const std::string SMALL = "-small";
    static const std::string BIG = "-big" + std::string(BIG_SIZE, '\0');

    MockSource source_0;
    StringKeyReader& unordered_key = StringKeyReader::Mock();
    AddDirectOutput(AddInput(&source_0)->GroupBy<StringKeyReader>(unordered_key.config()));
    AddScopeLevel(1);

    MockSource source_1;
    StringKeyReader& ordered_key = StringKeyReader::Mock();
    AddSortedOutput(AddInput(&source_1)->GroupBy<StringKeyReader>(ordered_key.config()),
                    list_of(0)(0));
    AddScopeLevel(1);

    std::string small_0 = "0" + SMALL;
    ordered_key.KeyOf(small_0) = "c";

    std::string small_1 = "1" + SMALL;
    ordered_key.KeyOf(small_1) = "a";

    std::string big_value_0 = "0" + BIG;
    ordered_key.KeyOf(big_value_0) = "b";

    std::string big_value_1 = "1" + BIG;
    ordered_key.KeyOf(big_value_1) = "d";

    std::string big_key = "2" + SMALL;
    ordered_key.KeyOf(big_key) = "b" + BIG;

    std::string unordered_small = "unordered" + SMALL;
    unordered_key.KeyOf(unordered_small) = BIG;  // unordered key will not consume partial buffer

    std::string unordered_big = "unordered" + BIG;
    unordered_key.KeyOf(unordered_big) = SMALL;

    PartialExecutor executor(1);  // 1MB buffer
    MockExecutor& listener = StartTest(&executor);
    {
        InSequence in_sequence;

        EXPECT_CALL(listener, Start("test"));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "a"), small_1));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "b"), big_value_0));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "c"), small_0));
        EXPECT_CALL(listener, Done());

        EXPECT_CALL(listener, Start("test"));
        EXPECT_CALL(listener, Got(0, ElementsAre("test", BIG), unordered_small));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "a"), small_1));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "c"), small_0));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "d"), big_value_1));
        EXPECT_CALL(listener, Done());

        EXPECT_CALL(listener, Start("test"));
        EXPECT_CALL(listener, Got(0, ElementsAre("test", SMALL), unordered_big));
        EXPECT_CALL(listener, Got(1, ElementsAre("test", "b" + BIG), big_key));
        EXPECT_CALL(listener, Done());
    }

    executor.BeginGroup("test");
    executor.FinishGroup();
    source_1.Dispatch(small_0);
    source_1.Dispatch(small_1);
    source_1.Dispatch(big_value_0);
    source_1.Dispatch(big_value_1);
    source_1.Dispatch(small_0);
    source_1.Dispatch(small_1);
    source_0.Dispatch(unordered_small);
    source_1.Dispatch(big_key);
    source_0.Dispatch(unordered_big);
    source_0.DispatchDone();
    source_1.DispatchDone();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
