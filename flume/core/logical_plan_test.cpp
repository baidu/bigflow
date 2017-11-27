/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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

#include "flume/core/logical_plan.h"

#include <map>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/objector.h"
#include "flume/core/processor.h"
#include "flume/core/sinker.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/planner/graph_helper.h"
#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace core {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::DoAll;
using ::testing::SetArgPointee;

typedef MockLoader TestLoader;
typedef MockSinker TestSinker;
typedef MockPartitioner TestPartitioner;
typedef MockProcessor TestProcessor;
typedef MockKeyReader TestKeyReader;
typedef MockObjector TestObjector;

class LogicalPlanTest : public testing::Test {
protected:
    typedef boost::adjacency_list<
        boost::listS, boost::listS, boost::directedS, PbLogicalPlanNode
    > LogicalDag;

    typedef boost::adjacency_list<
        boost::listS, boost::listS, boost::directedS, PbScope
    > ScopeTree;

protected:
    virtual void SetUp() {
        m_plan.reset(new LogicalPlan());
        ASSERT_EQ(std::string(), m_plan->global_scope()->identity());
    }

    void FinishPlan() {
        PbLogicalPlan message = m_plan->ToProtoMessage();
        message.CheckInitialized();

        planner::BuildLogicalDag(message, &m_logical_dag, &m_nodes);
        planner::BuildScopeTree(message, &m_scope_tree, &m_scopes);
    }

    PbLogicalPlanNode& ProtoOf(const LogicalPlan::Node* node) {
        return m_logical_dag[m_nodes[node->identity()]];
    }

    void ExpectFrom(const LogicalPlan::Node* to, const LogicalPlan::Node* from) {
        typedef boost::graph_traits<LogicalDag>::vertex_descriptor Vertex;

        ASSERT_EQ(1u, m_nodes.count(to->identity()));
        ASSERT_EQ(1u, m_nodes.count(from->identity()));

        Vertex to_vertex = m_nodes[to->identity()];
        Vertex from_vertex = m_nodes[from->identity()];
        ASSERT_TRUE(boost::edge(from_vertex, to_vertex, m_logical_dag).second)
            << "node[" << to->identity() << "] is not from node[" << from->identity() << "]";
    }

    PbScope& ProtoOf(const LogicalPlan::Scope* scope) {
        return m_scope_tree[m_scopes[scope->identity()]];
    }

    void ExpectFrom(const LogicalPlan::Scope* to, const LogicalPlan::Scope* from) {
        typedef boost::graph_traits<ScopeTree>::vertex_descriptor Vertex;

        ASSERT_EQ(1u, m_scopes.count(to->identity()));
        ASSERT_EQ(1u, m_scopes.count(from->identity()));

        Vertex to_vertex = m_scopes[to->identity()];
        Vertex from_vertex = m_scopes[from->identity()];
        ASSERT_TRUE(boost::edge(from_vertex, to_vertex, m_scope_tree).second)
            << "scope[" << to->identity() << "] is not from scope[" << from->identity() << "]";
    }

    void ExpectBelongsTo(const LogicalPlan::Node* node, const LogicalPlan::Scope* scope) {
        EXPECT_EQ(ProtoOf(node).scope(), ProtoOf(scope).id());
    }

protected:
    toft::scoped_ptr<LogicalPlan> m_plan;

    LogicalDag m_logical_dag;
    std::map<std::string, boost::graph_traits<LogicalDag>::vertex_descriptor> m_nodes;

    ScopeTree m_scope_tree;
    std::map<std::string, boost::graph_traits<ScopeTree>::vertex_descriptor> m_scopes;
};


TEST_F(LogicalPlanTest, Load) {
    LogicalPlan::Node* load =
            m_plan->Load("file1", "file2")->By<TestLoader>()->As<TestObjector>();
    load->set_debug_info("test");

    FinishPlan();

    EXPECT_EQ(1u, m_nodes.size());
    EXPECT_EQ(2u, m_scopes.size());

    EXPECT_EQ(PbLogicalPlanNode::LOAD_NODE, ProtoOf(load).type());
    EXPECT_EQ(false, ProtoOf(load).is_infinite());
    EXPECT_EQ("test", ProtoOf(load).debug_info());
    EXPECT_EQ(Entity<Objector>::Of<TestObjector>(""),
              Entity<Objector>::From(ProtoOf(load).objector()));
    EXPECT_THAT(ProtoOf(load).load_node().uri(), ElementsAre("file1", "file2"));
    EXPECT_EQ(Entity<Loader>::Of<TestLoader>(""),
              Entity<Loader>::From(ProtoOf(load).load_node().loader()));

    PbScope scope_message = ProtoOf(load->scope());
    EXPECT_EQ(false, scope_message.is_sorted());
    EXPECT_EQ(false, scope_message.is_infinite());
    EXPECT_EQ(PbScope::INPUT, scope_message.type());
    EXPECT_EQ(Entity<Loader>::Of<TestLoader>(""),
              Entity<Loader>::From(scope_message.input_scope().spliter()));
    EXPECT_THAT(scope_message.input_scope().uri(), ElementsAre("file1", "file2"));
    ExpectFrom(load->scope(), m_plan->global_scope());
    ExpectBelongsTo(load, load->scope());
}

TEST_F(LogicalPlanTest, LoadWithInfinite) {
    std::vector<std::string> uri_list;
    uri_list.push_back(std::string("file1"));
    uri_list.push_back(std::string("file2"));
    LogicalPlan::Node* load = m_plan->RepeatedlyLoad(uri_list)->By<TestLoader>()->As<TestObjector>();

    FinishPlan();

    EXPECT_EQ(true, ProtoOf(load).is_infinite());

    PbScope scope_message = ProtoOf(load->scope());
    EXPECT_EQ(true, scope_message.is_infinite());
}

TEST_F(LogicalPlanTest, Sink) {
    LogicalPlan::Node* load = m_plan->Load("file")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* sink = m_plan->Sink(m_plan->global_scope(), load)->By<TestSinker>();

    FinishPlan();

    EXPECT_EQ(2u, m_nodes.size());
    EXPECT_EQ(2u, m_scopes.size());

    EXPECT_EQ(PbLogicalPlanNode::LOAD_NODE, ProtoOf(load).type());
    EXPECT_EQ(PbLogicalPlanNode::SINK_NODE, ProtoOf(sink).type());
    EXPECT_EQ(false, ProtoOf(sink).is_infinite());
    EXPECT_EQ(Entity<Objector>::Of<TestObjector>(""),
              Entity<Objector>::From(ProtoOf(load).objector()));
    EXPECT_EQ(Entity<Sinker>::Of<TestSinker>(""),
              Entity<Sinker>::From(ProtoOf(sink).sink_node().sinker()));

    EXPECT_EQ(false, ProtoOf(sink->scope()).is_infinite());
    EXPECT_EQ(false, ProtoOf(sink->scope()).is_sorted());
    ExpectFrom(sink, load);
    ExpectBelongsTo(sink, m_plan->global_scope());
}

TEST_F(LogicalPlanTest, SinkWithInfinite) {
    std::vector<std::string> uri_list;
    uri_list.push_back(std::string("file1"));
    LogicalPlan::Node* load = m_plan->RepeatedlyLoad(uri_list)->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* sink = m_plan->Sink(m_plan->global_scope(), load)->By<TestSinker>();

    FinishPlan();

    EXPECT_EQ(true, ProtoOf(sink).is_infinite());
}

TEST_F(LogicalPlanTest, Process) {
    LogicalPlan::Node* source_1 = m_plan->Load("file1")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* source_2 = m_plan->Load("file2")->By<TestLoader>()->As<TestObjector>();

    LogicalPlan::ProcessNode* node_1 =
            source_1->ProcessBy<TestProcessor>()->As<TestObjector>("config1")
                ->input(0)->AllowPartialProcessing()
                ->done();

    LogicalPlan::ProcessNode* node_2 =
            source_1->CombineWith(node_1)->By<TestProcessor>("config2")->As<TestObjector>()
                ->input(0)->AllowPartialProcessing()
                ->input(1)->PrepareBeforeProcessing()
                ->done();

    LogicalPlan::ProcessNode* node_3 = source_1->CombineWith(source_2)
            ->By<TestProcessor>()->As<TestObjector>()->PrepareAtLeast(1);

    FinishPlan();

    EXPECT_EQ(5u, m_nodes.size());
    EXPECT_EQ(3u, m_scopes.size());

    // check node_1
    EXPECT_EQ(PbLogicalPlanNode::PROCESS_NODE, ProtoOf(node_1).type());
    EXPECT_EQ(false, ProtoOf(node_1).is_infinite());
    EXPECT_EQ(Entity<Objector>::Of<TestObjector>("config1"),
              Entity<Objector>::From(ProtoOf(node_1).objector()));
    EXPECT_EQ(Entity<Processor>::Of<TestProcessor>(""),
              Entity<Processor>::From(ProtoOf(node_1).process_node().processor()));
    EXPECT_EQ(1, ProtoOf(node_1).process_node().input_size());
    EXPECT_EQ(0, ProtoOf(node_1).process_node().least_prepared_inputs());
    EXPECT_EQ(true, ProtoOf(node_1).process_node().input(0).is_partial());
    EXPECT_EQ(false, ProtoOf(node_1).process_node().input(0).is_prepared());
    ExpectBelongsTo(node_1, source_1->scope());
    ExpectFrom(node_1, source_1);

    // check node_2
    EXPECT_EQ(PbLogicalPlanNode::PROCESS_NODE, ProtoOf(node_2).type());
    EXPECT_EQ(false, ProtoOf(node_2).is_infinite());
    EXPECT_EQ(Entity<Objector>::Of<TestObjector>(""),
              Entity<Objector>::From(ProtoOf(node_2).objector()));
    EXPECT_EQ(Entity<Processor>::Of<TestProcessor>("config2"),
              Entity<Processor>::From(ProtoOf(node_2).process_node().processor()));
    EXPECT_EQ(2, ProtoOf(node_2).process_node().input_size());
    EXPECT_EQ(0, ProtoOf(node_2).process_node().least_prepared_inputs());
    EXPECT_EQ(true, ProtoOf(node_2).process_node().input(0).is_partial());
    EXPECT_EQ(false, ProtoOf(node_2).process_node().input(0).is_prepared());
    EXPECT_EQ(false, ProtoOf(node_2).process_node().input(1).is_partial());
    EXPECT_EQ(true, ProtoOf(node_2).process_node().input(1).is_prepared());
    ExpectBelongsTo(node_2, source_1->scope());
    ExpectFrom(node_2, node_1);
    ExpectFrom(node_2, source_1);

    // check node_3
    EXPECT_EQ(PbLogicalPlanNode::PROCESS_NODE, ProtoOf(node_3).type());
    EXPECT_EQ(false, ProtoOf(node_3).is_infinite());
    EXPECT_EQ(Entity<Objector>::Of<TestObjector>(""),
              Entity<Objector>::From(ProtoOf(node_3).objector()));
    EXPECT_EQ(Entity<Processor>::Of<TestProcessor>(""),
              Entity<Processor>::From(ProtoOf(node_3).process_node().processor()));
    EXPECT_EQ(2, ProtoOf(node_3).process_node().input_size());
    EXPECT_EQ(1, ProtoOf(node_3).process_node().least_prepared_inputs());
    EXPECT_EQ(false, ProtoOf(node_3).process_node().input(0).is_partial());
    EXPECT_EQ(false, ProtoOf(node_3).process_node().input(0).is_prepared());
    EXPECT_EQ(false, ProtoOf(node_3).process_node().input(1).is_partial());
    EXPECT_EQ(false, ProtoOf(node_3).process_node().input(1).is_prepared());
    ExpectBelongsTo(node_3, m_plan->global_scope());
    ExpectFrom(node_3, source_1);
    ExpectFrom(node_3, source_2);
}

TEST_F(LogicalPlanTest, ProcessWithInfinite) {
    std::vector<std::string> uri_list;
    uri_list.push_back(std::string("file1"));
    LogicalPlan::Node* source_1 =
            m_plan->RepeatedlyLoad(uri_list)->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* source_2 = m_plan->Load("file2")->By<TestLoader>()->As<TestObjector>();

    LogicalPlan::ProcessNode* node_1 =
            source_1->ProcessBy<TestProcessor>()->As<TestObjector>("config1")
                ->input(0)->AllowPartialProcessing()
                ->done();

    LogicalPlan::ProcessNode* node_2 =
            source_1->CombineWith(node_1)->By<TestProcessor>("config2")->As<TestObjector>()
                ->input(0)->AllowPartialProcessing()
                ->input(1)->PrepareBeforeProcessing()
                ->done();

    LogicalPlan::ProcessNode* node_3 = source_1->CombineWith(source_2)
            ->By<TestProcessor>()->As<TestObjector>()->PrepareAtLeast(1);

    FinishPlan();

    // check node_1
    EXPECT_EQ(true, ProtoOf(node_1).is_infinite());

    // check node_2
    EXPECT_EQ(true, ProtoOf(node_2).is_infinite());

    // check node_3
    EXPECT_EQ(true, ProtoOf(node_3).is_infinite());
}

TEST_F(LogicalPlanTest, ShuffleByKey) {
    LogicalPlan::Node* start_0_1 = m_plan->Load("file1")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* node_0_1 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("1");
    LogicalPlan::Node* node_0_2 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("2");
    LogicalPlan::Node* node_0_3 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("3");

    LogicalPlan::ShuffleGroup* shuffle_1 =
            m_plan->Shuffle(start_0_1->scope(), node_0_1, node_0_2, node_0_3);
    LogicalPlan::ShuffleNode* start_1_1 = shuffle_1->node(0)->MatchBy<TestKeyReader>("1");
    LogicalPlan::ShuffleNode* start_1_2 = shuffle_1->node(1)->MatchBy<TestKeyReader>("2");
    LogicalPlan::ShuffleNode* start_1_3 = shuffle_1->node(2)->MatchBy<TestKeyReader>("3");

    LogicalPlan::Node* node_1_1 = start_1_1->SortBy<TestKeyReader>();  // implicit shuffle
    LogicalPlan::Node* node_1_2 = start_1_2;
    LogicalPlan::Node* start_2_1 = start_1_3->GroupBy<TestKeyReader>();          // shuffle 2
    LogicalPlan::Node* start_3_1 = start_1_3->SortAndGroupBy<TestKeyReader>();   // shuffle 3

    LogicalPlan::ShuffleGroup* shuffle_4 =
            m_plan->Shuffle(shuffle_1->scope(), node_1_1, node_1_2)
                ->node(0)->MatchBy<TestKeyReader>()
                ->node(1)->MatchAny()
            ->done();

    LogicalPlan::ShuffleGroup* shuffle_5 =
            m_plan->Shuffle(m_plan->global_scope(), node_1_1, node_1_2)
                ->node(0)->MatchBy<TestKeyReader>()
                ->node(1)->MatchAny()
            ->done();

    FinishPlan();

    EXPECT_EQ(15u, m_nodes.size());
    EXPECT_EQ(8u, m_scopes.size());

    // check shuffle_1
    {
        EXPECT_EQ(PbScope::GROUP, ProtoOf(shuffle_1->scope()).type());
        EXPECT_EQ(false, ProtoOf(shuffle_1->scope()).is_infinite());
        EXPECT_EQ(false, ProtoOf(shuffle_1->scope()).is_sorted());
        EXPECT_EQ(false, ProtoOf(shuffle_1->scope()).has_concurrency());
        ExpectFrom(shuffle_1->scope(), start_0_1->scope());

        LogicalPlan::Node* checklist[] = {
            node_0_1, start_1_1, node_0_2, start_1_2, node_0_3, start_1_3
        };
        for (int i = 0; i < 3; ++i) {
            LogicalPlan::Node* from = checklist[i * 2];
            LogicalPlan::Node* node = checklist[i * 2 + 1];

            EXPECT_EQ(false, ProtoOf(from).is_infinite());
            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node).type());
            EXPECT_EQ(PbShuffleNode::KEY, ProtoOf(node).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>(std::string(1, '1' + i)),
                      Entity<Objector>::From(ProtoOf(node).objector()));
            EXPECT_EQ(Entity<KeyReader>::Of<TestKeyReader>(std::string(1, '1' + i)),
                      Entity<KeyReader>::From(ProtoOf(node).shuffle_node().key_reader()));

            ExpectBelongsTo(node, shuffle_1->scope());
            ExpectFrom(node, from);
        }
    }

    // check implicit shuffle
    {
        const LogicalPlan::UnionNode* handle = dynamic_cast<LogicalPlan::UnionNode*>(node_1_1);
        const LogicalPlan::ShuffleNode* node =
                dynamic_cast<const LogicalPlan::ShuffleNode*>(handle->from(0));

        EXPECT_EQ(PbLogicalPlanNode::UNION_NODE, ProtoOf(handle).type());
        EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node).type());
        EXPECT_EQ(PbShuffleNode::KEY, ProtoOf(node).shuffle_node().type());
        EXPECT_EQ(false, ProtoOf(node).is_infinite());
        EXPECT_EQ(Entity<Objector>::Of<TestObjector>("1"),
                  Entity<Objector>::From(ProtoOf(node).objector()));
        EXPECT_EQ(Entity<KeyReader>::Of<TestKeyReader>(""),
                  Entity<KeyReader>::From(ProtoOf(node).shuffle_node().key_reader()));

        EXPECT_EQ(PbScope::GROUP, ProtoOf(node->scope()).type());
        EXPECT_EQ(false, ProtoOf(node->scope()).is_infinite());
        EXPECT_EQ(true, ProtoOf(node->scope()).is_sorted());
        EXPECT_EQ(false, ProtoOf(node->scope()).has_concurrency());

        EXPECT_EQ(false, ProtoOf(handle).is_infinite());
        ExpectFrom(handle, node);
        ExpectFrom(node, start_1_1);
        ExpectFrom(node->scope(), shuffle_1->scope());

        ExpectBelongsTo(handle, shuffle_1->scope());
        ExpectBelongsTo(node, node->scope());
    }

    // check shuffle 2/3
    {
        LogicalPlan::Node* checklist[] = {start_2_1, start_3_1};
        for (int i = 0; i < 2; ++i) {
            LogicalPlan::Node* node = checklist[i];

            EXPECT_EQ(PbScope::GROUP, ProtoOf(node->scope()).type());
            EXPECT_EQ(false, ProtoOf(node->scope()).is_infinite());
            EXPECT_EQ(static_cast<bool>(i), ProtoOf(node->scope()).is_sorted());
            EXPECT_EQ(false, ProtoOf(node->scope()).has_concurrency());
            ExpectFrom(node->scope(), shuffle_1->scope());

            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node).type());
            EXPECT_EQ(PbShuffleNode::KEY, ProtoOf(node).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>("3"),
                      Entity<Objector>::From(ProtoOf(node).objector()));
            EXPECT_EQ(Entity<KeyReader>::Of<TestKeyReader>(""),
                      Entity<KeyReader>::From(ProtoOf(node).shuffle_node().key_reader()));
            ExpectBelongsTo(node, node->scope());
        }
    }

    // check shuffle 4/5
    {
        LogicalPlan::ShuffleGroup* checklist[] = {shuffle_4, shuffle_5};
        const LogicalPlan::Scope* scopes[] = {
            shuffle_4->scope(), shuffle_1->scope(), shuffle_5->scope(), m_plan->global_scope()
        };
        for (int i = 0; i < 2; ++i) {
            LogicalPlan::Node* node_1 = checklist[i]->node(0);
            LogicalPlan::Node* node_2 = checklist[i]->node(1);
            const LogicalPlan::Scope* scope = scopes[2 * i];
            const LogicalPlan::Scope* father_scope = scopes[2 * i + 1];

            EXPECT_EQ(PbScope::GROUP, ProtoOf(scope).type());
            EXPECT_EQ(false, ProtoOf(scope).is_infinite());
            EXPECT_EQ(false, ProtoOf(scope).is_sorted());
            EXPECT_EQ(false, ProtoOf(scope).has_concurrency());
            ExpectFrom(scope, father_scope);

            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node_1).type());
            EXPECT_EQ(PbShuffleNode::KEY, ProtoOf(node_1).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node_1).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>("1"),
                      Entity<Objector>::From(ProtoOf(node_1).objector()));
            EXPECT_EQ(Entity<KeyReader>::Of<TestKeyReader>(""),
                      Entity<KeyReader>::From(ProtoOf(node_1).shuffle_node().key_reader()));
            ExpectBelongsTo(node_1, scope);

            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node_2).type());
            EXPECT_EQ(PbShuffleNode::BROADCAST, ProtoOf(node_2).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node_2).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>("2"),
                      Entity<Objector>::From(ProtoOf(node_2).objector()));
            EXPECT_FALSE(ProtoOf(node_2).shuffle_node().has_key_reader());
            ExpectBelongsTo(node_2, scope);
        }
    }
}

TEST_F(LogicalPlanTest, ShuffleByKeyWithInfinite) {
    std::vector<std::string> uri_list;
    uri_list.push_back(std::string("file1"));
    LogicalPlan::Node* start_0_1 =
            m_plan->RepeatedlyLoad(uri_list)->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* node_0_1 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("1");
    LogicalPlan::Node* node_0_2 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("2");
    LogicalPlan::Node* node_0_3 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("3");

    LogicalPlan::ShuffleGroup* shuffle_1 =
            m_plan->Shuffle(start_0_1->scope(), node_0_1, node_0_2, node_0_3);
    LogicalPlan::ShuffleNode* start_1_1 = shuffle_1->node(0)->MatchBy<TestKeyReader>("1");
    LogicalPlan::ShuffleNode* start_1_2 = shuffle_1->node(1)->MatchBy<TestKeyReader>("2");
    LogicalPlan::ShuffleNode* start_1_3 = shuffle_1->node(2)->MatchBy<TestKeyReader>("3");

    FinishPlan();

    // check shuffle_1
    {
        EXPECT_EQ(true, ProtoOf(shuffle_1->scope()).is_infinite());
        EXPECT_EQ(true, ProtoOf(shuffle_1->scope()).is_stream());

        LogicalPlan::Node* checklist[] = {
            node_0_1, start_1_1, node_0_2, start_1_2, node_0_3, start_1_3
        };
        for (int i = 0; i < 3; ++i) {
            LogicalPlan::Node* from = checklist[i * 2];
            LogicalPlan::Node* node = checklist[i * 2 + 1];

            EXPECT_EQ(true, ProtoOf(from).is_infinite());
            EXPECT_EQ(true, ProtoOf(node).is_infinite());
        }
    }
}

TEST_F(LogicalPlanTest, ShuffleBySequence) {
    LogicalPlan::Node* start_0_1 = m_plan->Load("file1")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* node_0_1 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("0");
    LogicalPlan::Node* node_0_2 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("1");
    LogicalPlan::Node* node_0_3 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("2");

    LogicalPlan::ShuffleGroup* shuffle =
            m_plan->Shuffle(m_plan->global_scope(), node_0_1, node_0_2, node_0_3)
                        ->WithConcurrency(10)
                            ->node(0)->DistributeAll()
                            ->node(1)->DistributeByDefault()
                            ->node(2)->DistributeBy<TestPartitioner>()
            ->done();

    FinishPlan();

    EXPECT_EQ(7u, m_nodes.size());
    EXPECT_EQ(3u, m_scopes.size());

    // check shuffle
    {
        EXPECT_EQ(PbScope::BUCKET, ProtoOf(shuffle->scope()).type());
        EXPECT_EQ(false, ProtoOf(shuffle->scope()).is_infinite());
        EXPECT_EQ(false, ProtoOf(shuffle->scope()).is_sorted());
        EXPECT_EQ(true, ProtoOf(shuffle->scope()).has_concurrency());
        EXPECT_EQ(10u, ProtoOf(shuffle->scope()).concurrency());
        EXPECT_EQ(10u, ProtoOf(shuffle->scope()).bucket_scope().bucket_size());
        ExpectFrom(shuffle->scope(), m_plan->global_scope());

        // node 0
        {
            LogicalPlan::Node* node = shuffle->node(0);

            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node).type());
            EXPECT_EQ(PbShuffleNode::BROADCAST, ProtoOf(node).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>("0"),
                      Entity<Objector>::From(ProtoOf(node).objector()));

            ExpectBelongsTo(node, shuffle->scope());
            ExpectFrom(node, node_0_1);
        }

        // node 1
        {
            LogicalPlan::Node* node = shuffle->node(1);

            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node).type());
            EXPECT_EQ(PbShuffleNode::SEQUENCE, ProtoOf(node).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>("1"),
                      Entity<Objector>::From(ProtoOf(node).objector()));
            EXPECT_EQ(false, ProtoOf(node).shuffle_node().has_partitioner());

            ExpectBelongsTo(node, shuffle->scope());
            ExpectFrom(node, node_0_2);
        }

        // node 2
        {
            LogicalPlan::Node* node = shuffle->node(2);

            EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(node).type());
            EXPECT_EQ(PbShuffleNode::SEQUENCE, ProtoOf(node).shuffle_node().type());
            EXPECT_EQ(false, ProtoOf(node).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>("2"),
                      Entity<Objector>::From(ProtoOf(node).objector()));
            EXPECT_EQ(true, ProtoOf(node).shuffle_node().has_partitioner());
            EXPECT_EQ(Entity<Partitioner>::Of<TestPartitioner>(""),
                      Entity<Partitioner>::From(ProtoOf(node).shuffle_node().partitioner()));

            ExpectBelongsTo(node, shuffle->scope());
            ExpectFrom(node, node_0_3);
        }
    }
}

TEST_F(LogicalPlanTest, ShuffleBySequenceWithInfinite) {
    std::vector<std::string> uri_list;
    uri_list.push_back(std::string("file1"));
    LogicalPlan::Node* start_0_1 =
            m_plan->RepeatedlyLoad(uri_list)->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* node_0_1 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("0");
    LogicalPlan::Node* node_0_2 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("1");

    LogicalPlan::ShuffleGroup* shuffle =
            m_plan->Shuffle(m_plan->global_scope(), node_0_1, node_0_2)
                        ->WithConcurrency(10)
                            ->node(0)->DistributeByDefault()
                            ->node(1)->DistributeBy<TestPartitioner>()
            ->done();

    LogicalPlan::ShuffleGroup* shuffle2 =
            m_plan->Shuffle(m_plan->global_scope(), node_0_1)
                        ->WithConcurrency(10)
                            ->node(0)->DistributeAsBatch()
            ->done();

    FinishPlan();

    // check shuffle
    {
        EXPECT_EQ(true, ProtoOf(shuffle->scope()).is_infinite());
        EXPECT_EQ(true, ProtoOf(shuffle->scope()).is_stream());

        // node 0
        {
            LogicalPlan::Node* node = shuffle->node(0);

            EXPECT_EQ(true, ProtoOf(node).is_infinite());
        }

        // node 1
        {
            LogicalPlan::Node* node = shuffle->node(1);

            EXPECT_EQ(true, ProtoOf(node).is_infinite());
        }
    }

    // check shuffle2
    {
        EXPECT_EQ(true, ProtoOf(shuffle2->scope()).is_infinite());
        EXPECT_EQ(false, ProtoOf(shuffle2->scope()).is_stream());

        // node 0
        {
            LogicalPlan::Node* node = shuffle2->node(0);

            EXPECT_EQ(false, ProtoOf(node).is_infinite());
        }
    }
}

TEST_F(LogicalPlanTest, Union) {
    LogicalPlan::Node* source_1 = m_plan->Load("file1")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* source_2 = m_plan->Load("file2")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* start = m_plan->Union(source_1, source_2);
    LogicalPlan::Node* node_1 = start->GroupBy<TestKeyReader>();
    LogicalPlan::Node* node_2 = node_1->GroupBy<TestKeyReader>();
    LogicalPlan::Node* node_3 = node_2->LeaveScope();
    LogicalPlan::Node* node_4 = node_2->RemoveScope();

    FinishPlan();

    // check start
    {
        LogicalPlan::Node* node = start;

        EXPECT_EQ(PbLogicalPlanNode::UNION_NODE, ProtoOf(node).type());
        EXPECT_EQ(false, ProtoOf(node).is_infinite());
        EXPECT_EQ(Entity<Objector>::Of<TestObjector>(""),
                  Entity<Objector>::From(ProtoOf(node).objector()));
        ExpectBelongsTo(node, m_plan->global_scope());
        ExpectFrom(node, source_1);
        ExpectFrom(node, source_2);
    }

    // check node 3/4
    {
        LogicalPlan::Node* checklist[] = {node_3, node_4};
        const LogicalPlan::Scope* scopes[] = {node_1->scope(), m_plan->global_scope()};

        for (int i = 0; i < 2; ++i) {
            LogicalPlan::Node* node = checklist[i];

            EXPECT_EQ(PbLogicalPlanNode::UNION_NODE, ProtoOf(node).type());
            EXPECT_EQ(false, ProtoOf(node).is_infinite());
            EXPECT_EQ(Entity<Objector>::Of<TestObjector>(""),
                      Entity<Objector>::From(ProtoOf(node).objector()));
            ExpectBelongsTo(node, scopes[i]);
            ExpectFrom(node, node_2);
        }
    }
}

TEST_F(LogicalPlanTest, UnionWithInfinite) {
    std::vector<std::string> uri_list;
    uri_list.push_back(std::string("file1"));
    LogicalPlan::Node* source_1 =
            m_plan->RepeatedlyLoad(uri_list)->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* source_2 = m_plan->Load("file2")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* start = m_plan->Union(source_1, source_2);
    LogicalPlan::Node* node_1 = start->GroupBy<TestKeyReader>();
    LogicalPlan::Node* node_2 = node_1->GroupBy<TestKeyReader>();
    LogicalPlan::Node* node_3 = node_2->LeaveScope();
    LogicalPlan::Node* node_4 = node_2->RemoveScope();
    LogicalPlan::Node* node_5 = start->DistributeInto(10);
    LogicalPlan::Node* node_6 = node_5->GroupBy<TestKeyReader>();
    LogicalPlan::Node* node_7 = node_6->LeaveScope();
    LogicalPlan::Node* node_8 = node_6->RemoveScope();

    FinishPlan();

    // check start
    {
        LogicalPlan::Node* node = start;

        EXPECT_EQ(true, ProtoOf(node).is_infinite());
    }

    // check node 3/4
    {
        LogicalPlan::Node* checklist[] = {node_3, node_4};

        for (int i = 0; i < 2; ++i) {
            LogicalPlan::Node* node = checklist[i];

            EXPECT_EQ(true, ProtoOf(node).is_infinite());
        }
    }

    // check node 7
    {
        LogicalPlan::Node* node = node_7;

        EXPECT_EQ(true, ProtoOf(node).is_infinite());
    }

    // check node 8
    {
        LogicalPlan::Node* node = node_8;

        EXPECT_EQ(true, ProtoOf(node).is_infinite());
    }
}

TEST_F(LogicalPlanTest, BroadcastTo) {
    LogicalPlan::Node* start_0_1 = m_plan->Load("file1")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* node_0_1 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("1");
    LogicalPlan::Node* node_0_2 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("2");
    LogicalPlan::Node* node_0_3 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("3");

    LogicalPlan::ShuffleGroup* shuffle_1 =
            m_plan->Shuffle(start_0_1->scope(), node_0_1, node_0_2, node_0_3);
    LogicalPlan::ShuffleNode* start_1_1 = shuffle_1->node(0)->MatchBy<TestKeyReader>("1");
    LogicalPlan::ShuffleNode* start_1_2 = shuffle_1->node(1)->MatchBy<TestKeyReader>("2");
    LogicalPlan::ShuffleNode* start_1_3 = shuffle_1->node(2)->MatchBy<TestKeyReader>("3");

    LogicalPlan::Node* node_1_1 = start_1_1->SortBy<TestKeyReader>();  // implicit shuffle
    LogicalPlan::Node* node_1_2 = start_1_2;
    LogicalPlan::Node* start_2_1 = start_1_3->GroupBy<TestKeyReader>();          // shuffle 2
    LogicalPlan::Node* start_3_1 = start_1_3->SortAndGroupBy<TestKeyReader>();   // shuffle 3

    LogicalPlan::Node* node_1_4 = node_1_2->ProcessBy<TestProcessor>()->As<TestObjector>("4");

    LogicalPlan::ShuffleGroup* shuffle_4 =
            m_plan->Shuffle(shuffle_1->scope(), node_1_1, node_1_2)
                ->node(0)->MatchBy<TestKeyReader>()
                ->node(1)->MatchAny()
            ->done();

    LogicalPlan::UnionNode* union_node_4 = m_plan->BroadcastTo(node_0_3, shuffle_4->scope());
    LogicalPlan::UnionNode* union_node_1 = m_plan->BroadcastTo(node_1_4, shuffle_1->scope());
    FinishPlan();

    EXPECT_EQ(18u, m_nodes.size()) << "The orginal plan has 14 nodes, "
                                   << "+3 additional ones generated by down-forward broadcast, "
                                   << "+1 additional one generated by same-level broadcast, "
                                   << "so 18 nodes are expected";
    EXPECT_EQ(7u, m_scopes.size());

    // Down forward broadcast
    {
        EXPECT_EQ(PbLogicalPlanNode::UNION_NODE, ProtoOf(union_node_4).type());
        ExpectBelongsTo(union_node_4, shuffle_4->scope());

        EXPECT_EQ(1u, union_node_4->from_size());
        const LogicalPlan::Node* broadcast_1 = union_node_4->from(0);
        EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(broadcast_1).type());
        EXPECT_EQ(PbShuffleNode::BROADCAST, ProtoOf(broadcast_1).shuffle_node().type());
        ExpectBelongsTo(broadcast_1, shuffle_4->scope());

        const LogicalPlan::Node* broadcast_2 =
        static_cast<const LogicalPlan::ShuffleNode*>(broadcast_1)->from();
        EXPECT_EQ(PbLogicalPlanNode::SHUFFLE_NODE, ProtoOf(broadcast_2).type());
        EXPECT_EQ(PbShuffleNode::BROADCAST, ProtoOf(broadcast_2).shuffle_node().type());
        ExpectBelongsTo(broadcast_2, shuffle_1->scope());

        ExpectFrom(broadcast_2, node_0_3);
    }

    // Same level broadcast
    {
        EXPECT_EQ(PbLogicalPlanNode::UNION_NODE, ProtoOf(union_node_1).type());
        ExpectBelongsTo(union_node_1, shuffle_1->scope());

        ExpectFrom(union_node_1, node_1_4);
    }
}

typedef LogicalPlanTest LogicalPlanDeathTest;

TEST_F(LogicalPlanDeathTest, UpForwardBroadcast) {
    LogicalPlan::Node* start_0_1 = m_plan->Load("file1")->By<TestLoader>()->As<TestObjector>();
    LogicalPlan::Node* node_0_1 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("1");
    LogicalPlan::Node* node_0_2 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("2");
    LogicalPlan::Node* node_0_3 = start_0_1->ProcessBy<TestProcessor>()->As<TestObjector>("3");

    LogicalPlan::ShuffleGroup* shuffle_1 =
            m_plan->Shuffle(start_0_1->scope(), node_0_1, node_0_2, node_0_3);
    LogicalPlan::ShuffleNode* start_1_1 = shuffle_1->node(0)->MatchBy<TestKeyReader>("1");
    LogicalPlan::ShuffleNode* start_1_2 = shuffle_1->node(1)->MatchBy<TestKeyReader>("2");

    LogicalPlan::Node* node_1_1 = start_1_1->SortBy<TestKeyReader>();  // implicit shuffle
    LogicalPlan::Node* node_1_2 = start_1_2;
    LogicalPlan::Node* node_1_4 = node_1_2->ProcessBy<TestProcessor>()->As<TestObjector>("4");

    LogicalPlan::ShuffleGroup* shuffle_4 =
            m_plan->Shuffle(shuffle_1->scope(), node_1_1, node_1_2)
                ->node(0)->MatchBy<TestKeyReader>()
                ->node(1)->MatchAny()
            ->done();
    {
        EXPECT_DEATH(m_plan->BroadcastTo(node_1_4, start_0_1->scope()),
                     "Up-forward broadcast is forbidden.")
            << "Broadcasting to ancestor scopes should be forbidden.";
        EXPECT_DEATH(m_plan->BroadcastTo(node_1_4, start_0_1->scope()->father()),
                     "Up-forward broadcast is forbidden.")
            << "Broadcasting to ancestor scopes should be forbidden.";
        EXPECT_DEATH(m_plan->BroadcastTo(shuffle_4->node(0), start_0_1->scope()),
                     "Up-forward broadcast is forbidden.")
            << "Broadcasting to ancestor scopes should be forbidden.";
    }
}

ACTION_P(Emit, object) {
    arg1->Emit(object);
}

TEST_F(LogicalPlanTest, RunLocally) {
    std::vector<std::string> splits(1, "split");
    int object = 0;

    MockLoader& loader = MockLoader::Mock("loader");
    {
        EXPECT_CALL(loader, Split("file", _))
            .WillOnce(SetArgPointee<1>(splits));
        EXPECT_CALL(loader, Load("split", _))
            .WillOnce(Emit(&object));
    }

    MockSinker& sinker = MockSinker::Mock("sinker");
    {
        EXPECT_CALL(sinker, Open(ElementsAre("")));
        EXPECT_CALL(sinker, Sink(&object));
        EXPECT_CALL(sinker, Close());
    }

    LogicalPlan::Node* load = m_plan->Load("file")->By<MockLoader>("loader")->As<MockObjector>();
    m_plan->Sink(m_plan->global_scope(), load)->By<MockSinker>("sinker");
    m_plan->RunLocally();
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
