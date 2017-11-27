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

#include "flume/planner/local/local_planner.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "boost/shared_ptr.hpp"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"
#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/logical_plan.h"
#include "flume/core/objector.h"
#include "flume/core/processor.h"
#include "flume/core/sinker.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/planner/graph_helper.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/session.h"

namespace baidu {
namespace flume {
namespace planner {
namespace local {

using ::testing::AllOf;
using ::testing::Contains;
using ::testing::ElementsAre;

using core::Entity;
using core::Loader;
using core::LogicalPlan;

class ExecutorVisitor {
public:
    explicit ExecutorVisitor(const PbExecutor& message) : m_message(&message) {
        std::vector<Vertex> vertices;
        EXPECT_TRUE(BuildExecutorDag(message, &m_dag, &vertices));
        EXPECT_EQ(message.child_size() + 2, static_cast<int>(vertices.size()));

        m_in = vertices.front();
        m_out = vertices.back();
        for (int i = 1; i <= message.child_size(); ++i) {
            Vertex v = vertices[i];
            std::string identity = m_dag[v].visitor->identity();
            EXPECT_EQ(0u, m_childs.count(identity))
                    << "two sub-executor has same identity: " << identity;
            m_childs[identity] = v;
        }
    }

    std::string identity() const {
        switch (m_message->type()) {
            case PbExecutor::EXTERNAL:
                return m_message->external_executor().id();
            case PbExecutor::LOGICAL:
                return m_message->logical_executor().node().id();
            case PbExecutor::SHUFFLE:
                return m_message->shuffle_executor().scope().id();
            case PbExecutor::PROCESSOR:
                return m_message->processor_executor().identity();
            default:
                return "";
        }
    }

    const PbExecutor* operator->() const { return m_message; }

    int size() const { return m_childs.size(); }
    ExecutorVisitor& operator[](const std::string& id) {
        LOG_IF(FATAL, m_childs.find(id) == m_childs.end())
            << "expected executor not found: " << id;
        return *m_dag[m_childs.find(id)->second].visitor;
    }

    int GetDependencyCounts() const {
        return boost::num_edges(m_dag);
    }

    std::set<std::string> SourcesBetween(const ExecutorVisitor& from, const ExecutorVisitor& to) {
        CheckIsChild(from);
        CheckIsChild(to);
        return GetSources(m_childs[from.identity()], m_childs[to.identity()]);
    }

    std::set<std::string> SourcesFromParentTo(const ExecutorVisitor& to) {
        CheckIsChild(to);
        return GetSources(m_in, m_childs[to.identity()]);
    }

    std::set<std::string> OutputsFrom(const ExecutorVisitor& from) {
        CheckIsChild(from);
        return GetSources(m_childs[from.identity()], m_out);
    }

    std::set<std::string> DirectOutputs() {
        return GetSources(m_in, m_out);
    }

    void CheckIsChild(const ExecutorVisitor& child) {
        EXPECT_EQ(1u, m_childs.count(child.identity()));
    }

private:
    struct VertexInfo {
        boost::shared_ptr<ExecutorVisitor> visitor;

        VertexInfo& operator=(const PbExecutor& message) {
            visitor.reset(new ExecutorVisitor(message));
            return *this;
        }
    };

    struct EdgeInfo {
        std::vector<std::string> sources;

        EdgeInfo& operator=(const std::string& source) {
            sources.push_back(source);
            return *this;
        }
    };

    typedef boost::adjacency_list<
        boost::setS, boost::vecS, boost::directedS, VertexInfo, EdgeInfo
    > ExecutorDag;
    typedef boost::graph_traits<ExecutorDag>::vertex_descriptor Vertex;
    typedef boost::graph_traits<ExecutorDag>::edge_descriptor Edge;

private:
    std::set<std::string> GetSources(Vertex from, Vertex to) {
        bool has_edge = false;
        Edge edge;
        boost::tie(edge, has_edge) = boost::edge(from, to, m_dag);
        if (has_edge) {
            return std::set<std::string>(m_dag[edge].sources.begin(), m_dag[edge].sources.end());
        } else {
            return std::set<std::string>();
        }
    }

private:
    const PbExecutor* m_message;
    ExecutorDag m_dag;

    Vertex m_in;
    Vertex m_out;
    std::map<std::string, Vertex> m_childs;
};

class LocalPlannerTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_logical_plan.reset(new LogicalPlan());
    }

    ExecutorVisitor& FinishPlan() {
        LocalPlanner planner;

        m_physical_plan = planner.Plan(m_logical_plan->ToProtoMessage());
        m_physical_plan.CheckInitialized();
        EXPECT_EQ(1, m_physical_plan.job_size());

        const PbJob& job = m_physical_plan.job(0);
        EXPECT_EQ(PbJob::LOCAL, job.type());
        EXPECT_TRUE(job.has_local_job());

        for (int i = 0; i < job.local_job().input_size(); ++i) {
            const PbLocalInput& input = job.local_job().input(i);
            EXPECT_EQ(0u, m_inputs.count(input.id()));
            m_inputs[input.id()] = input;
        }

        const PbLocalTask& task = job.local_job().task();
        m_visitor.reset(new ExecutorVisitor(task.root()));
        return *m_visitor;
    }

    size_t GetLocalInputCounts() {
        return m_inputs.size();
    }

    Entity<Loader> GetLocalInputSpliter(const std::string& id) {
        CHECK_EQ(1u, m_inputs.count(id));
        return Entity<Loader>::From(m_inputs[id].spliter());
    }

    std::vector<std::string> GetLocalInputUri(const std::string& id) {
        CHECK_EQ(1u, m_inputs.count(id));
        return std::vector<std::string>(m_inputs[id].uri().begin(), m_inputs[id].uri().end());
    }

protected:
    toft::scoped_ptr<LogicalPlan> m_logical_plan;
    PbPhysicalPlan m_physical_plan;

    toft::scoped_ptr<ExecutorVisitor> m_visitor;
    std::map<std::string, PbLocalInput> m_inputs;
};

std::set<std::string> AsSources(const LogicalPlan::Node* s1,
                                const LogicalPlan::Node* s2 = NULL,
                                const LogicalPlan::Node* s3 = NULL,
                                const LogicalPlan::Node* s4 = NULL) {
    std::set<std::string> sources;
    const LogicalPlan::Node* params[] = {s1, s2, s3, s4};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            sources.insert(params[i]->identity());
        }
    }
    return sources;
}

TEST_F(LocalPlannerTest, LoadSink) {
    LogicalPlan::Node* node_1 =
            m_logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_2 =
            m_logical_plan->Sink(m_logical_plan->global_scope(), node_1)->By<MockSinker>();

    ExecutorVisitor& root = FinishPlan();
    ASSERT_EQ(2, root.size());

    ExecutorVisitor& input = root[node_1->scope()->identity()];
    ASSERT_EQ(1, input.size());
    ExecutorVisitor& load = input[node_1->identity()];

    ExecutorVisitor& sink = root[node_2->identity()];

    EXPECT_EQ(PbExecutor::TASK, root->type());
    EXPECT_EQ(0u, root->scope_level());
    EXPECT_EQ(0, root->input_size());
    EXPECT_EQ(0, root->output_size());
    EXPECT_EQ(1, root.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), root.SourcesBetween(input, sink));

    EXPECT_EQ(PbExecutor::EXTERNAL, input->type());
    EXPECT_EQ(1u, input->scope_level());
    EXPECT_EQ(0, input->input_size());
    EXPECT_THAT(input->output(), ElementsAre(node_1->identity()));
    EXPECT_EQ(1, input.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), input.OutputsFrom(load));

    EXPECT_EQ(0, load.size());
    EXPECT_EQ(PbExecutor::LOGICAL, load->type());
    EXPECT_EQ(1u, load->scope_level());
    EXPECT_EQ(0, load->input_size());
    EXPECT_THAT(load->output(), ElementsAre(node_1->identity()));
    EXPECT_EQ(1, load.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), load.DirectOutputs());

    EXPECT_EQ(0, sink.size());
    EXPECT_EQ(PbExecutor::LOGICAL, sink->type());
    EXPECT_EQ(0u, sink->scope_level());
    EXPECT_THAT(sink->input(), ElementsAre(node_1->identity()));
    EXPECT_EQ(0, sink->output_size());
    EXPECT_EQ(0, sink.GetDependencyCounts());

    EXPECT_EQ(1u, GetLocalInputCounts());
    EXPECT_EQ(GetLocalInputSpliter(input.identity()), Entity<Loader>::Of<MockLoader>(""));
    EXPECT_THAT(GetLocalInputUri(input.identity()), ElementsAre("test.txt"));
}

TEST_F(LocalPlannerTest, ShuffleUnion) {
    LogicalPlan::Node* node_1 =
            m_logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_2 = node_1->GroupBy<MockKeyReader>("1");
    LogicalPlan::Node* node_3 =
            m_logical_plan->Union(m_logical_plan->global_scope(), node_1, node_2);
    LogicalPlan::Node* node_4 =
            m_logical_plan->Sink(m_logical_plan->global_scope(), node_3)->By<MockSinker>();

    ExecutorVisitor& root = FinishPlan();
    ASSERT_EQ(3, root.size());

    ExecutorVisitor& input = root[node_1->scope()->identity()];
    ASSERT_EQ(2, input.size());
    ExecutorVisitor& load = input[node_1->identity()];
    ExecutorVisitor& shuffle = input[node_2->scope()->identity()];

    ExecutorVisitor& union_ = root[node_3->identity()];

    EXPECT_EQ(PbExecutor::TASK, root->type());
    EXPECT_EQ(0u, root->scope_level());
    EXPECT_EQ(0, root->input_size());
    EXPECT_EQ(0, root->output_size());
    EXPECT_EQ(2, root.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1, node_2), root.SourcesBetween(input, union_));

    EXPECT_EQ(PbExecutor::EXTERNAL, input->type());
    EXPECT_EQ(1u, input->scope_level());
    EXPECT_EQ(0, input->input_size());
    EXPECT_EQ(2, input->output_size());
    EXPECT_THAT(input->output(), Contains(node_1->identity()));
    EXPECT_THAT(input->output(), Contains(node_2->identity()));
    EXPECT_EQ(3, input.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), input.SourcesBetween(load, shuffle));
    EXPECT_EQ(AsSources(node_1), input.OutputsFrom(load));
    EXPECT_EQ(AsSources(node_2), input.OutputsFrom(shuffle));

    EXPECT_EQ(0, load.size());
    EXPECT_EQ(PbExecutor::LOGICAL, load->type());
    EXPECT_EQ(1u, load->scope_level());
    EXPECT_EQ(0, load->input_size());
    EXPECT_EQ(1, load->output_size());
    EXPECT_THAT(load->output(), Contains(node_1->identity()));
    EXPECT_EQ(1, load.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), load.DirectOutputs());

    EXPECT_EQ(0, shuffle.size());
    EXPECT_EQ(PbExecutor::SHUFFLE, shuffle->type());
    EXPECT_EQ(2u, shuffle->scope_level());
    EXPECT_THAT(shuffle->input(), ElementsAre(node_1->identity()));
    EXPECT_THAT(shuffle->output(), ElementsAre(node_2->identity()));
    EXPECT_EQ(1, shuffle.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_2), shuffle.DirectOutputs());

    EXPECT_EQ(0, union_.size());
    EXPECT_EQ(PbExecutor::LOGICAL, union_->type());
    EXPECT_EQ(0u, union_->scope_level());
    EXPECT_EQ(2, union_->input_size());
    EXPECT_THAT(union_->input(), Contains(node_1->identity()));
    EXPECT_THAT(union_->input(), Contains(node_2->identity()));
    EXPECT_THAT(union_->output(), ElementsAre(node_3->identity()));
    EXPECT_EQ(1, union_.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_3), union_.DirectOutputs());

    ExecutorVisitor &sink = root[node_4->identity()];
    EXPECT_EQ(0, sink.size());
    EXPECT_EQ(PbExecutor::LOGICAL, sink->type());
    EXPECT_EQ(0u, sink->scope_level());
    EXPECT_THAT(sink->input(), ElementsAre(node_3->identity()));
    EXPECT_EQ(0, sink->output_size());
    EXPECT_EQ(0, sink.GetDependencyCounts());

    EXPECT_EQ(1u, GetLocalInputCounts());
    EXPECT_EQ(GetLocalInputSpliter(input.identity()), Entity<Loader>::Of<MockLoader>(""));
    EXPECT_THAT(GetLocalInputUri(input.identity()), ElementsAre("test.txt"));
}

TEST_F(LocalPlannerTest, Process) {
    LogicalPlan::Node* node_1 =
            m_logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_1_1 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();
    LogicalPlan::Node* node_1_2 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();

    LogicalPlan::ShuffleGroup* shuffle_group =
            m_logical_plan->Shuffle(m_logical_plan->global_scope(), node_1_1, node_1_2)
                    ->node(0)->MatchBy<MockKeyReader>()
                    ->node(1)->MatchBy<MockKeyReader>()
            ->done();
    LogicalPlan::Node* node_2_1 = shuffle_group->node(0);
    LogicalPlan::Node* node_2_2 = shuffle_group->node(1);
    LogicalPlan::Node* node_2_3 =
            node_2_1->CombineWith(node_2_2)->By<MockProcessor>()->As<MockObjector>();

    LogicalPlan::Node* node_3 = m_logical_plan
            ->Process(m_logical_plan->global_scope(), node_1_1, node_2_3)
            ->By<MockProcessor>()->As<MockObjector>();
    LogicalPlan::Node* node_4 =
            m_logical_plan->Sink(m_logical_plan->global_scope(), node_3)->By<MockSinker>();

    ExecutorVisitor& root = FinishPlan();
    ASSERT_EQ(4, root.size());

    ExecutorVisitor& input = root[node_1->scope()->identity()];
    ASSERT_EQ(3, input.size());
    ExecutorVisitor& load = input[node_1->identity()];
    ExecutorVisitor& process_1_1 = input[node_1_1->identity()];
    ExecutorVisitor& process_1_2 = input[node_1_2->identity()];

    ExecutorVisitor& shuffle = root[shuffle_group->identity()];
    ASSERT_EQ(1, shuffle.size());
    ExecutorVisitor& process_2 = shuffle[node_2_3->identity()];

    ExecutorVisitor& process_3 = root[node_3->identity()];

    // root
    EXPECT_EQ(PbExecutor::TASK, root->type());
    EXPECT_EQ(0u, root->scope_level());
    EXPECT_EQ(0, root->input_size());
    EXPECT_EQ(0, root->output_size());
    EXPECT_EQ(4, root.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1_1, node_1_2), root.SourcesBetween(input, shuffle));
    EXPECT_EQ(AsSources(node_1_1), root.SourcesBetween(input, process_3));
    EXPECT_EQ(AsSources(node_2_3), root.SourcesBetween(shuffle, process_3));

    // input
    EXPECT_EQ(PbExecutor::EXTERNAL, input->type());
    EXPECT_EQ(1u, input->scope_level());
    EXPECT_EQ(0, input->input_size());
    EXPECT_EQ(2, input->output_size());
    EXPECT_THAT(input->output(), Contains(node_1_1->identity()));
    EXPECT_THAT(input->output(), Contains(node_1_2->identity()));
    EXPECT_EQ(4, input.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), input.SourcesBetween(load, process_1_1));
    EXPECT_EQ(AsSources(node_1), input.SourcesBetween(load, process_1_2));
    EXPECT_EQ(AsSources(node_1_1), input.OutputsFrom(process_1_1));
    EXPECT_EQ(AsSources(node_1_2), input.OutputsFrom(process_1_2));

    // load
    EXPECT_EQ(PbExecutor::LOGICAL, load->type());
    EXPECT_EQ(1u, load->scope_level());
    EXPECT_EQ(0, load->input_size());
    EXPECT_EQ(1, load->output_size());
    EXPECT_THAT(load->output(), Contains(node_1->identity()));
    EXPECT_EQ(1, load.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1), load.DirectOutputs());

    // process_1_1
    EXPECT_EQ(0, process_1_1.size());
    EXPECT_EQ(PbExecutor::PROCESSOR, process_1_1->type());
    EXPECT_EQ(1u, process_1_1->scope_level());
    EXPECT_THAT(process_1_1->input(), ElementsAre(node_1->identity()));
    EXPECT_THAT(process_1_1->output(), ElementsAre(node_1_1->identity()));
    EXPECT_EQ(1, process_1_1.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1_1), process_1_1.DirectOutputs());

    // process_1_2
    EXPECT_EQ(0, process_1_2.size());
    EXPECT_EQ(PbExecutor::PROCESSOR, process_1_2->type());
    EXPECT_EQ(1u, process_1_2->scope_level());
    EXPECT_THAT(process_1_2->input(), ElementsAre(node_1->identity()));
    EXPECT_THAT(process_1_2->output(), ElementsAre(node_1_2->identity()));
    EXPECT_EQ(1, process_1_2.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_1_2), process_1_2.DirectOutputs());

    // shuffle
    EXPECT_EQ(PbExecutor::SHUFFLE, shuffle->type());
    EXPECT_EQ(1u, shuffle->scope_level());
    EXPECT_EQ(2, shuffle->input_size());
    EXPECT_THAT(shuffle->input(), Contains(node_1_1->identity()));
    EXPECT_THAT(shuffle->input(), Contains(node_1_2->identity()));
    EXPECT_THAT(shuffle->output(), ElementsAre(node_2_3->identity()));
    EXPECT_EQ(2, shuffle.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_2_1, node_2_2), shuffle.SourcesFromParentTo(process_2));
    EXPECT_EQ(AsSources(node_2_3), shuffle.OutputsFrom(process_2));

    // process_2
    EXPECT_EQ(0, process_2.size());
    EXPECT_EQ(PbExecutor::PROCESSOR, process_2->type());
    EXPECT_EQ(1u, process_2->scope_level());
    EXPECT_EQ(2, process_2->input_size());
    EXPECT_THAT(process_2->input(), Contains(node_2_1->identity()));
    EXPECT_THAT(process_2->input(), Contains(node_2_2->identity()));
    EXPECT_THAT(process_2->output(), ElementsAre(node_2_3->identity()));
    EXPECT_EQ(1, process_2.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_2_3), process_2.DirectOutputs());

    // process_3
    EXPECT_EQ(0, process_3.size());
    EXPECT_EQ(PbExecutor::PROCESSOR, process_3->type());
    EXPECT_EQ(0u, process_3->scope_level());
    EXPECT_EQ(2, process_3->input_size());
    EXPECT_THAT(process_3->input(), Contains(node_1_1->identity()));
    EXPECT_THAT(process_3->input(), Contains(node_2_3->identity()));
    EXPECT_THAT(process_3->output(), ElementsAre(node_3->identity()));
    EXPECT_EQ(1, process_3.GetDependencyCounts());
    EXPECT_EQ(AsSources(node_3), process_3.DirectOutputs());

    // Sink(node_4)
    ExecutorVisitor &sink = root[node_4->identity()];
    EXPECT_EQ(0, sink.size());
    EXPECT_EQ(PbExecutor::LOGICAL, sink->type());
    EXPECT_EQ(0u, sink->scope_level());
    EXPECT_THAT(sink->input(), ElementsAre(node_3->identity()));
    EXPECT_EQ(0, sink->output_size());
    EXPECT_EQ(0, sink.GetDependencyCounts());

    EXPECT_EQ(1u, GetLocalInputCounts());
    EXPECT_EQ(GetLocalInputSpliter(input.identity()), Entity<Loader>::Of<MockLoader>(""));
    EXPECT_THAT(GetLocalInputUri(input.identity()), ElementsAre("test.txt"));
}

}  // namespace local
}  // namespace planner
}  // namespace flume
}  // namespace baidu
