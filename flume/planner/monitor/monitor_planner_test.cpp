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
// Author: Pan Yuchang(BDG)<bigflow-opensource@baidu.com>
// Description:

#include "flume/planner/monitor/monitor_planner.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "boost/foreach.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/tuple/tuple.hpp"
#include "boost/tuple/tuple_comparison.hpp"
#include "boost/tuple/tuple_io.hpp"
#include "google/protobuf/message.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/algorithm.h"

#include "flume/core/entity.h"
#include "flume/core/loader.h"
#include "flume/core/logical_plan.h"
#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/planner/graph_helper.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

using ::testing::AllOf;
using ::testing::Contains;

using core::Entity;
using core::Loader;
using core::LogicalPlan;
using core::Partitioner;

class MonitorPlannerTest : public PlanTest {
public:
    virtual void SetUp() {
        m_logical_plan.reset(new LogicalPlan());
    }

    void FinishPlan() {
        runtime::Resource resource;
        MonitorPlanner planner;
        planner.SetDebugDirectory(resource.GetRootEntry());

        m_physical_plan = planner.Plan(m_logical_plan->ToProtoMessage());
        std::string debug_string = m_physical_plan.DebugString();

        std::string test_name =
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
        std::string cmd = "cp -r " + resource.ViewAsDirectory() + " ./" + test_name;
        system(cmd.c_str());
    }

    bool CheckExecutorType(const PbExecutor& executor) {
        switch (executor.type()) {
            case PbExecutor::TASK:
                return true;
            case PbExecutor::EXTERNAL:
                return executor.has_external_executor();
            case PbExecutor::PROCESSOR:
                return executor.has_processor_executor();
            case PbExecutor::LOGICAL:
                return executor.has_logical_executor();
            case PbExecutor::SHUFFLE:
                return executor.has_shuffle_executor();
            case PbExecutor::WRITE_CACHE:
                return true;
            case PbExecutor::PARTIAL:
                return executor.has_partial_executor();
            default:
                return false;
        }
    }

    template<PbExecutor::Type T, bool flag>
    int FindExecutorNum(const PbExecutor& executor) {
        int total = 0;
        if (executor.type() == T || flag) {
            if (CheckExecutorType(executor)) {
                ++total;
            } else {
                LOG(INFO) << "type:" << T << "  do not contain accurate executor";
            }
        }
        BOOST_FOREACH(PbExecutor executor, executor.child()) {
            total += FindExecutorNum<T, flag>(executor);
        }
        return total;
    }

protected:
    toft::scoped_ptr<LogicalPlan> m_logical_plan;
    PbPhysicalPlan m_physical_plan;
};

TEST_F(MonitorPlannerTest, DISABLED_NoPreparedTest) {
    LogicalPlan::Node* node_1 =  m_logical_plan
        ->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_2 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();

    m_logical_plan->Sink(m_logical_plan->global_scope(), node_2)->By<MockSinker>("1");
    FinishPlan();

    const PbMonitorJob& job = m_physical_plan.job(0).monitor_job();
    EXPECT_TRUE(job.has_worker_stream_task());
    EXPECT_TRUE(job.has_worker_prepared_task());
    EXPECT_TRUE(job.has_client_stream_task());
    EXPECT_TRUE(job.has_client_prepared_task());

    const PbMonitorTask& worker_task = job.worker_stream_task();
    EXPECT_TRUE(worker_task.has_input());
    EXPECT_EQ(1, worker_task.monitor_writer_size());
    EXPECT_FALSE(worker_task.has_monitor_reader());
    EXPECT_EQ(4, (FindExecutorNum<PbExecutor::TASK, true>(worker_task.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(worker_task.root())));
    EXPECT_EQ(1, (FindExecutorNum<PbExecutor::PROCESSOR, false>(worker_task.root())));
    EXPECT_EQ(2, (FindExecutorNum<PbExecutor::EXTERNAL, false>(worker_task.root())));

    const PbMonitorTask& client_task = job.client_stream_task();
    EXPECT_FALSE(client_task.has_input());
    EXPECT_EQ(1, client_task.monitor_writer_size());
    EXPECT_TRUE(client_task.has_monitor_reader());
    EXPECT_EQ(4, (FindExecutorNum<PbExecutor::TASK, true>(client_task.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::PROCESSOR, false>(client_task.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(client_task.root())));
    EXPECT_EQ(3, (FindExecutorNum<PbExecutor::EXTERNAL, false>(client_task.root())));

    const PbMonitorTask& worker_task_p = job.worker_prepared_task();
    EXPECT_FALSE(worker_task_p.has_input());
    EXPECT_EQ(0, worker_task_p.monitor_writer_size());
    EXPECT_FALSE(worker_task_p.has_monitor_reader());
    EXPECT_EQ(1, (FindExecutorNum<PbExecutor::TASK, true>(worker_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::PROCESSOR, false>(worker_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(worker_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::EXTERNAL, false>(worker_task_p.root())));

    const PbMonitorTask& client_task_p = job.client_prepared_task();
    EXPECT_FALSE(client_task_p.has_input());
    EXPECT_EQ(0, client_task_p.monitor_writer_size());
    EXPECT_FALSE(client_task_p.has_monitor_reader());
    EXPECT_EQ(1, (FindExecutorNum<PbExecutor::TASK, true>(client_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::PROCESSOR, false>(client_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(client_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::EXTERNAL, false>(client_task_p.root())));
}

// TODO(wenxiang): repair monitor planner and fix this test
TEST_F(MonitorPlannerTest, DISABLED_PreparedTest) {
    LogicalPlan::Node* node_1 =
            m_logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_1_1 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();
    LogicalPlan::ProcessNode* node_1_2 = node_1_1->ProcessBy<MockProcessor>()->As<MockObjector>();
    LogicalPlan::ProcessNode* node_1_3 = node_1_1->ProcessBy<MockProcessor>()->As<MockObjector>();
    node_1_2->input(0)->set_is_prepared(true);
    node_1_3->input(0)->set_is_prepared(true);

    LogicalPlan::ShuffleGroup* shuffle_group =
            m_logical_plan->Shuffle(m_logical_plan->global_scope(), node_1_2, node_1_3)
                    ->node(0)->MatchBy<MockKeyReader>()
                    ->node(1)->MatchBy<MockKeyReader>()
            ->done();

    LogicalPlan::Node* node_2_1 = shuffle_group->node(0);
    LogicalPlan::Node* node_2_2 = shuffle_group->node(1);
    LogicalPlan::ProcessNode* node_2_3 =
            node_2_1->CombineWith(node_2_2)->By<MockProcessor>()->As<MockObjector>();
    node_2_3->input(0)->AllowPartialProcessing();
    node_2_3->input(1)->AllowPartialProcessing();

    LogicalPlan::ShuffleGroup* sub_shuffle =
            m_logical_plan->Shuffle(shuffle_group->scope(), node_2_3)
                    ->node(0)->MatchBy<MockKeyReader>()
            ->done();
    LogicalPlan::Node* node_2_4 = sub_shuffle->node(0);
    LogicalPlan::ProcessNode* node_2_5 = node_2_4->ProcessBy<MockProcessor>()->As<MockObjector>();
    node_2_5->input(0)->set_is_prepared(true);

    LogicalPlan::Node* node_3 = m_logical_plan
            ->Process(m_logical_plan->global_scope(), node_1_1, node_2_5)
            ->By<MockProcessor>()->As<MockObjector>();

    LogicalPlan::Node* node_4 = m_logical_plan
        ->Sink(m_logical_plan->global_scope(), node_3)->By<MockSinker>("1");
    CHECK_NOTNULL(node_4);

    FinishPlan();

    const PbMonitorJob& job = m_physical_plan.job(0).monitor_job();

    EXPECT_TRUE(job.has_worker_stream_task());
    EXPECT_TRUE(job.has_worker_prepared_task());
    EXPECT_TRUE(job.has_client_stream_task());
    EXPECT_TRUE(job.has_client_prepared_task());

    const PbMonitorTask& worker_task = job.worker_stream_task();
    EXPECT_TRUE(worker_task.has_input());
    EXPECT_EQ(2, worker_task.monitor_writer_size());
    EXPECT_FALSE(worker_task.has_monitor_reader());
    EXPECT_EQ(5, (FindExecutorNum<PbExecutor::TASK, true>(worker_task.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(worker_task.root())));
    EXPECT_EQ(1, (FindExecutorNum<PbExecutor::PROCESSOR, false>(worker_task.root())));
    EXPECT_EQ(3, (FindExecutorNum<PbExecutor::EXTERNAL, false>(worker_task.root())));

    const PbMonitorTask& client_task = job.client_stream_task();
    EXPECT_FALSE(client_task.has_input());
    EXPECT_EQ(2, client_task.monitor_writer_size());
    EXPECT_TRUE(client_task.has_monitor_reader());
    EXPECT_EQ(9, (FindExecutorNum<PbExecutor::TASK, true>(client_task.root())));
    EXPECT_EQ(1, (FindExecutorNum<PbExecutor::PROCESSOR, false>(client_task.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(client_task.root())));
    EXPECT_EQ(5, (FindExecutorNum<PbExecutor::EXTERNAL, false>(client_task.root())));
    EXPECT_EQ(2, (FindExecutorNum<PbExecutor::SHUFFLE, false>(client_task.root())));

    const PbMonitorTask& worker_task_p = job.worker_prepared_task();
    EXPECT_FALSE(worker_task_p.has_input());
    EXPECT_EQ(2, worker_task_p.monitor_writer_size());
    EXPECT_TRUE(worker_task_p.has_monitor_reader());
    EXPECT_EQ(8, (FindExecutorNum<PbExecutor::TASK, true>(worker_task_p.root())));
    EXPECT_EQ(2, (FindExecutorNum<PbExecutor::PROCESSOR, false>(worker_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(worker_task_p.root())));
    EXPECT_EQ(4, (FindExecutorNum<PbExecutor::EXTERNAL, false>(worker_task_p.root())));
    EXPECT_EQ(1, (FindExecutorNum<PbExecutor::SHUFFLE, false>(worker_task_p.root())));

    const PbMonitorTask& client_task_p = job.client_prepared_task();
    EXPECT_FALSE(client_task_p.has_input());
    EXPECT_EQ(1, client_task_p.monitor_writer_size());
    EXPECT_TRUE(client_task_p.has_monitor_reader());
    EXPECT_EQ(9, (FindExecutorNum<PbExecutor::TASK, true>(client_task_p.root())));
    EXPECT_EQ(2, (FindExecutorNum<PbExecutor::PROCESSOR, false>(client_task_p.root())));
    EXPECT_EQ(0, (FindExecutorNum<PbExecutor::LOGICAL, false>(client_task_p.root())));
    EXPECT_EQ(4, (FindExecutorNum<PbExecutor::EXTERNAL, false>(client_task_p.root())));
    EXPECT_EQ(2, (FindExecutorNum<PbExecutor::SHUFFLE, false>(client_task_p.root())));
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu
