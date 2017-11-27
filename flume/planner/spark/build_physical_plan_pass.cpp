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
// Author: Pan Yuchang <bigflow-opensource@baidu.com>
//         Wang Cong <bigflow-opensource@baidu.com>
//

#include "flume/planner/spark/build_physical_plan_pass.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

namespace {

PbSparkJob& SparkJobOf(Plan* plan) { // NOLINT
    return plan->Root()->get<PbSparkJob>();
}

PbSparkJob* JobMessageOf(Plan* plan) {
    return plan->Root()->get<PbPhysicalPlan>().mutable_job(0)->mutable_spark_job();
}

class GenerateSparkJobRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan *plan, Unit *unit) {
        return unit == plan->Root();
    }

    virtual bool Run(Plan *plan, Unit *root) {
        CHECK(!root->has<PbPhysicalPlan>());

        PbPhysicalPlan &pb_physical_plan = root->get<PbPhysicalPlan>();
        PbJob *pb_job = pb_physical_plan.add_job();
        pb_job->set_id("Du0825");
        pb_job->set_type(PbJob::SPARK);

        if (!root->get<planner::JobConfig>().is_null()) {
            *pb_job->mutable_job_config() = *root->get<planner::JobConfig>();
        }

        return false;
    }
};

class InitializeTaskRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::TASK;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        PbSparkTask* task = &unit->get<PbSparkTask>();
        JobConfig job_config = plan->Root()->get<planner::JobConfig>();
        task->set_do_cpu_profile(job_config->cpu_profile());
        task->set_do_heap_profile(job_config->heap_profile());

        task->set_task_index(*unit->get<TaskIndex>());
        *task->mutable_root() = unit->get<PbExecutor>();
        task->clear_hadoop_input();
        task->clear_shuffle_input();
        task->clear_shuffle_output();
        return false;
    }
};

template<ExternalExecutor N>
class ExternalExecutorRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::EXTERNAL_EXECUTOR && unit->get<ExternalExecutor>() == N;
    }

protected:
    PbSparkTask* TaskMessageOf(Unit* unit) {
        return &unit->task()->get<PbSparkTask>();
    }

    template<typename T>
    T& CheckAndGet(Unit* unit) {
        CHECK(unit->has<T>());
        return unit->get<T>();
    }
};

class AddHadoopInputRule : public ExternalExecutorRule<HADOOP_INPUT> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        PbSparkTask *task = TaskMessageOf(unit);
        *task->mutable_hadoop_input() = CheckAndGet<PbSparkTask::PbHadoopInput>(unit);

        task->set_type(PbSparkTask::INPUT);
        return false;
    }
};

class AddCacheInputRule : public ExternalExecutorRule<CACHE_INPUT> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        PbSparkTask *task = TaskMessageOf(unit);
        *task->mutable_cache_input() = CheckAndGet<PbSparkTask::PbCacheInput>(unit);

        task->set_type(PbSparkTask::INPUT);
        return false;
    }
};

class AddShuffleInputRule : public ExternalExecutorRule<SHUFFLE_INPUT> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        PbSparkTask *task = TaskMessageOf(unit);
        *task->mutable_shuffle_input() = CheckAndGet<PbSparkTask::PbShuffleInput>(unit);

        task->set_type(PbSparkTask::GENERAL);
        return false;
    }
};

class AddShuffleOutputRule : public ExternalExecutorRule<SHUFFLE_OUTPUT> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        *TaskMessageOf(unit)->mutable_shuffle_output() = CheckAndGet<PbSparkTask::PbShuffleOutput>(unit);
        return false;
    }
};

class BuildTaskPbRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::TASK;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        int rdd_index = *unit->get<RddIndex>();

        PbSparkRDD* rdd = SparkJobOf(plan).mutable_rdd(rdd_index);
        CHECK_EQ(rdd_index, rdd->rdd_index());
        *rdd->add_task() = unit->get<PbSparkTask>();

        return false;
    }
};

class BuildRDDPbRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit == plan->Root();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        PbSparkJob& spark_rdds = SparkJobOf(plan);
        PbSparkJob* job = JobMessageOf(plan);
        for (int i = 0; i < spark_rdds.rdd_size(); ++i) {
            *job->add_rdd() = spark_rdds.rdd(i);
        }

        for (auto &kv : unit->get<NodeIdToCacheTasks>()) {
            job->mutable_job_info()->add_cache_node_id(kv.first);
            job->mutable_job_info()->add_cache_task_id(*kv.second->get<TaskIndex>());
        }

        return false;
    }
};

}  // anonymous namespace

BuildPhysicalPlanPass::~BuildPhysicalPlanPass() {}

bool BuildPhysicalPlanPass::Run(Plan* plan) {
    DepthFirstDispatcher first(DepthFirstDispatcher::PRE_ORDER);
    first.AddRule(new GenerateSparkJobRule());
    first.AddRule(new InitializeTaskRule());
    first.AddRule(new AddHadoopInputRule());
    first.AddRule(new AddCacheInputRule());
    first.AddRule(new AddShuffleInputRule());
    first.AddRule(new AddShuffleOutputRule());
    first.Run(plan);

    TaskDispatcher second;
    second.AddRule(new BuildTaskPbRule());
    second.Run(plan);

    DepthFirstDispatcher third(DepthFirstDispatcher::PRE_ORDER);
    third.AddRule(new BuildRDDPbRule());
    third.Run(plan);
    return false;
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
