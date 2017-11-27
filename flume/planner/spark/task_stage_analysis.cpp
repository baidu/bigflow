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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
//         Wang Cong<wangcong09@baidu.com>
// Description:

#include "flume/planner/spark/task_stage_analysis.h"

#include <algorithm>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"

#include "flume/flags.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class TaskStageAnalysis::BuildRddsPass::Impl {
public:
    static bool Run(Plan* plan) {
        plan->Root()->clear<PbSparkJob>();

        RuleDispatcher cleaner;
        cleaner.AddRule(new ClearTagRule);
        cleaner.Run(plan);

        TaskDispatcher index_builder(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        index_builder.AddRule(new BuildRddIndexRule);
        index_builder.Run(plan);

        TaskDispatcher post_index_builder(TaskDispatcher::POST_TOPOLOGY_ORDER);
        post_index_builder.AddRule(new BuildRddIndexPostRule);
        post_index_builder.Run(plan);

        TaskDispatcher pre_task_setter(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        pre_task_setter.AddRule(new FreshPreTaskInSameRddRule);
        pre_task_setter.Run(plan);

        plan->Root()->get<Debug>(); // draw pass result
        return false;
    }

    class ClearTagRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<RddIndex>();
            return false;
        }
    };

    // BuildRddIndexRule and BuildRddIndexPostRule:
    //
    // Mark RddIndex through a 'topological level' of a task, for example
    // t1->t2->t3->t4, t5->t6->t4
    // t1,t5 will be marked RddIndex with 0
    // t2 will be marked RddIndex with 1
    // t3,t6 will be marked RddIndex with 2
    // t4 will be marked RddIndex with 3
    class BuildRddIndexRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            CHECK(unit->has<TaskFlow>());
            TaskFlow& info = unit->get<TaskFlow>();
            int level = 0;
            BOOST_FOREACH(Unit* task, info.input_tasks) {
                if (*task->get<RddIndex>() + 1 > level) {
                    level = *task->get<RddIndex>() + 1;
                }
            }
            *unit->get<RddIndex>() = level;
            return true;
        }
    };

    class BuildRddIndexPostRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            CHECK(unit->has<TaskFlow>());
            TaskFlow& info = unit->get<TaskFlow>();
            if (info.output_tasks.empty() || info.input_tasks.empty()) {
                return false;
            }
            int level = *((*info.output_tasks.begin())->get<RddIndex>());
            BOOST_FOREACH(Unit* task, info.output_tasks) {
                if (*task->get<RddIndex>() < level) {
                    level = *task->get<RddIndex>();
                }
            }
            *unit->get<RddIndex>() = level - 1;
            return true;
        }
    };

    /**
     * For tasks in same rdd, set PreTaskInSameRdd tag for another task
     * in same rdd
     */
    class FreshPreTaskInSameRddRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            int rdd_id = *unit->get<RddIndex>();
            Unit* pre_task_in_same_rdd = _rdd_to_task[rdd_id];
            if (pre_task_in_same_rdd == NULL) {
                pre_task_in_same_rdd = unit;
            }

            // set pre tesk which shares same rdd index
            unit->get<PreTaskInSameRdd>().Assign(pre_task_in_same_rdd);

            _rdd_to_task[rdd_id] = unit;

            std::string label = "RDD: " +
                boost::lexical_cast<std::string>(rdd_id);
            DrawPlanPass::UpdateLabel(unit, "10-rdd", label);
            return false;
        }

    private:
        // last task which shared same rdd index
        std::map<int, Unit*> _rdd_to_task;
    };
};

bool TaskStageAnalysis::BuildRddsPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

