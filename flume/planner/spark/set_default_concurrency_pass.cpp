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

#include "flume/planner/spark/set_default_concurrency_pass.h"

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/flags.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/task_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class SetDefaultConcurrencyPass::SetTaskConcurrencyAndBucketSize::Impl {
public:
    static bool Run(Plan* plan) {
        TaskDispatcher dispatcher1(TaskDispatcher::POST_TOPOLOGY_ORDER);
        dispatcher1.AddRule(new SetTaskConcurrencyRule);
        dispatcher1.Run(plan);

        DepthFirstDispatcher dispatcher2(DepthFirstDispatcher::PRE_ORDER);
        dispatcher2.AddRule(new SetBucketSizeRule);
        dispatcher2.Run(plan);

        return false;
    }

private:
    class SetTaskConcurrencyRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbSparkTask* message = &unit->get<PbSparkTask>();
            if (unit->has<TaskConcurrency>()) {
                message->set_concurrency(*unit->get<TaskConcurrency>());
            }  else if (!unit->has<IsHadoopInput>()) {
                uint32_t default_concurrency =
                    FlumeDefaultConcurrency(plan->Root()->get<JobConfig>());
                *unit->get<TaskConcurrency>() = default_concurrency;
                message->set_concurrency(default_concurrency);
            }

            std::ostringstream label;
            label << "partition number: ";

            if (message->has_concurrency()) {
                label << message->concurrency();
            } else {
                label << "none";
            }
            DrawPlanPass::UpdateLabel(unit, "40-task-number", label.str());

            return false;
        }

        uint32_t FlumeDefaultConcurrency(const JobConfig& job_config) {
            if (job_config.is_null()) {
                return FLAGS_flume_default_concurrency;
            }
            if (job_config->has_default_concurrency()) {
                return job_config->default_concurrency();
            }
            return FLAGS_flume_default_concurrency;
        }

    };

    class SetBucketSizeRule : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() <= Unit::TASK) {
                return false;
            }
            if (unit->father()->type() != Unit::TASK || unit->father()->has<IsHadoopInput>()) {
                return false;
            }
            if (unit->is_leaf() || !unit->has<PbScope>() || !unit->has<IsLocalDistribute>()) {
                // leaf unit
                // or control unit which do not has scope
                // or is not local distribute
                return false;
            }
            PbScope& pb_scope = unit->get<PbScope>();

            if (pb_scope.type() != PbScope::BUCKET) {
                return false;
            }

            if (pb_scope.mutable_bucket_scope()->has_bucket_size()) {
                return false;
            }

            return true;
        }

        bool Run(Plan* plan, Unit* unit) {
            // set bucket size with task concurrency
            Unit* task = unit->task();
            CHECK(task->has<TaskConcurrency>());
            int concurrency = *task->get<TaskConcurrency>();
            PbScope& pb_scope = unit->get<PbScope>();
            CHECK(pb_scope.has_bucket_scope());
            pb_scope.mutable_bucket_scope()->set_bucket_size(concurrency);
            std::string scope_id = unit->identity();
            unit->set<NotUserSetBucketSize>();

            // set promoted scope bucket size with the same concurrency
            std::set<Unit*> upstream_scopes;
            DataFlow& data_flow = unit->get<DataFlow>();
            BOOST_FOREACH(Unit* up_node, data_flow.upstreams) {
                upstream_scopes.insert(up_node->father());
            }
            BOOST_FOREACH(Unit* father, upstream_scopes) {
                CHECK(father->has<PbScope>());
                PbScope& pb_up_scope = father->get<PbScope>();
                if (pb_up_scope.id() == scope_id) {
                    CHECK(pb_up_scope.type() == PbScope::BUCKET);
                    CHECK(!pb_up_scope.bucket_scope().has_bucket_size());
                    pb_up_scope.mutable_bucket_scope()->set_bucket_size(concurrency);
                    father->set<NotUserSetBucketSize>();
                }
            }

            return false;
        }
    };
};

bool SetDefaultConcurrencyPass::SetTaskConcurrencyAndBucketSize::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

