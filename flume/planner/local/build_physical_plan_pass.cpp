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
// Author: bigflow-opensource@baidu.com
//

#include "flume/planner/local/build_physical_plan_pass.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace local {

namespace {
class GenerateLocalJobRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan *plan, Unit *unit) {
        return unit == plan->Root();
    }

    virtual bool Run(Plan *plan, Unit *root) {
        CHECK(!root->has<PbPhysicalPlan>());

        PbPhysicalPlan &pb_physical_plan = root->get<PbPhysicalPlan>();
        PbJob *pb_job = pb_physical_plan.add_job();
        pb_job->set_id("");
        pb_job->set_type(PbJob::LOCAL);

        if (!root->get<planner::JobConfig>().is_null()) {
            *pb_job->mutable_job_config() = *root->get<planner::JobConfig>();
        }

        pb_physical_plan.mutable_environment()->CopyFrom(*root->get<planner::Environment>());
        return false;
    }
};

class FillJobInputInfoRule : public RuleDispatcher::Rule {
    virtual bool Accept(Plan *plan, Unit *unit) {
        return unit->is_leaf() && unit->type() == Unit::LOAD_NODE;
    }

    virtual bool Run(Plan *plan, Unit *unit) {
        CHECK(unit->has<PbLogicalPlanNode>());
        CHECK(plan->Root()->has<PbPhysicalPlan>());

        PbLocalJob *local_job = plan->Root()->get<PbPhysicalPlan>().mutable_job(0)
                                ->mutable_local_job();
        PbLocalInput *local_input = local_job->add_input();

        // We have alraedy add a logic-executor between scope and logical node
        // in build_logical_executor_pass
        CHECK(unit->father()->father() != NULL);
        CHECK(unit->father()->father()->has<PbScope>());

        const PbScope &scope = unit->father()->father()->get<PbScope>();
        const PbLogicalPlanNode &logical_node = unit->get<PbLogicalPlanNode>();

        local_input->set_id(scope.id());
        *local_input->mutable_spliter() = logical_node.load_node().loader();
        *local_input->mutable_uri() = logical_node.load_node().uri();

        *local_job->mutable_task()->mutable_root() = plan->Root()->get<PbExecutor>();

        return false;
    }
};
}  // anonymous namespace

BuildPhysicalPlanPass::~BuildPhysicalPlanPass() {}

bool BuildPhysicalPlanPass::Run(Plan* plan) {
    RuleDispatcher first;
    first.AddRule(new GenerateLocalJobRule());
    first.Run(plan);

    RuleDispatcher second;
    second.AddRule(new FillJobInputInfoRule());
    return second.Run(plan);
}

}  // namespace local
}  // namespace planner
}  // namespace flume
}  // namespace baidu
