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

#include "flume/planner/local/build_local_input_executor_pass.h"

#include "flume/planner/common/cache_util.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace local {

namespace {

class BuildLocalLoaderExecutorRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan *plan, Unit *unit) {
        if (!unit->has<PbScope>()) {
            return false;
        }

        return unit->get<PbScope>().type() == PbScope::INPUT;
    }

    virtual bool Run(Plan *plan, Unit *unit) {
        CHECK(unit->has<PbScope>());

        PbExecutor &pb_executor = unit->get<PbExecutor>();
        if (!unit->has<IsInfinite>()) {
            unit->set_type(Unit::EXTERNAL_EXECUTOR);
            pb_executor.set_type(PbExecutor::EXTERNAL);
            pb_executor.mutable_external_executor()->set_id(unit->get<PbScope>().id());
        } else {
            unit->set_type(Unit::STREAM_EXTERNAL_EXECUTOR);
            pb_executor.set_type(PbExecutor::STREAM_EXTERNAL);
            pb_executor.mutable_stream_external_executor()->set_id(unit->get<PbScope>().id());
        }

        /* hack for cache logic
        PbScope &pb_scope = unit->get<PbScope>();
        if (pb_scope.input_scope().spliter().name() == "SequenceFileAsBinaryInputFormat") {
            *(pb_scope.mutable_input_scope()->mutable_spliter()) = LocalCacheLoaderEntity();

            typedef std::vector<Unit*>::iterator Iter;
            // Change load node under this scope also
            for (Iter it = unit->children().begin();
                 it != unit->children().end(); ++it) {
                Unit *child = *it;
                if (child->has<PbLogicalPlanNode>()
                    && child->get<PbLogicalPlanNode>().type() == PbLogicalPlanNode::LOAD_NODE) {
                    PbLogicalPlanNode &pb_logical_node = child->get<PbLogicalPlanNode>();
                    CHECK(pb_logical_node.has_load_node());
                    CHECK(pb_logical_node.load_node().loader().name() == "SequenceFileAsBinaryInputFormat");
                    *(pb_logical_node.mutable_load_node()->mutable_loader()) = LocalCacheLoaderEntity();
                }
            }
        }
        */

        return false;
    }
};
} // anonymous namespace

bool BuildLocalInputExecutorPass::Run(Plan *plan) {
    RuleDispatcher rule_dispatcher;
    rule_dispatcher.AddRule(new BuildLocalLoaderExecutorRule());
    return rule_dispatcher.Run(plan);
}

} // namespace local
} // namespace planner
} // namespace flume
} // namespace baidu
