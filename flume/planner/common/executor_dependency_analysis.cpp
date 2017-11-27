/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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

#include "flume/planner/common/executor_dependency_analysis.h"

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class ExecutorDependencyAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher init_phase;
        init_phase.AddRule(new CleanTags);
        init_phase.AddRule(new FindTransferInputExecutor);
        init_phase.Run(plan);

        DepthFirstDispatcher update_phase(DepthFirstDispatcher::PRE_ORDER);
        update_phase.AddRule(new UpdateSubExecutors);
        update_phase.AddRule(new UpdateLeadingExecutors);
        update_phase.AddRule(new UpdateUpstreamDispatchers);
        update_phase.Run(plan);

        return false;
    }

    struct IsTransferInput {};

    class CleanTags : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<SubExecutors>();
            unit->clear<LeadingExecutors>();
            unit->clear<UpstreamDispatchers>();

            unit->clear<IsTransferInput>();

            return false;
        }
    };

    class FindTransferInputExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::EXTERNAL_EXECUTOR) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() != Unit::CHANNEL) {
                    return false;
                }

                BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                    if (need->task() == unit->task()) {
                        return false;
                    }
                }
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsTransferInput>();
            return false;
        }
    };

    class UpdateSubExecutors : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->type() >= Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            SubExecutors* sub_executors = &unit->get<SubExecutors>();
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (!child->is_leaf()) {
                    sub_executors->insert(child);
                }
            }
            return false;
        }
    };

    class UpdateLeadingExecutors : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->type() > Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const SubExecutors& brothers = unit->father()->get<SubExecutors>();

            LeadingExecutors* leaders = &unit->get<LeadingExecutors>();
            BOOST_FOREACH(const DataFlow::Dependency& dep, unit->get<DataFlow>().inputs) {
                Unit* from = dep.first;
                BOOST_FOREACH(Unit* executor, brothers) {
                    if (!executor->has<IsTransferInput>() && from->is_descendant_of(executor)) {
                        leaders->insert(executor);
                    }
                }
            }

            return false;
        }
    };

    class UpdateUpstreamDispatchers : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->type() > Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            UpstreamDispatchers* dispatchers = &unit->get<UpstreamDispatchers>();
            BOOST_FOREACH(Unit* upstream, unit->get<DataFlow>().upstreams) {
                if (upstream->father() == unit->father()) {
                    dispatchers->insert(upstream);
                    continue;
                }

                if (upstream->father()->has<IsTransferInput>()
                        && upstream->is_descendant_of(unit->father())) {
                    dispatchers->insert(upstream);
                    continue;
                }
            }
            return false;
        }
    };
};

bool ExecutorDependencyAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
