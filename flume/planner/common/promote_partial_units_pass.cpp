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
//         Pan Yuchang <bigflow-opensource@baidu.com>

#include <iterator>
#include <map>
#include <set>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/range/algorithm.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/promote_partial_units_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class PromotePartialUnitsPass::FullShuffleCountAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher clean_phase;
        clean_phase.AddRule(new CleanTags);
        clean_phase.Run(plan);

        DepthFirstDispatcher init_task_input(DepthFirstDispatcher::PRE_ORDER);
        init_task_input.AddRule(new MarkIsUnderDistributeEveryScope);
        init_task_input.Run(plan);

        TopologicalDispatcher calc_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        calc_phase.AddRule(new CalculateUnitInputTask);
        calc_phase.Run(plan);

        DepthFirstDispatcher mark_distribute(DepthFirstDispatcher::POST_ORDER);
        mark_distribute.AddRule(new CalculateDistributeEveryUnitTaskInput);
        mark_distribute.Run(plan);

        TopologicalDispatcher analyze_phase(TopologicalDispatcher::REVERSE_ORDER);
        analyze_phase.AddRule(new AnalyzeNonPartialUsageOfProcessNode);
        analyze_phase.AddRule(new AnalyzeNonPartialUsageOfOtherNodes);
        analyze_phase.Run(plan);

        TopologicalDispatcher count_phase(TopologicalDispatcher::REVERSE_ORDER);
        count_phase.AddRule(new CountForShuffleNode);
        count_phase.AddRule(new CountForUnionAndChannelNode);
        count_phase.Run(plan);

        return false;
    }

    struct HasNonPartialUser {};

    class CleanTags : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<FullShuffleCount>();
            unit->clear<HasNonPartialUser>();
            unit->clear<IsUnderDistributeEveryScope>();
            unit->clear<DistributeEveryScopeUnit>();
            unit->clear<UnitInputTasks>();
            return false;
        }
    };

    class AnalyzeNonPartialUsageOfProcessNode : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::PROCESS_NODE;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            std::set<std::string> non_partial_inputs;
            PbProcessNode node = unit->get<PbLogicalPlanNode>().process_node();
            for (int i = 0; i < node.input_size(); ++i) {
                if (!node.input(i).is_partial()) {
                    non_partial_inputs.insert(node.input(i).from());
                }
            }

            /* if unit is under distribute every scope, it is not partial */
            bool unit_not_move_under_distribute = false;
            if (unit->has<IsUnderDistributeEveryScope>()) {
                Unit* scope_unit = unit->get<DistributeEveryScopeUnit>();
                if (scope_unit->get<UnitInputTasks>().size() > 1u) {
                    unit_not_move_under_distribute = true;
                }
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (!unit->has<IsUnderDistributeEveryScope>() &&
                        need->task() == unit->task() &&
                        non_partial_inputs.count(need->identity())) {
                    need->set<HasNonPartialUser>();
                }
                if (need->task() == unit->task() &&
                        unit_not_move_under_distribute) {
                    need->set<HasNonPartialUser>();
                }
            }

            return false;
        }
    };

    class AnalyzeNonPartialUsageOfOtherNodes : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            switch (unit->type()) {
                case Unit::SINK_NODE:
                    return true;
                case Unit::UNION_NODE:
                    {
                        if (unit->has<IsUnderDistributeEveryScope>()) {
                            Unit* scope_unit = unit->get<DistributeEveryScopeUnit>();
                            if (scope_unit->get<UnitInputTasks>().size() > 1u) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                        return unit->has<HasNonPartialUser>();
                    }
                default:
                    return false;
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() == unit->task()) {
                    need->set<HasNonPartialUser>();
                }
            }
            return false;
        }
    };

    class CountForShuffleNode : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_NODE;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            *unit->get<FullShuffleCount>() = unit->has<HasNonPartialUser>() ? 1 : 0;
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                *unit->get<FullShuffleCount>() += *user->get<FullShuffleCount>();
            }
            return false;
        }
    };

    class CountForUnionAndChannelNode : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::UNION_NODE || unit->type() == Unit::CHANNEL;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            *unit->get<FullShuffleCount>() = 0;
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                *unit->get<FullShuffleCount>() += *user->get<FullShuffleCount>();
            }
            return false;
        }
    };

    class MarkIsUnderDistributeEveryScope : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {

            if (IsDistributeScopeUnit(unit)) {
                return true;
            }

            Unit* father = unit->father();
            if (father && father->has<IsUnderDistributeEveryScope>()) {
                return true;
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsUnderDistributeEveryScope>();
            Unit* scope_unit = NULL;

            if (IsDistributeScopeUnit(unit)) {
                scope_unit = unit;
            } else if (unit->father()->has<DistributeEveryScopeUnit>()) {
                scope_unit = unit->father()->get<DistributeEveryScopeUnit>();
            }
            CHECK_NOTNULL(scope_unit);
            unit->get<DistributeEveryScopeUnit>().Assign(scope_unit);
            return false;
        }

        bool IsDistributeScopeUnit(Unit* unit) {
            if (unit->has<PbScope>() && unit->get<PbScope>().distribute_every()) {
                return true;
            }
            return false;
        }
    };

    class CalculateUnitInputTask : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            UnitInputTasks& input_tasks = unit->get<UnitInputTasks>();
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    // task terminal
                    input_tasks.insert(need->task());
                } else {
                    // not task terminal
                    UnitInputTasks& need_input_tasks = need->get<UnitInputTasks>();
                    std::copy(need_input_tasks.begin(), need_input_tasks.end(),
                            std::inserter(input_tasks, input_tasks.begin()));
                }
            }
            return false;
        }

    };

    class CalculateDistributeEveryUnitTaskInput : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }
        virtual bool Run(Plan* plan, Unit* unit) {
            UnitInputTasks& tasks = unit->get<UnitInputTasks>();
            BOOST_FOREACH(Unit* child, unit->children()) {
                UnitInputTasks& input_tasks = child->get<UnitInputTasks>();
                std::copy(input_tasks.begin(), input_tasks.end(),
                        std::inserter(tasks, tasks.begin()));
            }
            return false;
        }
    };

};

bool PromotePartialUnitsPass::FullShuffleCountAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
};

class PromotePartialUnitsPass::PromotableAnalysis::Impl {
public:
    static bool Run(Plan* plan) {

        TopologicalDispatcher dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        dispatcher.AddRule(new CleanTags);
        dispatcher.AddRule(new SetIsBoundry);
        dispatcher.AddRule(new AnalyzeShuffleNode);
        dispatcher.AddRule(new AnalyzeStreamShuffleNode);
        dispatcher.AddRule(new AnalyzeUnionNode);
        dispatcher.AddRule(new AnalyzeProcessNode);
        bool is_changed = dispatcher.Run(plan);

        /*
         * For units under distribute every scope, if the input of the scope comes
         * from the same task, we will promote all units under this scope, otherwise stop
         * promoting.
         */
        TopologicalDispatcher analyze_unit_under_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        analyze_unit_under_phase.AddRule(new AnalyzeNodeUnderDistributedByRecord);
        is_changed |= analyze_unit_under_phase.Run(plan);

        TopologicalDispatcher remove_dispatcher(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        remove_dispatcher.AddRule(new ExcludeDownstreamCandidates);
        is_changed |= remove_dispatcher.Run(plan);

        return is_changed;
    }

    struct IsBoundry {};
    struct HasPromotableUpstreams {};
    class CleanTags : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<IsBoundry>();
            unit->clear<ShouldPromote>();
            unit->clear<HasPromotableUpstreams>();
            unit->clear<OnlyClone>();
            return false;
        }
    };

    class SetIsBoundry : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task() || need->type() == Unit::CHANNEL) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsBoundry>();
            return false;
        }
    };

    class SetShouldPromoteRule : public RuleDispatcher::Rule {
    public:
        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<ShouldPromote>();
            DrawPlanPass::AddMessage(unit, "should promote to upstream task");
            return true;
        }
    };

    class AnalyzeShuffleNode : public SetShouldPromoteRule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_NODE || !unit->has<IsBoundry>()) {
                return false;
            }

            if (unit->has<Cloned>()) {
                return false;
            }

            const PbShuffleNode& node = unit->get<PbLogicalPlanNode>().shuffle_node();
            if (node.type() == PbShuffleNode::BROADCAST) {
                // promote broadcast unit will cause more transfer datas
                return false;
            }

            if (unit->has<IsInfinite>()
                    && unit->father()->father() != unit->task()) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    // always move heading shuffle node to upstream task
                    return true;
                }

                int self_shuffles = *unit->get<FullShuffleCount>();
                int extra_shuffles = *need->get<FullShuffleCount>() - self_shuffles;
                if (self_shuffles > 0 && extra_shuffles > 0) {
                    // promote this node will cause extra shuffles
                    return false;
                }
            }

            return true;
        }
    };

    class AnalyzeUnionNode : public SetShouldPromoteRule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::UNION_NODE || !unit->has<IsBoundry>()) {
                return false;
            }

            if (unit->has<IsInfinite>()) {
                return false;
            }

            if (unit->father()->has<IsInfinite>()) {
                return false;
            }

            return true;
        }
    };

    class AnalyzeProcessNode : public SetShouldPromoteRule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::PROCESS_NODE || !unit->has<IsBoundry>()) {
                return false;
            }

            const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
            for (int i = 0; i < node.input_size(); ++i) {
                if (!node.input(i).is_partial()) {
                    return false;
                }
            }

            if (unit->has<IsInfinite>()) {
                return false;
            }

            if (unit->father()->has<IsInfinite>()) {
                return false;
            }

            return true;
        }
    };

    class AnalyzeNodeUnderDistributedByRecord : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() == Unit::CHANNEL || !unit->has<IsBoundry>()) {
                return false;
            }
            return unit->has<IsUnderDistributeEveryScope>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* scope_unit = unit->get<DistributeEveryScopeUnit>();
            if (scope_unit->get<UnitInputTasks>().size() > 1u) {
                unit->clear<ShouldPromote>();
                return false;
            } else {
                unit->set<ShouldPromote>();
                DrawPlanPass::AddMessage(unit, "should promote to upstream task");
                return true;
            }
        }
    };

    class AnalyzeStreamShuffleNode : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_NODE || !unit->has<ShouldPromote>()) {
                return false;
            }

            if (!unit->father()->has<IsInfinite>()) {
                return false;
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<OnlyClone>();
            DrawPlanPass::AddMessage(unit, "should clone to upstream task");
            return false;
        }
    };

    class ExcludeDownstreamCandidates : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->has<ShouldPromote>() || need->has<HasPromotableUpstreams>()) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldPromote>();
            unit->set<HasPromotableUpstreams>();
            return false;
        }
    };
};

bool PromotePartialUnitsPass::PromotableAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}

class PromotePartialUnitsPass::PromoteUnitsPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher clean_phase;
        clean_phase.AddRule(new CleanTags);
        clean_phase.Run(plan);

        TopologicalDispatcher analyze_need_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        analyze_need_phase.AddRule(new UpdateRealNeedsForChannel);
        analyze_need_phase.AddRule(new UpdateRealNeedsForOthers);
        analyze_need_phase.Run(plan);

        DepthFirstDispatcher analyze_copy_phase(DepthFirstDispatcher::POST_ORDER);
        analyze_copy_phase.AddRule(new CollectCopyToTasksForLeafs);
        analyze_copy_phase.AddRule(new CollectCopyToTasksForControls);
        analyze_copy_phase.AddRule(new RegisterCopyForTasks);
        analyze_copy_phase.Run(plan);

        DepthFirstDispatcher copy_phase(DepthFirstDispatcher::PRE_ORDER);
        copy_phase.AddRule(new MakeCopyForControls);
        copy_phase.AddRule(new MakeCopyForLeafs);
        copy_phase.AddRule(new MakeCopyForStreamShuffleNode);
        bool changed = copy_phase.Run(plan);

        RuleDispatcher remove_phase;
        remove_phase.AddRule(new RemoveOnlyChannelPathRule);
        remove_phase.AddRule(new RemoveOnlyChannelUnderShuffleExecutor);
        changed |= remove_phase.Run(plan);

        return changed;
    }

    // real needs across channel units
    struct RealNeeds : public std::set<Unit*> {};

    // target tasks which this unit will be copied to
    struct CopyToTasks : public std::set<Unit*> {};

    // id of channel between need and unit
    struct ChannelIdentity : public Value<std::string> {};

    class CleanTags : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<CopyToTasks>();
            unit->clear<RealNeeds>();
            return false;
        }
    };

    class UpdateRealNeedsForChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CHANNEL;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                boost::copy(unit->get<RealNeeds>(), Inserter(user->get<RealNeeds>()));
            }
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                *need->get<ChannelIdentity>() = unit->identity();
            }
            return false;
        }
    };

    class UpdateRealNeedsForOthers : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() != Unit::CHANNEL;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                user->get<RealNeeds>().insert(unit);
            }
            return false;
        }
    };

    class CollectCopyToTasksForLeafs : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf() && unit->has<ShouldPromote>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->get<RealNeeds>()) {
                unit->get<CopyToTasks>().insert(need->task());
            }
            unit->clear<ShouldPromote>();
            return false;
        }
    };

    class CollectCopyToTasksForControls : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() > Unit::TASK && unit->has<CopyToTasks>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->father()->get<CopyToTasks>().insert(unit->get<CopyToTasks>().begin(),
                                                      unit->get<CopyToTasks>().end());
            if (unit->has<OnlyClone>()) {
                unit->father()->set<OnlyClone>();
            }
            return false;
        }
    };

    class RegisterCopyForTasks : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<CopyToTasks>() && unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* task, unit->get<CopyToTasks>()) {
                unit->get<Copies>()[task] = task;  // just a workaround
            }
            return false;
        }
    };

    class MakeCopyForControls : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->has<Cloned>()) {
                return false;
            }
            return unit->has<CopyToTasks>() && unit->type() > Unit::TASK && !unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            bool is_changed = false;
            BOOST_FOREACH(Unit* task, unit->get<CopyToTasks>()) {
                if (task == unit->task() || unit->get<Copies>()[task] != NULL) {
                    continue;
                }

                Unit* copy = unit->clone();
                copy->clear<IsLocalDistribute>();  // FIXME(wenxiang): add un-clonable tags
                copy->clear<IsDistributeAsBatch>();  // FIXME(wenxiang): add un-clonable tags)
                plan->AddControl(unit->father()->get<Copies>()[task], copy);

                copy->get<Origin>().Assign(unit);
                unit->get<Copies>()[task] = copy;

                is_changed = true;
            }
            if (unit->has<OnlyClone>()) {
                unit->set<Cloned>();
            }
            return is_changed;
        }
    };

    class MakeCopyForLeafs : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->has<OnlyClone>() || unit->has<Cloned>()) {
                return false;
            }
            return unit->has<CopyToTasks>() && unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            // remove old dependency
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                plan->RemoveDependency(need, unit);
            }

            // make copies
            BOOST_FOREACH(Unit* task, unit->get<CopyToTasks>()) {
                if (unit->get<Copies>().count(task) > 0) {
                    continue;
                }

                Unit* copy = unit->clone();
                copy->set<IsPromoted>();
                copy->get<Origin>().Assign(unit);
                unit->get<Copies>()[task] = copy;

                // set control
                if (task == unit->task()) {
                    plan->AddControl(unit->father(), copy);
                } else {
                    plan->AddControl(unit->father()->get<Copies>()[task], copy);
                }

                // add new dependency
                BOOST_FOREACH(Unit* need, unit->get<RealNeeds>()) {
                    if (need->task() == task) {
                        plan->AddDependency(need, copy);
                        if (need->has<ChannelIdentity>()) {
                            std::string origin_identity = *need->get<ChannelIdentity>();
                            plan->ReplaceFrom(copy, origin_identity, need->identity());
                        }
                    }
                }
                plan->AddDependency(copy, unit);
            }

            unit->set_type(Unit::CHANNEL);
            *unit->get<OriginIdentity>() = unit->identity();
            return true;
        }
    };

    class MakeCopyForStreamShuffleNode : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_NODE) {
                return false;
            }
            if (unit->has<Cloned>()) {
                return false;
            }
            return unit->has<CopyToTasks>() && unit->has<OnlyClone>() && unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            CHECK_EQ(1u,  unit->get<CopyToTasks>().size());

            // remove old dependency
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                plan->RemoveDependency(need, unit);
            }

            // make copies
            BOOST_FOREACH(Unit* task, unit->get<CopyToTasks>()) {
                if (unit->get<Copies>().count(task) > 0) {
                    continue;
                }

                Unit* copy = unit->clone();
                copy->set<IsPromoted>();
                copy->get<Origin>().Assign(unit);
                unit->get<Copies>()[task] = copy;
                std::string from_identity = copy->get<PbLogicalPlanNode>().shuffle_node().from();
                std::string copy_identity = copy->change_identity();
                copy->get<PbLogicalPlanNode>().set_id(copy_identity);
                plan->ReplaceFrom(unit, from_identity, copy_identity);

                // set control
                if (task == unit->task()) {
                    plan->AddControl(unit->father(), copy);
                } else {
                    plan->AddControl(unit->father()->get<Copies>()[task], copy);
                }

                // add new dependency
                BOOST_FOREACH(Unit* need, unit->get<RealNeeds>()) {
                    if (need->task() == task) {
                        plan->AddDependency(need, copy);
                        if (need->has<ChannelIdentity>()) {
                            std::string origin_identity = *need->get<ChannelIdentity>();
                            plan->ReplaceFrom(copy, origin_identity, need->identity());
                        }
                    }
                }
                plan->AddDependency(copy, unit);
            }

            unit->set<Cloned>();
            *unit->get<OriginIdentity>() = unit->identity();
            return true;
        }
    };

    class RemoveOnlyChannelPathRule : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL) {
                return false;
            }

            CHECK(!unit->direct_needs().empty());
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() == unit->task()) {
                    return false;
                }
            }

            if (unit->direct_users().size() != 1) {
                return false;
            }
            return unit->direct_users().front()->task() != unit->task();
        }

        virtual bool Run(Plan* plan, Unit* unit) {

            Unit* user = unit->direct_users().front();
            plan->RemoveDependency(unit, user);
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                plan->AddDependency(need, user);
            }
            return true;
        }
    };

    class RemoveOnlyChannelUnderShuffleExecutor : public RuleDispatcher::Rule {
        /*
         * replace dependency of channel under SHUFFLE_EXECUTOR if there
         * exists only channel
         */
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<PbScope>() || !unit->get<PbScope>().distribute_every()) {
                return false;
            }
            if (unit->children().size() < 1u) {
                return false;
            }
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() != Unit::CHANNEL) {
                    return false;
                }
            }
            return true;
        }
        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* child, unit->children()) {
                plan->RemoveControl(unit, child);
                plan->AddControl(unit->father(), child);
            }
            return true;
        }
    };
};

bool PromotePartialUnitsPass::PromoteUnitsPass::Run(Plan* plan) {
    return Impl::Run(plan);
};

class PromotePartialUnitsPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new RenameLocalPromotedUnits);
        dispatcher.AddRule(new SetIsPartial);
        dispatcher.AddRule(new ClearIsPromoted);
        return dispatcher.Run(plan);
    }

    // rename local channel unit
    class RenameLocalPromotedUnits : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<IsPromoted>() && unit->get<Origin>()->task() == unit->task();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* origin = unit->get<Origin>();
            CHECK(origin->has<PbLogicalPlanNode>());

            std::string old_identity = origin->identity();
            std::string new_identity = origin->change_identity();
            origin->get<PbLogicalPlanNode>().set_id(new_identity);
            BOOST_FOREACH(Unit* user, origin->direct_users()) {
                if (user->type() != Unit::CHANNEL) {
                    plan->ReplaceFrom(user, old_identity, new_identity);
                }
            }
            return true;
        }
    };

    class SetIsPartial : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<IsPromoted>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsPartial>();
            DrawPlanPass::UpdateLabel(unit, "20-is-partial", "IsPartial");
            return false;
        }
    };

    class ClearIsPromoted : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<IsPromoted>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<IsPromoted>();
            return false;
        }
    };
};

bool PromotePartialUnitsPass::Run(Plan* plan) {
    return Impl::Run(plan);
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

