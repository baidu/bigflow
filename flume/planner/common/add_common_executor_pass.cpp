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
// Author: Wen Xiang <wenxiang@baidu.com>
//

#include "flume/planner/common/add_common_executor_pass.h"

#include "boost/foreach.hpp"
#include "boost/range/adaptors.hpp"
#include "boost/range/algorithm.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/partial_executor.h"

namespace baidu {
namespace flume {
namespace planner {

class AddCommonExecutorPass::PartialAnalysis::Impl {
public:
    struct DependantDispatchers : std::set<Unit*> {};

    static void InitTags(Plan* plan) {
        RuleDispatcher clean_phase;
        clean_phase.AddRule(new ClearTags);
        clean_phase.Run(plan);

        DepthFirstDispatcher init_under_partial_phase(DepthFirstDispatcher::PRE_ORDER);
        init_under_partial_phase.AddRule(new SetIsUnderPartialExecutor);
        init_under_partial_phase.Run(plan);

        RuleDispatcher init_streaming_phase;
        init_streaming_phase.AddRule(new SetIsStreamingUnit);
        init_streaming_phase.Run(plan);

        TopologicalDispatcher init_dependant_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        init_dependant_phase.AddRule(new UpdateDependantDispatchers);
        init_dependant_phase.Run(plan);
    }

    class ClearTags : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<IsUnderPartialExecutor>();
            unit->clear<IsStreamingUnit>();
            unit->clear<ShouldUnderPartialExecutor>();
            unit->clear<ShouldBeSeparated>();
            unit->clear<DependantDispatchers>();

            return false;
        }
    };

    class SetIsStreamingUnit : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            switch (unit->type()) {
                case Unit::UNION_NODE:
                case Unit::SHUFFLE_NODE: {
                    return true;
                }
                case Unit::PROCESS_NODE: {
                    const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
                    if (node.least_prepared_inputs() > 0) {
                        return false;
                    }

                    for (int i = 0; i < node.input_size(); ++i) {
                        const PbProcessNode::Input& input = node.input(i);
                        if (input.is_prepared()) {
                            return false;
                        }
                    }

                    return true;
                }
                default: {
                    return false;
                }
            }
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsStreamingUnit>();
            return false;
        }
    };

    class SetIsUnderPartialExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            Unit* father = unit->father();
            if (father == NULL) {
                return false;
            }

            return father->type() == Unit::PARTIAL_EXECUTOR
                    || father->has<IsUnderPartialExecutor>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsUnderPartialExecutor>();
            return false;
        }
    };

    class UpdateDependantDispatchers : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<IsUnderPartialExecutor>()
                    && unit->father()->type() != Unit::PARTIAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            DependantDispatchers& dispatchers = unit->get<DependantDispatchers>();
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->father()->type() == Unit::PARTIAL_EXECUTOR) {
                    dispatchers.insert(need);
                } else {
                    dispatchers.insert(need->get<DependantDispatchers>().begin(),
                                       need->get<DependantDispatchers>().end());
                }
            }

            return false;
        }
    };

    static void AnalyzeShouldUnderPartialExecutor(Plan* plan) {
        TopologicalDispatcher leaf_search_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        leaf_search_phase.AddRule(new IncludePartialLeafs);
        leaf_search_phase.Run(plan);

        DepthFirstDispatcher control_search_phase(DepthFirstDispatcher::POST_ORDER);
        control_search_phase.AddRule(new IncludePartialControls);
        control_search_phase.Run(plan);

        bool is_changed = false;
        do {
            // recursively exluding ShouldUnderPartialExecutor, according to the
            // implementation of PartialExecutor

            TopologicalDispatcher exclude_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
            exclude_phase.AddRule(new ExcludeHeadingNonStreamingLeafs);
            exclude_phase.AddRule(new ExcludeHeadingChannelUnits);
            is_changed = exclude_phase.Run(plan);

            TopologicalDispatcher reverse_exclude_phase(TopologicalDispatcher::REVERSE_ORDER);
            reverse_exclude_phase.AddRule(new ExcludeIsolatedPartialLeafs);
            is_changed |= reverse_exclude_phase.Run(plan);

            DepthFirstDispatcher clean_phase(DepthFirstDispatcher::POST_ORDER);
            clean_phase.AddRule(new CleanByNonPartialChild);
            is_changed |= clean_phase.Run(plan);

            DepthFirstDispatcher reverse_clean_phase(DepthFirstDispatcher::PRE_ORDER);
            reverse_clean_phase.AddRule(new CleanByNonPartialFather);
            is_changed |= reverse_clean_phase.Run(plan);
        } while (is_changed);
    }

    class IncludePartialLeafs : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->has<IsUnderPartialExecutor>()) {
                return false;
            }

            if (unit->has<IsPartial>() && !unit->has<IsInfinite>()) {
                // IsPartial is the most basic traits to enable PartialExecutor
                return true;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    continue;
                }

                if (need->has<IsUnderPartialExecutor>()) {
                    // PartialExecutor should not have outside user
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<ShouldUnderPartialExecutor>();
            return false;
        }
    };

    class IncludePartialControls : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_leaf()
                    || unit->type() <= Unit::TASK
                    || unit->type() == Unit::PARTIAL_EXECUTOR) {
                return false;
            }

            if (unit->has<IsInfinite>()) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (!child->has<ShouldUnderPartialExecutor>()) {
                    // if any child is not suitable, the whole executor should not be
                    // placed under PartialExecutor
                    return false;
                }
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<ShouldUnderPartialExecutor>();
            return false;
        }
    };

    // Heading non-streaming units do not need partial buffer, DatasetManager is good for
    // managing memory usage at such case
    class ExcludeHeadingNonStreamingLeafs : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<ShouldUnderPartialExecutor>()
                    || unit->type() == Unit::CHANNEL
                    || unit->has<IsStreamingUnit>()) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    continue;
                }

                if (!need->has<IsUnderPartialExecutor>()
                        && !need->has<ShouldUnderPartialExecutor>()) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldUnderPartialExecutor>();
            return true;
        }
    };

    // Upstream boundry of a task can not make use of partial buffer, they may contain
    // keys along with records, which are not handled by PartialExecutor
    class ExcludeHeadingChannelUnits : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<ShouldUnderPartialExecutor>() || unit->type() != Unit::CHANNEL) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    return true;
                }

                if (need->has<IsUnderPartialExecutor>()
                        || need->has<ShouldUnderPartialExecutor>()) {
                    return false;
                }
            }
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldUnderPartialExecutor>();
            return true;
        }
    };

    // The implementation of PartialExecutor forbid it has outside outputs, so we should
    // may sure if some unit is under PartialExecutor, all its downstreams in same task
    // should also be under PartialExecutor
    class ExcludeIsolatedPartialLeafs : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<ShouldUnderPartialExecutor>()) {
                return false;
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->task() != unit->task()) {
                    continue;
                }

                if (!user->has<IsUnderPartialExecutor>()
                        && !user->has<ShouldUnderPartialExecutor>()) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldUnderPartialExecutor>();
            return true;
        }
    };

    class CleanByNonPartialChild : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_leaf()
                    || unit->type() <= Unit::TASK
                    || !unit->has<ShouldUnderPartialExecutor>()) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (!child->has<ShouldUnderPartialExecutor>()) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldUnderPartialExecutor>();
            return true;
        }
    };

    class CleanByNonPartialFather : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            Unit* father = unit->father();
            if (father == NULL
                    || father == unit->task()
                    || father->has<ShouldUnderPartialExecutor>()) {
                return false;
            }

            return unit->has<ShouldUnderPartialExecutor>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldUnderPartialExecutor>();
            return true;
        }
    };

    static void AnalyzeShouldBeSeparated(Plan* plan) {
        TopologicalDispatcher search_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        search_phase.AddRule(new SetShouldBeSeparatedForScopeChannel);
        search_phase.AddRule(new SearchShouldBeSeparatedUnits);
        search_phase.Run(plan);

        TopologicalDispatcher exclude_phase(TopologicalDispatcher::REVERSE_ORDER);
        exclude_phase.AddRule(new ExcludeIncompleteSeparatedPath);
        exclude_phase.Run(plan);

        TopologicalDispatcher update_should_separated_phase(TopologicalDispatcher::REVERSE_ORDER);
        update_should_separated_phase.AddRule(new UpdateShouldSeparated);
        update_should_separated_phase.Run(plan);
    }

    class SetShouldBeSeparatedForScopeChannel : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->has<IsSeparated>()
                    && unit->has<IsUnderPartialExecutor>()
                    && unit->type() == Unit::CHANNEL
                    && unit->father()->type() == Unit::SCOPE
                    && unit->father()->father()->type() == Unit::PARTIAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<ShouldBeSeparated>().Assign(unit);
            return false;
        }
    };

    class SearchShouldBeSeparatedUnits : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->has<IsSeparated>()
                    || !unit->has<IsUnderPartialExecutor>()
                    || !unit->is_leaf()
                    || unit->father()->type() != Unit::SCOPE
                    || unit->get<DependantDispatchers>().size() != 1) {
                return false;
            }
            Unit* scope = unit->father();

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (!need->has<ShouldBeSeparated>()) {
                    return false;
                }

                if (need->father() == scope) {
                    // under same scope
                    continue;
                }

                if (unit->is_descendant_of(need->father())) {
                    // enter sub scope, so also under same scope
                    continue;
                }

                // so this is leaving scope unit, thus should not be separated
                return false;
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->has<ShouldBeSeparated>()) {
                    unit->get<ShouldBeSeparated>() = need->get<ShouldBeSeparated>();
                }
            }
            return true;
        }
    };

    class ExcludeIncompleteSeparatedPath : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<ShouldBeSeparated>()) {
                return false;
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->type() == Unit::CHANNEL
                        && user->father()->type() == Unit::EXTERNAL_EXECUTOR) {
                    continue;
                }

                if (user->task() == unit->task() && !user->has<ShouldBeSeparated>()) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldBeSeparated>();
            return true;
        }
    };

    class UpdateShouldSeparated : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<ShouldBeSeparated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* should_separated = unit->get<ShouldBeSeparated>();
            if (!should_separated->has<ShouldBeSeparated>()) {
                unit->clear<ShouldBeSeparated>();
            }
            return true;
        }
    };
};

bool AddCommonExecutorPass::PartialAnalysis::Run(Plan* plan) {
    Impl::InitTags(plan);
    Impl::AnalyzeShouldUnderPartialExecutor(plan);
    Impl::AnalyzeShouldBeSeparated(plan);
    return false;
}

class AddCommonExecutorPass::AddPartialExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher move_phase(DepthFirstDispatcher::POST_ORDER);
        move_phase.AddRule(new MoveToPartialExecutor);
        if (move_phase.Run(plan)) {
            // plan is changed, run again
            return true;
        }

        TopologicalDispatcher execute_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        execute_phase.AddRule(new ExecuteTopLevelStreamingLeafs);

        JobConfig& job_conf = plan->Root()->get<JobConfig>();
        if (job_conf.is_null()
            || !job_conf->has_must_keep_empty_group()
            || !job_conf->must_keep_empty_group()) {
            execute_phase.AddRule(new PromoteNestedStreamingLeafs);
        }

        execute_phase.AddRule(new DelegatePassedByInput);
        if (execute_phase.Run(plan)) {
            return true;
        }

        TopologicalDispatcher separate_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        separate_phase.AddRule(new MoveUnitToSeparatedScope);
        return separate_phase.Run(plan);
    }

    typedef TaskSingleton<runtime::PartialExecutor> PartialExecutor;

    struct StreamingExecuted {};

    class MoveToPartialExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->father() == unit->task() && unit->has<ShouldUnderPartialExecutor>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = NULL;
            if (unit->task()->has<PartialExecutor>()) {
                executor = unit->task()->get<PartialExecutor>();
            } else {
                executor = unit->task()->clone();
                executor->change_identity();
                executor->set_type(Unit::PARTIAL_EXECUTOR);
                plan->AddControl(unit->task(), executor);
                unit->task()->get<PartialExecutor>().Assign(executor);
            }

            plan->ReplaceControl(unit->task(), unit, executor);
            return true;
        }
    };

    class ExecuteTopLevelStreamingLeafs : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf() || unit->has<ExecutedByFather>()) {
                return false;
            }

            if (unit->father()->type() != Unit::PARTIAL_EXECUTOR
                    || !unit->has<IsUnderPartialExecutor>()
                    || !unit->has<IsStreamingUnit>()) {
                // is not top level streaming partial units
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task() || !need->has<IsUnderPartialExecutor>()) {
                    // there is no requirement for outside froms
                    continue;
                }

                if (!need->has<StreamingExecuted>()) {
                    // inside need must also be executed directely by PartialExecutor
                    return false;
                }
            }
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<ExecutedByFather>();
            unit->set<StreamingExecuted>();
            return true;
        }
    };

    class PromoteNestedStreamingLeafs : public RuleDispatcher::Rule {
        struct RealFrom : public Pointer<Unit> {};

        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf()
                    || unit->has<ExecutedByFather>()
                    || !unit->has<IsUnderPartialExecutor>()
                    ||  !unit->has<IsStreamingUnit>()) {
                return false;
            }

            if (unit->father()->type() != Unit::SCOPE) {
                // not under shuffle scope
                return false;
            }
            Unit* scope = unit->father();

            if (unit->type() == Unit::PROCESS_NODE &&
                    !unit->get<PbLogicalPlanNode>().process_node().is_ignore_group()) {
                // exclude group relavent processers
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task() || !need->has<IsUnderPartialExecutor>()) {
                    // there is no requirement for outside froms
                    continue;
                }

                if (need->father() != scope && need->is_descendant_of(scope)) {
                    // leave scope will discard key, which is not supported by
                    // PartialExecutor
                    return false;
                }

                if (!need->has<StreamingExecuted>() && !need->has<RealFrom>()) {
                    return false;
                }
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* clone = unit->clone();
            clone->get<PbLogicalPlanNode>().set_id(clone->change_identity());
            clone->get<PbScope>() = unit->father()->get<PbScope>();
            clone->set<ExecutedByFather>();
            clone->set<StreamingExecuted>();
            plan->AddControl(unit->task()->get<PartialExecutor>(), clone);

            unit->set_type(Unit::CHANNEL);
            *unit->get<OriginIdentity>() = unit->identity();
            unit->get<RealFrom>().Assign(clone);

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->has<RealFrom>()) {
                    Unit* real_from = need->get<RealFrom>();
                    plan->AddDependency(real_from, clone);
                    plan->ReplaceFrom(clone, need->identity(), real_from->identity());
                } else {
                    plan->AddDependency(need, clone);
                }
                plan->RemoveDependency(need, unit);
            }
            plan->AddDependency(clone, unit);

            return true;
        }
    };

    class DelegatePassedByInput : public RuleDispatcher::Rule {
        struct Delegates : std::map<Unit*, Unit*> {};  // one union for each user

        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf()
                    || !unit->has<IsUnderPartialExecutor>()
                    || unit->father()->type() == Unit::PARTIAL_EXECUTOR) {
                // not nested inside PartialExecutor
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() == unit->task() && need->has<IsUnderPartialExecutor>()) {
                    // ignore inside from
                    continue;
                }
                return true;
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* partial_executor = unit->task()->get<PartialExecutor>();
            Delegates& delegates = partial_executor->get<Delegates>();
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() == unit->task() && need->has<IsUnderPartialExecutor>()) {
                    continue;
                }

                Unit* &delegate = delegates[need];
                if (delegate == NULL) {
                    delegate = need->clone();
                    delegate->set<ExecutedByFather>();
                    delegate->set<StreamingExecuted>();
                    delegate->set_type(Unit::UNION_NODE);

                    PbLogicalPlanNode* message = &delegate->get<PbLogicalPlanNode>();
                    message->set_id(delegate->change_identity());
                    message->set_type(PbLogicalPlanNode::UNION_NODE);
                    message->mutable_union_node()->clear_from();
                    message->mutable_union_node()->add_from(need->identity());

                    plan->AddControl(partial_executor, delegate);
                    plan->AddDependency(need, delegate);
                }

                plan->RemoveDependency(need, unit);
                plan->AddDependency(delegate, unit);
                switch (unit->type()) {
                    case Unit::LOAD_NODE:
                    case Unit::SINK_NODE:
                    case Unit::PROCESS_NODE:
                    case Unit::UNION_NODE:
                    case Unit::SHUFFLE_NODE:
                        plan->ReplaceFrom(unit, need->identity(), delegate->identity());
                    default:
                        break;
                }
            }

            return true;
        }
    };

    // tags at separation root, origin scope -> separated scope
    struct SeparatedScopes : std::map<Unit*, Unit*> {};

    class MoveUnitToSeparatedScope : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<ShouldBeSeparated>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* root = unit->get<ShouldBeSeparated>();
            Unit* scope = unit->father();

            Unit* &separated_scope = root->get<SeparatedScopes>()[scope];
            if (separated_scope == NULL) {
                // it's safe for sharing same identity between executors
                separated_scope = scope->clone();

                if (scope->father()->type() == Unit::PARTIAL_EXECUTOR) {
                    plan->AddControl(scope->father(), separated_scope);
                } else {
                    BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                        // now need's father is outter separated scope cloned before
                        plan->AddControl(need->father(), separated_scope);
                    }
                }
            }

            plan->RemoveControl(scope, unit);
            plan->AddControl(separated_scope, unit);

            unit->clear<ShouldBeSeparated>();
            unit->set<IsSeparated>();
            DrawPlanPass::UpdateLabel(unit, "06-is-separated", "IsSeparated");

            return true;
        }
    };
};

bool AddCommonExecutorPass::AddPartialExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class AddCommonExecutorPass::AddShuffleExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
        dispatcher.AddRule(new AssignShuffleExecutor);
        return dispatcher.Run(plan);
    }

    class AssignShuffleExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SCOPE) {
                return false;
            }

            CHECK(unit->has<PbScope>());
            switch (unit->get<PbScope>().type()) {
                case PbScope::GROUP:
                case PbScope::BUCKET:
                case PbScope::WINDOW:
                    break;
                default:
                    return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (!child->is_leaf()) {
                    continue;
                }

                if (child->type() != Unit::SHUFFLE_NODE && child->type() != Unit::CHANNEL) {
                    // SHUFFLE_EXECUTOR does not recognize other leafs
                    return false;
                }
            }
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set_type(Unit::SHUFFLE_EXECUTOR);
            if (unit->has<IsInfinite>()) {
                unit->set_type(Unit::STREAM_SHUFFLE_EXECUTOR);
            }
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::SHUFFLE_NODE || child->type() == Unit::CHANNEL) {
                    child->set<ExecutedByFather>();
                    if (unit->has<IsLocalDistribute>()
                            && !unit->get<PbScope>().distribute_every()) {
                        // local distribute shuffle do not obey priority rule
                        child->set<DisablePriority>();
                    }
                }
            }
            return true;
        }
    };
};

bool AddCommonExecutorPass::AddShuffleExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class AddCommonExecutorPass::AddLogicalExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new AddLogicalExecutor);
        return dispatcher.Run(plan);
    }

    class AddLogicalExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf() || unit->has<ExecutedByFather>()) {
                return false;
            }

            switch (unit->type()) {
                case Unit::LOAD_NODE:
                case Unit::SINK_NODE:
                case Unit::UNION_NODE:
                case Unit::PROCESS_NODE:
                    break;
                default:
                    return false;
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = plan->NewUnit(unit->identity(), /*is_leaf =*/ false);
            executor->set_type(Unit::LOGICAL_EXECUTOR);
            if (unit->has<IsInfinite>()) {
                executor->set_type(Unit::STREAM_LOGICAL_EXECUTOR);
            }
            plan->ReplaceControl(unit->father(), unit, executor);

            unit->set<DisablePriority>();
            unit->set<ExecutedByFather>();
            return true;
        }
    };
};

bool AddCommonExecutorPass::AddLogicalExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class AddCommonExecutorPass::FixChannelUnitPass::Impl {
public:
    static bool Run(Plan* plan) {
        bool is_changed = false;

        RuleDispatcher add_union_phase;
        add_union_phase.AddRule(new AddUnionForSameTaskChannel);
        is_changed |= add_union_phase.Run(plan);

        DepthFirstDispatcher add_channel_phase(DepthFirstDispatcher::PRE_ORDER);
        add_channel_phase.AddRule(new AddMissingScopeChannel);
        is_changed |= add_channel_phase.Run(plan);

        return is_changed;
    }

    class AddUnionForSameTaskChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL) {
                return false;
            }
            if (unit->father()->type() != Unit::TASK && unit->father()->type() != Unit::SCOPE) {
                // channel is already under specific executor
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->is_descendant_of(unit->father())) {
                    // the need of this channel is coming from same executor, which is
                    // caused by in-place promote
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* union_ = unit->clone();
            union_->set_type(Unit::UNION_NODE);
            plan->AddControl(unit->father(), union_);

            PbLogicalPlanNode* message = &union_->get<PbLogicalPlanNode>();
            message->set_type(PbLogicalPlanNode::UNION_NODE);
            message->clear_union_node();
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->is_descendant_of(unit->father())) {
                    plan->RemoveDependency(need, unit);
                    plan->AddDependency(need, union_);
                    message->mutable_union_node()->add_from(need->identity());
                }
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                plan->RemoveDependency(unit, user);
                plan->AddDependency(union_, user);
            }

            plan->AddDependency(unit, union_);
            message->mutable_union_node()->add_from(unit->change_identity());

            return true;
        }
    };

    class AddMissingScopeChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL
                    || unit->father()->type() != Unit::SCOPE) {
                return false;
            }
            Unit* scope = unit->father();

            Unit* father  = scope->father();
            if (father == NULL || father->type() != Unit::SCOPE) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->father() == father) {
                    return false;
                }
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* scope = unit->father();
            Unit* father = scope->father();

            Unit* clone = unit->clone();
            plan->AddControl(scope, clone);
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                plan->RemoveDependency(unit, user);
                plan->AddDependency(clone, user);
            }

            unit->change_identity();
            plan->AddDependency(unit, clone);
            plan->RemoveControl(scope, unit);
            plan->AddControl(father, unit);

            return true;
        }
    };
};

bool AddCommonExecutorPass::FixChannelUnitPass::Run(Plan* plan) {
    return Impl::Run(plan);
};

class AddCommonExecutorPass::AddCacheWriterExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher dfs_dispatcher(DepthFirstDispatcher::POST_ORDER);
        dfs_dispatcher.AddRule(new AddCacheWriterExecutor);
        bool is_changed = dfs_dispatcher.Run(plan);

        RuleDispatcher clean_phase;
        clean_phase.AddRule(new ClearCacheTags);
        clean_phase.Run(plan);

        return is_changed;
    }

    class AddCacheWriterExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            // Channel Unit meanning the unit has been copied and promoted.
            // We build CacheWriterExecutor for the promoted units and skip
            // the channels here.
            return unit->type() != Unit::CHANNEL && unit->has<ShouldCache>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit *cache_unit = plan->NewUnit(/*is_leaf=*/ false);
            cache_unit->set_type(Unit::CACHE_WRITER);
            plan->AddControl(unit->father(), cache_unit);

            Unit *cache_source = plan->NewUnit(/*is_leaf=*/ true);
            cache_source->set_type(Unit::CHANNEL);
            plan->AddDependency(unit, cache_source);
            plan->AddControl(cache_unit, cache_source);

            return true;
        }
    };

    class ClearCacheTags : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<ShouldCache>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<ShouldCache>();
            DrawPlanPass::RemoveLabel(unit, "07-should-cache");

            return false;
        }
    };
};

bool AddCommonExecutorPass::AddCacheWriterExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

