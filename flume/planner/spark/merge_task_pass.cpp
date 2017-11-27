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
//         Zhang Yuncong <bigflow-opensource@baidu.com>
//         Wang Cong <bigflow-opensource@baidu.com>
//

#include "flume/planner/spark/merge_task_pass.h"

#include <set>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "toft/base/unordered_set.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

namespace {

static const Unit::Type kTypes[] = {Unit::PROCESS_NODE, Unit::UNION_NODE, Unit::SINK_NODE};
static const std::set<Unit::Type> kMergeTypeSet(kTypes,
                                                kTypes + sizeof(kTypes) / sizeof(kTypes[0]));
struct PlusConcurrency {};

std::set<Unit*> GetTasksOfNeed(Unit* task) {
    CHECK_EQ(task->type(), Unit::TASK);
    const DataFlow& data_flow = task->get<DataFlow>();
    std::set<Unit*> tasks_of_need;
    BOOST_FOREACH(DataFlow::Dependency dependency, data_flow.inputs) {
        Unit* up_task = dependency.first->task();
        tasks_of_need.insert(up_task);
    }
    return tasks_of_need;
}

std::set<Unit*> GetTasksOfUser(Unit* task) {
    CHECK_EQ(task->type(), Unit::TASK);
    const DataFlow& data_flow = task->get<DataFlow>();
    std::set<Unit*> tasks_of_user;
    BOOST_FOREACH(DataFlow::Dependency dependency, data_flow.outputs) {
        Unit* down_task = dependency.second->task();
        tasks_of_user.insert(down_task);
    }
    return tasks_of_user;
}

void AssignTargetTask(Unit* target_task, Unit* task) {
    task->get<MergeTargetTask>().Assign(target_task);
    target_task->get<MergeTargetTask>().Assign(target_task);
    BOOST_FOREACH(Unit* child, target_task->children()) {
        child->get<MergeTargetTask>().Assign(target_task);
    }
    BOOST_FOREACH(Unit* child, task->children()) {
        child->get<MergeTargetTask>().Assign(target_task);
    }
    DrawPlanPass::AddMessage(target_task, "MergeTarget");
    DrawPlanPass::AddMessage(task, "NeedMerged");
}

bool IsNeed(Unit* origin, Unit* target) {
    BOOST_FOREACH(Unit* need, origin->direct_needs()) {
        if (need == target) {
            return true;
        }
    }
    return false;
}

bool IsUser(Unit* origin, Unit* target) {
    BOOST_FOREACH(Unit* user, origin->direct_users()) {
        if (user == target) {
            return true;
        }
    }
    return false;
}

bool IsNeedOrUser(Unit* origin, Unit* target) {
   return IsNeed(origin, target) || IsUser(origin, target);
}

} // namespace

class MergeTaskPass::ClearTagPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher clear_stage;
        clear_stage.AddRule(new ClearTaskTagRule);
        clear_stage.Run(plan);

        return false;
    }

private:
    class ClearTaskTagRule : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        bool Run(Plan* plan, Unit* unit) {
            unit->clear<MergeTargetTask>();
            unit->clear<PlusConcurrency>();
            return false;
        }
    };
};

bool MergeTaskPass::ClearTagPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class MergeTaskPass::MergeConcurrencyOneTaskAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        TaskDispatcher clear_stage(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        clear_stage.AddRule(new ClearMergeTag);
        clear_stage.Run(plan);

        TaskDispatcher init_stage(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        init_stage.AddRule(new FindTaskCanMergeRule);
        init_stage.Run(plan);

        TaskDispatcher reverse_topo(TaskDispatcher::POST_TOPOLOGY_ORDER);
        reverse_topo.AddRule(new FindMergeTargetTaskRule());
        bool changed = reverse_topo.Run(plan);

        return changed;
    }

private:
    struct MergeTag {};

    class ClearMergeTag : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        bool Run(Plan* plan, Unit* unit) {
            unit->clear<MergeTag>();
            return false;
        }
    };

    class FindTaskCanMergeRule : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::TASK) {
                return false;
            }
            // TODO(panyuchang)
            // if Mapper task's concurrency is set as 1,
            // on some conditions there will be errors.
            // i.e.  mapper 1   mapper 1
            //          \        /
            //           \      /
            //           reducer 1
            // this must not be merged, but now, we can't handle it.
            if (unit->has<TaskConcurrency>()) {
                return *unit->get<TaskConcurrency>() == 1;
            }
            bool can_merge = true;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (kMergeTypeSet.count(child->type()) == 0) {
                    can_merge = false;
                    break;
                }
            }
            return can_merge;
        }

        bool Run(Plan* plan, Unit* unit) {
            unit->get<MergeTag>();
            return true;
        }
    };

    class FindMergeTargetTaskRule : public RuleDispatcher::Rule {
    public:
        struct TasksCanMerge : std::set<Unit*> {};

        FindMergeTargetTaskRule() : m_is_first_time(true) {}

        bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::TASK || unit->empty() || !unit->has<MergeTag>()) {
                return false;
            }

            std::set<Unit*> tasks_upstream = GetTasksOfNeed(unit);
            TasksCanMerge& tasks_can_merge = unit->get<TasksCanMerge>();
            tasks_can_merge.clear();
            std::set<Unit*> intersection;
            std::set<Unit*> tasks_downstream;
            BOOST_FOREACH(Unit* task, tasks_upstream) {
                if (!task->has<MergeTag>()
                        || *task->get<RddIndex>() < *unit->get<RddIndex>() - 1) {
                    continue;
                }
                tasks_downstream = GetTasksOfUser(task);
                intersection.clear();
                std::set_intersection(tasks_upstream.begin(), tasks_upstream.end(),
                                      tasks_downstream.begin(), tasks_downstream.end(),
                                      std::insert_iterator< std::set<Unit*> >(intersection,
                                                                        intersection.begin())
                                    );
                bool is_ok = true;
                BOOST_FOREACH(Unit* int_task, intersection) {
                    if (!int_task->has<MergeTag>()) {
                        is_ok = false;
                        break;
                    }
                }
                if (is_ok) {
                    tasks_can_merge.insert(task);
                }
            }
            return tasks_can_merge.size() != 0 && m_is_first_time;
        }

        bool Run(Plan* plan, Unit* unit) {
            const TasksCanMerge& tasks_can_merge = unit->get<TasksCanMerge>();
            m_is_first_time = false;
            Unit* target_task = *tasks_can_merge.begin();
            BOOST_FOREACH(Unit* mg_task, tasks_can_merge) {
                if (*mg_task->get<RddIndex>() < *target_task->get<RddIndex>()) {
                    target_task = mg_task;
                }
            }
            BOOST_FOREACH(Unit* mg_task, tasks_can_merge) {
                AssignTargetTask(target_task, mg_task);
            }
            AssignTargetTask(target_task, unit);
            return true;
        }

    private:
        bool m_is_first_time;
    };
};

bool MergeTaskPass::MergeConcurrencyOneTaskAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}

class MergeTaskPass::MergeDownstreamTaskAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher init_stage(DepthFirstDispatcher::PRE_ORDER);
        init_stage.AddRule(new ClearDistributeTagRule);
        init_stage.AddRule(new FindTopLevelBucketScopeRule);
        init_stage.Run(plan);

        bool find_first_target = true;
        DepthFirstDispatcher find_phase(DepthFirstDispatcher::PRE_ORDER, find_first_target);
        find_phase.AddRule(new FindDistributeCanMergeRule);
        bool changed = find_phase.Run(plan);

        return changed;
    }

private:
    struct DefaultDistribute {};

    class ClearDistributeTagRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<DefaultDistribute>();
            return false;
        }
    };

    class FindTopLevelBucketScopeRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->father() == unit->task()
                    && unit->type() == Unit::SCOPE
                    && unit->get<PbScope>().type() == PbScope::BUCKET
                    && !unit->has<HasPartitioner>()
                    && !unit->empty();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            // set DefaultDistribute first, clear if not suitable in next rule
            unit->set<DefaultDistribute>();
            return false;
        }
    };

    class FindDistributeCanMergeRule : public RuleDispatcher::Rule {
    public:
        bool IsBroadcastShuffleNode(Unit* unit) {
            const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
            if (message.type() == PbLogicalPlanNode::SHUFFLE_NODE) {
                const PbShuffleNode& node = message.shuffle_node();
                if (node.type() == PbShuffleNode::BROADCAST) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->has<DefaultDistribute>()) {
                return false;
            }

            DataFlow& dataflow = unit->get<DataFlow>();
            CHECK(!dataflow.inputs.empty());

            // all inputs need come from the same task.
            Unit* up_task = dataflow.inputs[0].first->task();
            for (size_t i = 1; i < dataflow.inputs.size(); ++i) {
                if (dataflow.inputs[i].first->task() != up_task) {
                    return false;
                }
            }

            // all children can not be broadcast.
            // for (iterator it = m_childs.begin(); it < m_childs.end(); it++) {
            BOOST_FOREACH(Unit* child, unit->children()) {
                //Unit* child = *it;
                if (IsBroadcastShuffleNode(child)) {
                    return false;
                }
            }

            Unit* task = unit->task();
            if (task == up_task) {
                return false;
            }

            if (up_task->has<IsHadoopInput>()) {
                bool is_all_default =
                        !task->has<TaskConcurrency>() && !up_task->has<TaskConcurrency>();
                bool has_same_concurrency = task->has<TaskConcurrency>()
                        && up_task->has<TaskConcurrency>()
                        && *task->get<TaskConcurrency>() == *up_task->get<TaskConcurrency>();
                return is_all_default || has_same_concurrency;
            } else if (up_task->has<TaskConcurrency>()
                    && *up_task->get<TaskConcurrency>() == 1) {
                // if up_task is 'local'(explicit 1 concurrency)
                // merge if this task 'local' or task has default concurrency
                return !(task->has<TaskConcurrency>() && *task->get<TaskConcurrency>() != 1);
            } else {
                // for common case, default concurrency can be overrided by other
                // concurrency.
                return !task->has<TaskConcurrency>()
                        || !up_task->has<TaskConcurrency>()
                        || *task->get<TaskConcurrency>() == *up_task->get<TaskConcurrency>();
            }
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* task = unit->task();
            Unit* target_task = unit->get<DataFlow>().inputs[0].first->task();

            unit->get<MergeTargetTask>().Assign(target_task);
            task->get<MergeTargetTask>().Assign(target_task);
            DrawPlanPass::AddMessage(target_task, "MergeTarget");
            DrawPlanPass::AddMessage(task, "NeedMerged");
            return true;  // Do not modify this.
        }
    };
};

bool MergeTaskPass::MergeDownstreamTaskAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}

class MergeTaskPass::MergeBrotherTaskAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        TaskDispatcher find_stage(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        find_stage.AddRule(new FindTaskCanMergeRule);
        bool changed = find_stage.Run(plan);

        return changed;
    }

private:
    class FindTaskCanMergeRule : public RuleDispatcher::Rule {
    private:
        struct Info {
        public:
            int bucket_count;
            int group_count;
            int bucket_count_con;
            int group_count_con;
            std::set<std::string> bucket_ids;

            int concurrency;

            Info() : bucket_count(0), group_count(0),
                    bucket_count_con(0), group_count_con(0),
                    concurrency(0) {}
        };

        static bool CanBeMergedJudgeByInfo(const Info& info_a, const Info& info_b) {
            Info res;
            res.bucket_count = info_a.bucket_count + info_b.bucket_count;
            res.bucket_count_con = info_a.bucket_count_con + info_b.bucket_count_con;
            res.group_count = info_a.group_count + info_b.group_count;
            res.group_count_con = info_a.group_count_con + info_b.group_count_con;
            res.bucket_ids.insert(info_a.bucket_ids.begin(), info_a.bucket_ids.end());
            res.bucket_ids.insert(info_b.bucket_ids.begin(), info_b.bucket_ids.end());

            // concurrency situation:
            //  0 + 0
            //  0 + 1 (including 1 + 0, so do the following lines)
            //  0 + n
            //  1 + 1
            //  1 + n
            //  n + n
            // if we set n = 3, then the sum will be
            // 0, 1, 3, 2, 4, 6
            // we found that when the sum is 1 and 4, it can not be merged
            // so we use sum % 3 != 1 to judge if they can be merged.
            res.concurrency = info_a.concurrency > 1 ? 3 : info_a.concurrency;
            res.concurrency += info_b.concurrency > 1 ? 3 : info_b.concurrency;

            return (res.bucket_count == 0 || res.bucket_count_con == 0
                    || res.bucket_ids.size() + res.group_count_con <= 1)
                    && res.concurrency % 3 != 1;
        }

        static bool NeedPlusConcurrency(const Info& info_a, const Info& info_b) {
            return info_a.bucket_count_con == 0 && info_b.bucket_count_con == 0;
        }

        static Info GetInfoFromTask(Unit* task) {
            CHECK_EQ(task->type(), Unit::TASK);
            Info info;
            if (task->has<TaskConcurrency>()) {
                info.concurrency = *task->get<TaskConcurrency>();
            }
            BOOST_FOREACH(Unit* child, task->children()) {
                if (!child->has<PbScope>()) {
                    continue;
                }
                const PbScope& scope = child->get<PbScope>();
                if (scope.type() == PbScope::BUCKET) {
                    ++info.bucket_count;
                    if (scope.has_concurrency()) {
                        info.bucket_ids.insert(child->identity());
                        ++info.bucket_count_con;
                    }
                } else if (scope.type() == PbScope::GROUP) {
                    ++info.group_count;
                    if (scope.has_concurrency()) {
                        ++info.group_count_con;
                    }
                }
            }
            return info;
        }

    public:
        FindTaskCanMergeRule() : m_is_first_time(true) {}

        bool Accept(Plan* plan, Unit* unit) {
            return m_is_first_time
                    && unit->type() == Unit::TASK
                    // unmerge mappers
                    && *unit->get<RddIndex>() > 0
                    && unit->get<PreTaskInSameRdd>() != unit;
        }

        bool Run(Plan* plan, Unit* unit) {
            Unit* pre_task_in_same_vertex = unit->get<PreTaskInSameRdd>();

            Info task_info = GetInfoFromTask(unit);
            Info pre_task_info = GetInfoFromTask(pre_task_in_same_vertex);

            if (CanBeMergedJudgeByInfo(pre_task_info, task_info)) {
                AssignTargetTask(pre_task_in_same_vertex, unit);
                if (NeedPlusConcurrency(pre_task_info, task_info)) {
                    // if two tasks' concurrency value both 1
                    // then after merge, do not plus concurrency, keep 1
                    if (!(pre_task_in_same_vertex->has<TaskConcurrency>()
                            && *pre_task_in_same_vertex->get<TaskConcurrency>() == 1
                            && unit->has<TaskConcurrency>()
                            && *unit->get<TaskConcurrency>() == 1)) {
                        unit->get<PlusConcurrency>();
                        DrawPlanPass::AddMessage(unit, "NeedPlusConcurrency");
                    }
                }
                m_is_first_time = false;
                return true;
            }
            return false;
        }

    private:
        bool m_is_first_time;
    };
};

bool MergeTaskPass::MergeBrotherTaskAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}
class MergeTaskPass::MergeTaskActionPass::Impl {
public:
    static bool Run(Plan* plan) {
        TaskDispatcher merge_task_stage;
        merge_task_stage.AddRule(new MergeTaskRule());
        bool changed = merge_task_stage.Run(plan);

        if (!changed) {
            // if no task was merged, do not need to run following rules
            return false;
        }

        TaskDispatcher merge_concurrency_stage;
        merge_concurrency_stage.AddRule(new MergeConcurrencyRule());
        changed |= merge_concurrency_stage.Run(plan);

        DepthFirstDispatcher merge_scope_stage(DepthFirstDispatcher::PRE_ORDER);
        merge_scope_stage.AddRule(new MergeScopeWithSameIdentity);
        changed |= merge_scope_stage.Run(plan);

        TopologicalDispatcher merge_leaf_by_identity(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        merge_leaf_by_identity.AddRule(new MergeBrotherLeafNodeWithSameIdentity);
        changed |= merge_leaf_by_identity.Run(plan);

        TopologicalDispatcher merge_leaf_by_origin(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        merge_leaf_by_origin.AddRule(new MergeWithOriginUnitRule);
        changed |= merge_leaf_by_origin.Run(plan);

        return changed;
    }

private:
    class MergeTaskRule : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK && unit->has<MergeTargetTask>();
        }

        bool Run(Plan* plan, Unit* unit) {
            bool is_changed = false;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (!child->has<MergeTargetTask>()) {
                    continue;
                }
                Unit* target_task = child->get<MergeTargetTask>();
                if (unit == target_task) {
                    continue;
                }
                plan->RemoveControl(unit, child);
                plan->AddControl(target_task, child);
                is_changed = true;
            }
            return is_changed;
        }
    };

    class MergeConcurrencyRule : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK && unit->has<MergeTargetTask>();
        }

        bool Run(Plan* plan, Unit* unit) {
            CHECK(unit->has<MergeTargetTask>());
            Unit* target_task = unit->get<MergeTargetTask>();
            if (target_task == unit) {
                return false;
            }
            int concurrency = 0;
            if (unit->has<PlusConcurrency>()) {
                concurrency = target_task->has<TaskConcurrency>() ?
                        *target_task->get<TaskConcurrency>() : 0;
                concurrency +=  unit->has<TaskConcurrency>() ?
                        *unit->get<TaskConcurrency>() : 0;
            } else {
                if (!target_task->has<TaskConcurrency>() && unit->has<TaskConcurrency>()) {
                    concurrency = *unit->get<TaskConcurrency>();
                }
            }
            if (concurrency > 0) {
                *target_task->get<TaskConcurrency>() = concurrency;
                std::string label = "reduce number: "
                        + boost::lexical_cast<std::string>(concurrency);
                DrawPlanPass::UpdateLabel(target_task, "10-reduce-number", label);
            }
            return false;
        }
    };

    // just merge scope with same id in the same scope
    class MergeScopeWithSameIdentity : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_discard() || unit->is_leaf() || unit->type() < Unit::TASK) {
                return false;
            }
            Unit* father = unit->father();
            m_target_scope = NULL;
            BOOST_FOREACH(Unit* child, father->children()) {
                if (child != unit && unit->identity() == child->identity()) {
                    m_target_scope = child;
                    return true;
                }
            }
            return false;
        }

        bool Run(Plan* plan, Unit* unit) {
            CHECK_NOTNULL(m_target_scope);
            if (m_target_scope->has<IsLocalDistribute>()) {
                unit->set<IsLocalDistribute>();
            }

            BOOST_FOREACH(Unit* child, m_target_scope->children()) {
                plan->RemoveControl(m_target_scope, child);
                plan->AddControl(unit, child);
            }
            plan->RemoveUnit(m_target_scope);
            return true;
        }

    private:
        Unit* m_target_scope;
    };

    // just merge leafs in the same scope which are not users of other or needs of other
    class MergeBrotherLeafNodeWithSameIdentity : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_discard() || !unit->is_leaf()) {
                return false;
            }
            Unit* father = unit->father();
            m_target_node = NULL;
            BOOST_FOREACH(Unit* child, father->children()) {
                if (!child->is_leaf() || unit->type() == Unit::CHANNEL
                        || child == unit || unit->identity() != child->identity()) {
                    continue;
                }
                if (IsNeedOrUser(unit, child)) {
                    continue;
                }
                m_target_node = child;
                return true;
            }
            return false;
        }

        bool Run(Plan* plan, Unit* unit) {
            CHECK_NOTNULL(m_target_node);
            if (m_target_node->has<IsLocalDistribute>()) {
                unit->set<IsLocalDistribute>();
            }
            BOOST_FOREACH(Unit* need, m_target_node->direct_needs()) {
                plan->AddDependency(need, unit);
            }
            BOOST_FOREACH(Unit* user, m_target_node->direct_users()) {
                plan->AddDependency(unit, user);
            }
            plan->RemoveUnit(m_target_node);
            return true;
        }

    private:
        Unit* m_target_node;
    };

    // just merge leaf nodes when one of them has origin in the same scope and
    // the origin do not has other needs
    class MergeWithOriginUnitRule : public RuleDispatcher::Rule {
    public:
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_discard() || !unit->has<Origin>() || unit->get<Origin>()->is_discard()) {
                return false;
            }
            Unit* origin = unit->get<Origin>();
            Unit* origin_father = origin->father();
            if (origin_father != unit->father()
                    || !IsUser(unit, origin)) {
                return false;
            }
            CHECK_EQ(Unit::CHANNEL, origin->type());
            CHECK_NE(Unit::CHANNEL, unit->type());
            return true;
        }

        bool Run(Plan* plan, Unit* unit) {
            Unit* origin = unit->get<Origin>();
            std::string old_identity = origin->identity();
            std::string new_identity = unit->identity();
            if (origin->direct_needs().size() > 1) {
                // brother node with same id must be merged before,
                // if origin has more than 1 need, that means all needs must not
                // be in the same scope
                if (origin->identity() == unit->identity()) {
                    origin->change_identity();
                    new_identity = origin->identity();
                    BOOST_FOREACH(Unit* user, origin->direct_users()) {
                        if (user->type() != Unit::CHANNEL) {
                            plan->ReplaceFrom(user, old_identity, new_identity);
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            }

            BOOST_FOREACH(Unit* user, origin->direct_users()) {
                if (user->type() != Unit::CHANNEL) {
                    plan->ReplaceFrom(user, old_identity, new_identity);
                }
                plan->AddDependency(unit, user);
            }
            plan->RemoveUnit(origin);
            return true;
        }
    };
};

bool MergeTaskPass::MergeTaskActionPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
