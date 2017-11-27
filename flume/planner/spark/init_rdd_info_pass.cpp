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
// Author: Wang Cong<wangcong09@baidu.com>
//         Pan Yuchang(BDG)<panyuchang@baidu.com>
//         Wen Xiang<wenxiang@baidu.com>
// Description:

#include "flume/planner/spark/init_rdd_info_pass.h"

#include <algorithm>
#include <sstream>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"

#include "flume/flags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/tags.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class InitRddInfoPass::AddIndexForTaskPass::Impl {
public:
    static bool Run(Plan* plan) {
        TaskDispatcher clear_dispatcher;
        clear_dispatcher.AddRule(new ClearTaskIndexRule());
        clear_dispatcher.Run(plan);

        DepthFirstDispatcher first_task_dispatcher(DepthFirstDispatcher::PRE_ORDER);
        first_task_dispatcher.AddRule(new FirstTaskIndexBuilderRule());
        first_task_dispatcher.Run(plan);

        TaskDispatcher task_index_dispatcher;
        task_index_dispatcher.AddRule(new TaskIndexBuilderRule());
        task_index_dispatcher.Run(plan);

        // TaskDispatcher update_task_dispatcher;
        // update_task_dispatcher.AddRule(new UpdateParentTaskRule());
        // update_task_dispatcher.Run(plan);

        return true;
    }

    class ClearTaskIndexRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK && unit->has<TaskIndex>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<TaskIndex>();
            return false;
        }
    };

    /**
     * Assign index 0 to a mapper
     */
    class FirstTaskIndexBuilderRule : public RuleDispatcher::Rule {
    public:
        FirstTaskIndexBuilderRule() : m_has_found(false) {}

        virtual bool Accept(Plan* plan, Unit* unit) {
            // task 0 must belongs to a MapTask
            return !m_has_found && unit->type() == Unit::LOAD_NODE;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* task = unit->task();
            *(task->get<TaskIndex>()) = 0;
            std::string label = "task: " +
                boost::lexical_cast<std::string>(0);
            DrawPlanPass::UpdateLabel(task, "10-task", label);
            m_has_found = true;
            return false;
        }

    private:
        bool m_has_found;
    };

    class TaskIndexBuilderRule : public RuleDispatcher::Rule {
    public:
        TaskIndexBuilderRule() : m_index(1) {}

        virtual bool Accept(Plan* plan, Unit* unit) {
            // MapTask's index equals 0 no need calculate again
            return unit->type() == Unit::TASK && !unit->has<TaskIndex>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            std::string label = "task: " +
                boost::lexical_cast<std::string>(m_index);
            DrawPlanPass::UpdateLabel(unit, "10-task", label);
            *(unit->get<TaskIndex>()) = m_index++;
            return false;
        }

    private:
        int m_index;
    };

};

bool InitRddInfoPass::AddIndexForTaskPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

namespace {

PbSparkJob& PbSparkJobOfRoot(Unit* root) { // NOLINT
    return root->get<PbSparkJob>();
}

} // namespace

class InitRddInfoPass::InitRddPass::Impl {
public:
    static bool Run(Plan* plan) {
        plan->Root()->clear<PbSparkJob>();

        TaskDispatcher dispatcher1(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        dispatcher1.AddRule(new FreshPreTaskInSameRddRule);
        dispatcher1.Run(plan);

        TaskDispatcher dispatcher2(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        dispatcher2.AddRule(new UnmergeHadoopInputRddRule);
        dispatcher2.Run(plan);

        plan->Root()->get<Debug>(); // set

        return false;
    }

    // make tasks in same rdd id in chain
    // suppose t2, t3 are marked with same rdd id
    // pre task of t2 is t2, pre task of t3 is t2
    class FreshPreTaskInSameRddRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->get<PreTaskInSameRdd>() == unit;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbSparkJob& job = PbSparkJobOfRoot(plan->Root());
            int unit_rdd_index = *unit->get<RddIndex>();
            for (int rdd_index = job.rdd_size(); rdd_index <= unit_rdd_index; ++rdd_index) {
                PbSparkRDD* rdd = job.add_rdd();
                rdd->set_rdd_index(rdd_index);
            }
            return false;
        }
    };

    // we do not merge mappers into same rdd, we keep them apart
    // so keep one mapper with rdd id 0, change other mappers
    class UnmergeHadoopInputRddRule : public RuleDispatcher::Rule {
    public:
        UnmergeHadoopInputRddRule() : m_is_first_zero(true) {}

        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK
                    && unit->has<RddIndex>()
                    && *unit->get<RddIndex>() == 0;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            if (m_is_first_zero) {
                m_is_first_zero = false;
            } else {
                PbSparkJob& spark_job = PbSparkJobOfRoot(plan->Root());
                int new_value = spark_job.rdd_size();
                *unit->get<RddIndex>() = new_value;

                std::string label = "rdd: " +
                    boost::lexical_cast<std::string>(new_value);
                DrawPlanPass::UpdateLabel(unit, "10-rdd", label);

                PbSparkRDD *rdd = spark_job.add_rdd();
                rdd->set_rdd_index(new_value);
            }
            unit->get<PreTaskInSameRdd>().Assign(unit);
            return false;
        }

    private:
        bool m_is_first_zero;
    };
};

bool InitRddInfoPass::InitRddPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class InitRddInfoPass::UpdateRddInfoPass::Impl {
public:
    static bool Run(Plan* plan) {
        TaskDispatcher dispatcher1(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        dispatcher1.AddRule(new UpdateNextRddRule());
        dispatcher1.AddRule(new UpdateNextRddRuleForCache());
        dispatcher1.Run(plan);

        TaskDispatcher dispatcher2(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        dispatcher2.AddRule(new UpdateTaskPartitionOffsetRule());
        dispatcher2.Run(plan);

        TaskDispatcher dispatcher3(TaskDispatcher::PRE_TOPOLOGY_ORDER);
        dispatcher3.AddRule(new UpdateGeneralRddConcurrencyRule());
        dispatcher3.Run(plan);

        DepthFirstDispatcher dispatcher4(DepthFirstDispatcher::POST_ORDER);
        dispatcher4.AddRule(new CollectTaskMemoryUsageRule());
        dispatcher4.Run(plan);

        DepthFirstDispatcher dispatcher5(DepthFirstDispatcher::POST_ORDER);
        dispatcher5.AddRule(new UpdateRddMemoryUsageRule());
        dispatcher5.Run(plan);

        return false;
    }

    class UpdateNextRddRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            TaskFlow& info = unit->get<TaskFlow>();
            PbSparkJob& spark_job = PbSparkJobOfRoot(plan->Root());

            int unit_rdd_index = *unit->get<RddIndex>();
            PbSparkRDD* rdd_info = spark_job.mutable_rdd(unit_rdd_index);

            std::set<int>& parent_rdd = _parent_rdd_map[unit_rdd_index];

            BOOST_FOREACH(Unit* user_task, info.input_tasks) {
                int user_task_rdd_index = *user_task->get<RddIndex>();
                PbSparkRDD* user_task_rdd_info = spark_job.mutable_rdd(user_task_rdd_index);
                int user_task_rdd_info_index = user_task_rdd_info->rdd_index();

                if (parent_rdd.find(user_task_rdd_info_index) != parent_rdd.end()) {
                    continue;
                }

                rdd_info->add_parent_rdd_index(user_task_rdd_info_index);
                parent_rdd.insert(user_task_rdd_info_index);
            }
            return false;
        }

    private:
        std::map<int, std::set<int> > _parent_rdd_map;
    };

    class UpdateNextRddRuleForCache : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK
                   && unit->get<PbSparkTask>().type() == PbSparkTask::CACHE;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbSparkJob& job = PbSparkJobOfRoot(plan->Root());
            PbSparkRDD* rdd = job.mutable_rdd(*unit->get<RddIndex>());
            CHECK_EQ(rdd->rdd_index(), *unit->get<RddIndex>());
            for (auto origin: unit->get<CacheOrigin>()) {
                CHECK_NE(*origin->task()->get<RddIndex>(), *unit->get<RddIndex>());
                rdd->add_parent_rdd_index(*origin->task()->get<RddIndex>());
            }
            return false;
        }

    };

    class UpdateTaskPartitionOffsetRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            *unit->get<TaskOffset>() = CalculateConcurrencyOffset(unit);

            // move it to build physical plan pass
            PbSparkTask* task = &unit->get<PbSparkTask>();
            task->set_partition_offset(*unit->get<TaskOffset>());
            std::string label("partition");
            label += boost::lexical_cast<std::string>(task->partition_offset());
            label += "-";
            label += boost::lexical_cast<std::string>(task->partition_offset()
                                                      + task->concurrency());
            DrawPlanPass::UpdateLabel(unit, "11-partition-range", label);
            return false;
        }

    private:
        int CalculateConcurrencyOffset(Unit* unit) {
            if (!unit || unit->type() != Unit::TASK)
                return 0;
            Unit* pre_task = unit->get<PreTaskInSameRdd>();
            const PbSparkTask& pre_task_pb = pre_task->get<PbSparkTask>();
            int pre_concurrency = pre_task_pb.concurrency();
            if (pre_task == unit) {
                return 0;
            } else {
                return CalculateConcurrencyOffset(pre_task) + pre_concurrency;
            }
        }
    };

    class UpdateGeneralRddConcurrencyRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbSparkTask& task = unit->get<PbSparkTask>();
            int unit_rdd_index = *unit->get<RddIndex>();
            task.set_rdd_index(unit_rdd_index);

            PbSparkJob& spark_job = PbSparkJobOfRoot(plan->Root());
            PbSparkRDD* rdd_info = spark_job.mutable_rdd(unit_rdd_index);
            CHECK_EQ(unit_rdd_index, rdd_info->rdd_index());
            if (task.has_concurrency()) {
                rdd_info->set_concurrency(rdd_info->concurrency() + task.concurrency());
            }
            return false;
        }
    };

    class FixGeneralRddConcurrencyRule : public RuleDispatcher::Rule {
    public:
        struct Info {
            Info() : partitions(0) {}
            int partitions;
        };

    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::TASK) {
                return false;
            }
            std::set<int> concurrencies;
            PbSparkJob& spark_job = PbSparkJobOfRoot(plan->Root());
            PbSparkRDD* rdd_info = spark_job.mutable_rdd(*unit->get<RddIndex>());
            for (int i = 0; i < rdd_info->parent_rdd_index_size(); ++i) {
                concurrencies.insert(
                    spark_job.rdd(
                        rdd_info->parent_rdd_index(i)
                        ).concurrency());
            }
            if (concurrencies.size() > 0) {
                unit->get<Info>().partitions = *concurrencies.rbegin();
            }
            return concurrencies.size() > 1;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbSparkJob& spark_job = PbSparkJobOfRoot(plan->Root());
            PbSparkRDD* rdd_info = spark_job.mutable_rdd(*unit->get<RddIndex>());
            for (int i = 0; i < rdd_info->parent_rdd_index_size(); ++i) {
                spark_job.mutable_rdd(rdd_info->parent_rdd_index(i))
                    ->set_concurrency(unit->get<Info>().partitions);
            }
            return true;
        }
    };

    class CollectTaskMemoryUsageRule : public RuleDispatcher::Rule {
    public:
        struct MemoryLimit {
            MemoryLimit() : limit(0) {}
            int limit;
        };

    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }
        virtual bool Run(Plan* plan, Unit* unit) {
            int memory_limit = -1;
            if (unit->has<PbLogicalPlanNode>()) {
                const PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
                if (node.has_memory_limit()) {
                    memory_limit = std::max(node.memory_limit(), memory_limit);
                }
            }
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->has<MemoryLimit>()) {
                    memory_limit = std::max(child->get<MemoryLimit>().limit, memory_limit);
                }
            }
            if (memory_limit != -1) {
                unit->get<MemoryLimit>().limit = memory_limit;
            }
            return false;
        }
    };

    class UpdateRddMemoryUsageRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::TASK
                && unit->has<CollectTaskMemoryUsageRule::MemoryLimit>();
        }
        virtual bool Run(Plan* plan, Unit* unit) {
            int unit_rdd_index = *unit->get<RddIndex>();
            PbSparkJob& spark_job = PbSparkJobOfRoot(plan->Root());
            PbSparkRDD* rdd_info = spark_job.mutable_rdd(unit_rdd_index);

            int memory_limit = unit->get<CollectTaskMemoryUsageRule::MemoryLimit>().limit;
            rdd_info->set_memory_limit(std::max(rdd_info->memory_limit(), memory_limit));
            return false;
        }
    };
};

bool InitRddInfoPass::UpdateRddInfoPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

