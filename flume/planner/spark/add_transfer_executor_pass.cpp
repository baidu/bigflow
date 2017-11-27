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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>
//         Wang Cong <bigflow-opensource@baidu.com>

#include "flume/planner/spark/add_transfer_executor_pass.h"

#include "boost/foreach.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/common/util.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/unit.h"

#include "flume/runtime/io/io_format.h"
#include "flume/util/reflection.h"

namespace baidu {
namespace flume {

namespace runtime {
namespace spark {
class ShuffleInputExecutor;
class ShuffleOutputExecutor;
}  // namspace spark
}  // namespace runtime

namespace planner {
namespace spark {

class AddTransferExecutorPass::AddHadoopInputPass::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
        dispatcher.AddRule(new ChangeInputScope);
        dispatcher.AddRule(new ExecuteHadoopInput);
        return dispatcher.Run(plan);
    }

    class ChangeInputScope : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SCOPE && unit->get<PbScope>().type() == PbScope::INPUT;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set_type(Unit::EXTERNAL_EXECUTOR);
            if (unit->has<CacheNodeInfo>()) {
                unit->get<ExternalExecutor>() = CACHE_INPUT;
            } else {
                unit->get<ExternalExecutor>() = HADOOP_INPUT;
            }
            DrawPlanPass::UpdateLabel(unit, "02-external-type", "HADOOP_INPUT");
            return true;
        }
    };

    class ExecuteHadoopInput : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            using runtime::TextInputFormat;
            using runtime::SequenceFileAsBinaryInputFormat;

            if (unit->type() != Unit::LOAD_NODE) {
                return false;
            }
            const PbLoadNode& node = unit->get<PbLogicalPlanNode>().load_node();

            std::string classname = node.loader().name();
            if (classname != PrettyTypeName<TextInputFormat>()
                    && classname != PrettyTypeName<SequenceFileAsBinaryInputFormat>()) {
                return false;
            }

            if (unit->father()->type() == Unit::LOGICAL_EXECUTOR) {
                // LOAD_NODE is already executed, move it out
                return true;
            }

            return !unit->has<ExecutedByFather>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            if (unit->father()->type() == Unit::LOGICAL_EXECUTOR) {
                Unit* loader_executor = unit->father();
                Unit* hadoop_input_executor = unit->father()->father();
                CHECK_EQ(hadoop_input_executor->type(), Unit::EXTERNAL_EXECUTOR);

                plan->RemoveControl(loader_executor, unit);
                plan->AddControl(hadoop_input_executor, unit);
                plan->RemoveUnit(loader_executor);
            }

            unit->set<ExecutedByFather>();
            return false;
        }
    };
};

bool AddTransferExecutorPass::AddHadoopInputPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class AddTransferExecutorPass::AddShuffleInputPass::Impl {
public:
    static bool Run(Plan* plan) {
        // newly added unit will not be touched in the same traversal. so an extra apply
        // is needed.
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new BridgeForLogicalNode);
        dispatcher.AddRule(new BridgeForNestedChannel);
        dispatcher.AddRule(new CollectTopLevelChannel);
        return dispatcher.Run(plan);
    }

    typedef TaskSingleton<runtime::spark::ShuffleInputExecutor> ShuffleInputExecutor;

    class BridgeForLogicalNode : public RuleDispatcher::Rule {
    public:
        // one channel for eche origin
        struct ShuffleInputs : public std::map<std::string, Unit*> {};

        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf() || unit->type() == Unit::CHANNEL) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* task = unit->task();
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() == task) {
                    continue;
                }

                Unit* &channel = task->get<ShuffleInputs>()[need->identity()];
                if (channel == NULL) {
                    channel = need->clone();
                    channel->set_type(Unit::CHANNEL);
                    channel->clear<IsPartial>();
                    if (channel->has<OriginIdentity>()) {
                        channel->set_identity(*channel->get<OriginIdentity>());
                    }

                    // equip with leave scope
                    channel->clear<KeyScopes>();
                    channel->clear<OrderScopes>();
                    if (need->has<KeyScopes>()) {
                        const KeyScopes& keys = need->get<KeyScopes>();
                        for (size_t i = 1; i < keys.size(); ++i) {
                            if (keys[i].is_sorted()) {
                                channel->get<OrderScopes>().push_back(keys[i]);
                            } else {
                                break;
                            }
                        }
                    }

                    // add channel at task first, and will be moved into
                    // ShuffleInputExecutor at next run
                    plan->AddControl(task, channel);
                }

                plan->AddDependency(need, channel);
                plan->AddDependency(channel, unit);
                plan->RemoveDependency(need, unit);

                if (unit->has<PbLogicalPlanNode>()) {
                    const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
                    if (message.type() == PbLogicalPlanNode::SHUFFLE_NODE) {
                        const PbShuffleNode& node = message.shuffle_node();
                        if (node.type() == PbShuffleNode::BROADCAST) {
                            channel->set<IsBroadcast>();
                        }
                    }
                }
            }
            return true;
        }
    };

    class BridgeForNestedChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL || unit->father() == unit->task()) {
                return false;
            }

            Unit* task = unit->task();
            if (task->has<ShuffleInputExecutor>()
                    && task->get<ShuffleInputExecutor>() == unit->father()) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != task) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* clone = unit->clone();
            clone->change_identity();
            clone->clear<IsPartial>();
            plan->AddControl(unit->task(), clone);

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    plan->RemoveDependency(need, unit);
                    plan->AddDependency(need, clone);
                }
            }
            plan->AddDependency(clone, unit);

            if (unit->has<PbLogicalPlanNode>()) {
                const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
                if (message.type() == PbLogicalPlanNode::SHUFFLE_NODE) {
                    const PbShuffleNode& node = message.shuffle_node();
                    if (node.type() == PbShuffleNode::BROADCAST) {
                        clone->set<IsBroadcast>();
                    }
                }
            }

            return true;
        }
    };

    class CollectTopLevelChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL || unit->father() != unit->task()) {
                return false;
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = NULL;
            if (unit->task()->has<ShuffleInputExecutor>()) {
                executor = unit->task()->get<ShuffleInputExecutor>();
            } else {
                executor = plan->NewUnit(/*is_leaf =*/ false);
                executor->set_type(Unit::EXTERNAL_EXECUTOR);
                executor->get<ExternalExecutor>() = SHUFFLE_INPUT;
                plan->AddControl(unit->task(), executor);

                unit->task()->get<ShuffleInputExecutor>().Assign(executor);
                DrawPlanPass::UpdateLabel(executor, "02-external-type", "SHUFFLE_INPUT");
            }

            plan->RemoveControl(unit->task(), unit);
            plan->AddControl(executor, unit);
            unit->set<ExecutedByFather>();

            return true;
        }
    };
};

bool AddTransferExecutorPass::AddShuffleInputPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class AddTransferExecutorPass::AddShuffleOutputPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new AddShuffleOutputChannel);
        bool is_changed = dispatcher.Run(plan);

        RuleDispatcher dispatcher_move;
        dispatcher_move.AddRule(new MoveShuffleOutputRule);
        is_changed |= dispatcher_move.Run(plan);

        return is_changed;
    }

    typedef TaskSingleton<runtime::spark::ShuffleOutputExecutor> ShuffleOutputExecutor;

    // one channel in each target task
    struct ShuffleOutputs : public std::map<Unit*, Unit*> {};

    class AddShuffleOutputChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf()) {
                return false;
            }

            if (unit->type() == Unit::CHANNEL
                    && unit->task()->has<ShuffleOutputExecutor>()
                    && unit->task()->get<ShuffleOutputExecutor>() == unit->father()) {
                return false;
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->task() != unit->task()) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = NULL;
            if (unit->task()->has<ShuffleOutputExecutor>()) {
                executor = unit->task()->get<ShuffleOutputExecutor>();
            } else {
                executor = plan->NewUnit(/*is_leaf =*/ false);
                executor->set_type(Unit::EXTERNAL_EXECUTOR);
                executor->get<ExternalExecutor>() = SHUFFLE_OUTPUT;
                plan->AddControl(unit->task(), executor);

                DrawPlanPass::UpdateLabel(executor, "02-external-type", "SHUFFLE_OUTPUT");
                unit->task()->get<ShuffleOutputExecutor>().Assign(executor);
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->task() == unit->task()) {
                    continue;
                }

                Unit* &channel = unit->get<ShuffleOutputs>()[user->task()];
                if (channel == NULL) {
                    channel = unit->clone();  // this change will be saved in channels
                    channel->change_identity();
                    channel->set_type(Unit::CHANNEL);
                    channel->set<ExecutedByFather>();
                    if (!channel->has<OriginIdentity>()) {
                        *channel->get<OriginIdentity>() = unit->identity();
                    }
                    plan->AddControl(executor, channel);
                    plan->AddDependency(unit, channel);
                }
               plan->RemoveDependency(unit, user);
               plan->AddDependency(channel, user);
               if (user->type() == Unit::CHANNEL) {
                   if (user->has<IsBroadcast>()) {
                       channel->set<IsBroadcast>();
                   }
               }
           }

           return true;
       }
   };


   class MoveShuffleOutputRule : public RuleDispatcher::Rule {
   public:
       virtual bool Accept(Plan* plan, Unit* unit) {
           Unit* father = unit->father();
           return unit->type() == Unit::CHANNEL
                   && father->type() == Unit::EXTERNAL_EXECUTOR
                   && father->get<ExternalExecutor>() == SHUFFLE_OUTPUT;
       }
        virtual bool Run(Plan* plan, Unit* unit) {
            CHECK_EQ(unit->direct_needs().size(), 1);
            Unit* need = unit->direct_needs()[0];
            Unit* source = FindDirectPartialOutput(need);
            if (source == need) {
                return false;
            }

            // source is an dispatcher of PartialExecutor, where record can be sent to
            // other tasks directly withtout buffering at PartialExector
            source->set<DisablePriority>();
            plan->RemoveDependency(need, unit);
            plan->AddDependency(source, unit);

            return true;
        }

        Unit* FindDirectPartialOutput(Unit* unit) {
            Unit* current = unit;
            Unit* task = unit->task();
            while (current->type() == Unit::CHANNEL && current->task() == task) {
                if (current->direct_needs().size() != 1) {
                    return current;
                }
                current = current->direct_needs()[0];

                if (current->direct_users().size() > 1) {
                    return current;
                }

                if (current->father()->type() == Unit::PARTIAL_EXECUTOR) {
                    return current;
                }
            }
            return unit;
        }
    };
};

bool AddTransferExecutorPass::AddShuffleOutputPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
