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

#include "flume/planner/common/build_common_executor_pass.h"

#include <limits>

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
#include "flume/proto/transfer_encoding.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class BuildCommonExecutorPass::BuildStreamShuffleExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher build_phase;
        build_phase.AddRule(new BuildStreamShuffleExecutorMessage);
        build_phase.AddRule(new SetStreamShuffleType);
        return build_phase.Run(plan);
    }

    class BuildStreamShuffleExecutorMessage : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::STREAM_SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            typedef std::multimap<uint32_t, Unit*> DispatcherMap;  // priority -> dispatcher
            const uint32_t kMaxPriority = std::numeric_limits<uint32_t>::max();

            PbExecutor* message = &unit->get<PbExecutor>();
            message->set_type(PbExecutor::STREAM_SHUFFLE);

            PbStreamShuffleExecutor* sub_message = message->mutable_stream_shuffle_executor();
            sub_message->Clear();
            *sub_message->mutable_scope() = unit->get<PbScope>();

            DispatcherMap dispatchers;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::SHUFFLE_NODE) {
                    dispatchers.insert(std::make_pair(kMaxPriority, child));
                }
            }

            BOOST_FOREACH(DispatcherMap::value_type pair, dispatchers) {
                uint32_t priority = pair.first;
                Unit* dispatcher = pair.second;

                if (dispatcher->type() == Unit::SHUFFLE_NODE) {
                    *sub_message->add_node() = dispatcher->get<PbLogicalPlanNode>();
                }
            }

            if (unit->father()->type() == Unit::TASK) {
                sub_message->set_need_cache(false);
            } else {
                sub_message->set_need_cache(true);
            }

            return false;
        }
    };

    class SetStreamShuffleType : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::STREAM_SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const PbScope& scope = unit->get<PbScope>();
            PbStreamShuffleExecutor* message =
                    unit->get<PbExecutor>().mutable_stream_shuffle_executor();

            if (scope.type() == PbScope::WINDOW) {
                message->set_type(PbStreamShuffleExecutor::WINDOW);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "WINDOW");
                return false;
            }

            if (unit->has<IsDistributeAsBatch>()) {
                message->set_type(PbStreamShuffleExecutor::DISTRIBUTE_AS_BATCH);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "DISTRIBUTE_AS_BATCH");
                return false;
            }

            if (unit->has<IsLocalDistribute>()) {
                message->set_type(PbStreamShuffleExecutor::LOCAL_DISTRIBUTE);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "LOCAL_DISTRIBUTE");
                return false;
            }

            if (message->node_size() != 0) {
                message->set_type(PbStreamShuffleExecutor::LOCAL);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "LOCAL");
                return false;
            }

            return false;
        }
    };
};

bool BuildCommonExecutorPass::BuildStreamShuffleExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildCommonExecutorPass::BuildStreamLogicalExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new UpdateMessageFor<Unit::LOAD_NODE>());
        dispatcher.AddRule(new UpdateMessageFor<Unit::SINK_NODE>());
        dispatcher.AddRule(new UpdateMessageFor<Unit::UNION_NODE>());
        dispatcher.AddRule(new UpdateMessageForProcessNode());
        return dispatcher.Run(plan);
    }

    template<Unit::Type TYPE>
    class UpdateMessageFor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == TYPE && unit->father()->type() == Unit::STREAM_LOGICAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor* message = &unit->father()->get<PbExecutor>();
            message->set_type(PbExecutor::STREAM_LOGICAL);
            *message->mutable_stream_logical_executor()->mutable_node() =
                    unit->get<PbLogicalPlanNode>();

            return false;
        }
    };

    class UpdateMessageForProcessNode : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::PROCESS_NODE &&
                    unit->father()->type() == Unit::STREAM_LOGICAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->father();

            PbExecutor* message = &executor->get<PbExecutor>();
            message->set_type(PbExecutor::STREAM_PROCESSOR);

            const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
            PbStreamProcessorExecutor* sub_message = message->mutable_stream_processor_executor();

            sub_message->set_identity(unit->identity());
            sub_message->set_need_status(node.is_stateful());
            sub_message->set_is_ignore_group(node.is_ignore_group());
            *sub_message->mutable_processor() = node.processor();

            std::map<std::string, Unit*> froms;
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                froms[need->identity()] = need;
            }

            sub_message->clear_source();
            for (int i = 0; i < node.input_size(); ++i) {
                const PbProcessNode::Input& input = node.input(i);

                PbStreamProcessorExecutor::Source* source = sub_message->add_source();
                source->set_identity(input.from());
                Unit* need = froms[input.from()];
                source->set_is_batch(!need->has<IsInfinite>());
            }

            return false;
        }
    };
};

bool BuildCommonExecutorPass::BuildStreamLogicalExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildCommonExecutorPass::BuildShuffleExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher init_phase(DepthFirstDispatcher::POST_ORDER);
        init_phase.AddRule(new AnalyzeHasMergeSource);
        init_phase.AddRule(new AnalyzeIsPartialShuffle);
        init_phase.AddRule(new AnalyzePassByChannel);
        init_phase.Run(plan);

        RuleDispatcher build_phase;
        build_phase.AddRule(new BuildShuffleExecutorMessage);
        build_phase.AddRule(new SetShuffleType);
        build_phase.Run(plan);

        return false;
    }

    struct HasMergeSource {};
    struct IsPartialShuffle {};
    struct IsPassbyChannel {};

    class AnalyzeHasMergeSource : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::CHANNEL) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<HasMergeSource>();
            return false;
        }
    };

    class AnalyzeIsPartialShuffle : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<IsPartialShuffle>();
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->is_leaf() && !child->has<IsPartial>()) {
                    unit->clear<IsPartialShuffle>();
                }
            }

            return false;
        }
    };

    class AnalyzePassByChannel : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CHANNEL
                    && unit->father()->type() == Unit::SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<IsPassbyChannel>();
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->type() == Unit::CHANNEL
                        && user->father()->type() == Unit::SHUFFLE_EXECUTOR) {
                    unit->set<IsPassbyChannel>();
                }
            }

            return false;
        }
    };

    class BuildShuffleExecutorMessage : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            typedef std::multimap<uint32_t, Unit*> DispatcherMap;  // priority -> dispatcher
            const uint32_t kMaxPriority = std::numeric_limits<uint32_t>::max();

            PbExecutor* message = &unit->get<PbExecutor>();
            message->set_type(PbExecutor::SHUFFLE);

            PbShuffleExecutor* sub_message = message->mutable_shuffle_executor();
            sub_message->Clear();
            *sub_message->mutable_scope() = unit->get<PbScope>();

            DispatcherMap dispatchers;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::SHUFFLE_NODE || child->type() == Unit::CHANNEL) {
                    if (child->has<PreparePriority>()) {
                        dispatchers.insert(std::make_pair(*child->get<PreparePriority>(), child));
                    } else {
                        dispatchers.insert(std::make_pair(kMaxPriority, child));
                    }
                }
            }

            BOOST_FOREACH(DispatcherMap::value_type pair, dispatchers) {
                uint32_t priority = pair.first;
                Unit* dispatcher = pair.second;

                if (dispatcher->type() == Unit::SHUFFLE_NODE) {
                    *sub_message->add_node() = dispatcher->get<PbLogicalPlanNode>();
                }

                if (dispatcher->type() == Unit::CHANNEL) {
                    PbShuffleExecutor::MergeSource* source = sub_message->add_source();
                    source->set_input(dispatcher->direct_needs().front()->identity());
                    source->set_output(dispatcher->identity());
                    if (priority != kMaxPriority) {
                        source->set_priority(priority);
                    }
                }
            }

            return false;
        }
    };

    class SetShuffleType : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const PbScope& scope = unit->get<PbScope>();
            PbShuffleExecutor* message = unit->get<PbExecutor>().mutable_shuffle_executor();

            if (scope.distribute_every()) {
                // Distribute every record to a group
                message->set_type(PbShuffleExecutor::BY_RECORD);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "BY_RECORD");
                return false;
            }
            if (unit->has<IsLocalDistribute>()) {
                message->set_type(PbShuffleExecutor::LOCAL_DISTRIBUTE);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "LOCAL_DISTRIBUTE");
                return false;
            }

            if (message->node_size() == 0 && message->source_size() == 0) {
                // runtime::shuffle::MergeRunner has lowest overhead for empty shuffle
                message->set_type(PbShuffleExecutor::MERGE);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "MERGE");
                return false;
            }

            if (message->node_size() != 0 && message->source_size() != 0) {
                // has both merge and shuffle input, use CombineRunner
                message->set_type(PbShuffleExecutor::COMBINE);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "COMBINE");
                return false;
            }

            if (message->node_size() != 0 && message->source_size() == 0) {
                message->set_type(PbShuffleExecutor::LOCAL);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "LOCAL");
                return false;
            }

            // now for message->node_size() == 0 && message->source_size() != 0
            if (scope.type() == PbScope::BUCKET && !unit->has<IsPartialShuffle>()) {
                // for non-partial bucket shuffle, empty partitions should also be processed,
                // CombineRunner can handle this situation
                message->set_type(PbShuffleExecutor::COMBINE);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "COMBINE");
            } else {
                message->set_type(PbShuffleExecutor::MERGE);
                DrawPlanPass::UpdateLabel(unit, "05-shuffle-type", "MERGE");
            }

            return false;
        }
    };
};

bool BuildCommonExecutorPass::BuildShuffleExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildCommonExecutorPass::BuildLogicalExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new UpdateMessageFor<Unit::LOAD_NODE>());
        dispatcher.AddRule(new UpdateMessageFor<Unit::SINK_NODE>());
        dispatcher.AddRule(new UpdateMessageFor<Unit::UNION_NODE>());
        dispatcher.AddRule(new UpdateMessageForProcessNode());
        return dispatcher.Run(plan);
    }

    template<Unit::Type TYPE>
    class UpdateMessageFor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == TYPE && unit->father()->type() == Unit::LOGICAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor* message = &unit->father()->get<PbExecutor>();
            message->set_type(PbExecutor::LOGICAL);
            *message->mutable_logical_executor()->mutable_node() = unit->get<PbLogicalPlanNode>();

            return false;
        }
    };

    class UpdateMessageForProcessNode : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::PROCESS_NODE &&
                    unit->father()->type() == Unit::LOGICAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->father();

            PbExecutor* message = &executor->get<PbExecutor>();
            message->set_type(PbExecutor::PROCESSOR);
            PbProcessorExecutor* sub_message = message->mutable_processor_executor();
            sub_message->set_identity(unit->identity());
            *sub_message->mutable_processor() =
                    unit->get<PbLogicalPlanNode>().process_node().processor();

            std::map<std::string, Unit*> froms;
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                froms[need->identity()] = need;
            }

            sub_message->clear_source();
            const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
            for (int i = 0; i < node.input_size(); ++i) {
                const PbProcessNode::Input& input = node.input(i);

                PbProcessorExecutor::Source* source = sub_message->add_source();
                source->set_identity(input.from());

                if (froms[input.from()] == NULL) {
                    source->set_type(PbProcessorExecutor::DUMMY);
                } else {
                    Unit* need = froms[input.from()];
                    if (executor->has<NonPreparedNeeds>()
                            && executor->get<NonPreparedNeeds>().count(need) > 0) {
                        source->set_type(PbProcessorExecutor::REQUIRE_STREAM);
                    } else {
                        source->set_type(PbProcessorExecutor::REQUIRE_ITERATOR);
                    }
                }

                source->set_is_prepared(input.is_prepared());
            }

            if (unit->has<PartialKeyNumber>()) {
                sub_message->set_partial_key_number(*unit->get<PartialKeyNumber>());
            }

            return false;
        }
    };
};

bool BuildCommonExecutorPass::BuildLogicalExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildCommonExecutorPass::BuildPartialExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher init_phase(DepthFirstDispatcher::PRE_ORDER);
        init_phase.AddRule(new CleanTags);
        init_phase.AddRule(new InitShuffleLevelForPartialExecutor);
        init_phase.AddRule(new InitShuffleLevelForShuffleExecutor);
        init_phase.AddRule(new InitShuffleLevelForOtherExecutor);
        init_phase.Run(plan);

        TopologicalDispatcher analyze_priority_phase(TopologicalDispatcher::REVERSE_ORDER);
        analyze_priority_phase.AddRule(new AnalyzePriority);
        analyze_priority_phase.Run(plan);

        TopologicalDispatcher search_level_phase(TopologicalDispatcher::REVERSE_ORDER);
        search_level_phase.AddRule(new CollectPartialShuffleLevels);
        search_level_phase.Run(plan);

        TopologicalDispatcher search_output_phase(TopologicalDispatcher::TOPOLOGICAL_ORDER);
        search_output_phase.AddRule(new FindPartialOutputs);
        search_output_phase.AddRule(new FindShuffleChannels);
        search_output_phase.Run(plan);

        DepthFirstDispatcher update_output_phase(DepthFirstDispatcher::POST_ORDER);
        update_output_phase.AddRule(new UpdateInvolvedOutputs);
        update_output_phase.Run(plan);

        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
        dispatcher.AddRule(new InitializeMessage);
        dispatcher.AddRule(new AddScope);
        dispatcher.AddRule(new AddLogicalPlanNode);
        dispatcher.AddRule(new AddOutput);
        return dispatcher.Run(plan);
    }

    struct PartialShuffleLevel : public Pointer<Unit> {};
    struct Priorities : public std::vector<uint32_t> {};
    struct ScopeIdentites : public std::set<std::string> {};

    // to decide if use hash sort
    struct InvolvedOutputs : std::set<Unit*> {};
    struct InvolvedShuffleLevels : std::set<Unit*> {};

    class CleanTags : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<PartialShuffleLevel>();
            unit->clear<Priorities>();
            unit->clear<ScopeIdentites>();

            unit->clear<InvolvedOutputs>();
            unit->clear<InvolvedShuffleLevels>();

            return false;
        }
    };

    class InitShuffleLevelForPartialExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::PARTIAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<PartialShuffleLevel>().Assign(unit);
            return false;
        }
    };

    class InitShuffleLevelForShuffleExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_EXECUTOR
                    || !unit->father()->has<PartialShuffleLevel>()) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::CHANNEL) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<PartialShuffleLevel>().Assign(unit);
            return false;
        }
    };

    class InitShuffleLevelForOtherExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf()
                    && !unit->has<PartialShuffleLevel>()
                    && unit->father() != NULL
                    && unit->father()->has<PartialShuffleLevel>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<PartialShuffleLevel>() = unit->father()->get<PartialShuffleLevel>();
            return false;
        }
    };

    class FindPartialOutputs : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf() && unit->father()->type() == Unit::PARTIAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<InvolvedOutputs>().insert(unit);
            return false;
        }
    };

    class FindShuffleChannels : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::CHANNEL
                    || unit->father()->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            CHECK_EQ(unit->direct_needs().size(), 1);
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                return need->has<InvolvedOutputs>();
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                unit->get<InvolvedOutputs>().insert(need->get<InvolvedOutputs>().begin(),
                                                    need->get<InvolvedOutputs>().end());
            }

            return false;
        }
    };

    class UpdateInvolvedOutputs : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->has<InvolvedOutputs>()) {
                    return true;
                }
            }
            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->has<InvolvedOutputs>()) {
                    unit->get<InvolvedOutputs>().insert(child->get<InvolvedOutputs>().begin(),
                                                        child->get<InvolvedOutputs>().end());
                }
            }

            return false;
        }
    };

    class CollectPartialShuffleLevels : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf() || !unit->father()->has<PartialShuffleLevel>()) {
                // not a leaf under PartialExecutor
                return false;
            }

            if (unit->father()->type() == Unit::EXTERNAL_EXECUTOR) {
                // task output executor does not care record order
                return false;
            }

            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            bool is_passby_channel = false;

            InvolvedShuffleLevels& levels = unit->get<InvolvedShuffleLevels>();
            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->father()->type() == Unit::PARTIAL_EXECUTOR) {
                    continue;
                }

                if (user->has<InvolvedShuffleLevels>()) {
                    levels.insert(user->get<InvolvedShuffleLevels>().begin(),
                                  user->get<InvolvedShuffleLevels>().end());
                }

                if (user->type() == Unit::CHANNEL &&
                        user->father()->type() == Unit::SHUFFLE_EXECUTOR) {
                    is_passby_channel = true;
                }
            }

            if (!is_passby_channel && unit->father()->type() != Unit::PARTIAL_EXECUTOR) {
                levels.insert(unit->father()->get<PartialShuffleLevel>());
            }

            return false;
        }
    };

    class AnalyzePriority : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf()
                    || !unit->father()->has<PartialShuffleLevel>()
                    || !unit->has<PreparePriority>()) {
                return false;
            }

            if (unit->type() == Unit::CHANNEL
                    && unit->father()->type() == Unit::SHUFFLE_EXECUTOR) {
                return true;
            }

            if (unit->father()->type() == Unit::PARTIAL_EXECUTOR) {
                return true;
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Priorities& priorities = unit->get<Priorities>();
            priorities.push_back(*unit->get<PreparePriority>());

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->type() == Unit::CHANNEL
                        && user->father()->type() == Unit::SHUFFLE_EXECUTOR
                        && user->has<Priorities>()) {
                    boost::copy(user->get<Priorities>(), std::back_inserter(priorities));
                    break;
                }
            }
            return false;
        }
    };

    class InitializeMessage : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::PARTIAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor* message = &unit->get<PbExecutor>();
            message->set_type(PbExecutor::PARTIAL);

            PbPartialExecutor* sub_message = message->mutable_partial_executor();
            sub_message->clear_scope();
            sub_message->clear_scope_level();
            sub_message->clear_node();
            sub_message->clear_output();

            return false;
        }
    };

    class AddScope : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf()) {
                return false;
            }

            return unit->father()->type() == Unit::PARTIAL_EXECUTOR
                    && unit->type() == Unit::SHUFFLE_NODE;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->father();

            const PbScope& scope = unit->get<PbScope>();
            if (executor->get<ScopeIdentites>().count(scope.id()) > 0) {
                return false;
            }
            executor->get<ScopeIdentites>().insert(scope.id());

            PbPartialExecutor* sub_message =
                    executor->get<PbExecutor>().mutable_partial_executor();
            sub_message->add_scope()->CopyFrom(scope);

            return false;
        }
    };

    class AddLogicalPlanNode : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf() && unit->father()->type() == Unit::PARTIAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->father();
            PbPartialExecutor* sub_message =
                    executor->get<PbExecutor>().mutable_partial_executor();
            sub_message->add_node()->CopyFrom(unit->get<PbLogicalPlanNode>());

            CHECK(unit->has<ScopeLevel>())
                    << "Unit ID: " << unit->identity() << ", Type:" << unit->type_string();
            sub_message->add_scope_level(unit->get<ScopeLevel>().level);
            return false;
        }
    };

    class AddOutput : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf() || unit->father()->type() != Unit::PARTIAL_EXECUTOR) {
                return false;
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->father() != unit->father()) {
                    return true;
                }
            }

            return false;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->father();
            PbPartialExecutor* sub_message =
                    executor->get<PbExecutor>().mutable_partial_executor();

            PbPartialExecutor::Output* output = sub_message->add_output();
            output->set_identity(unit->identity());
            output->mutable_objector()->CopyFrom(unit->get<PbLogicalPlanNode>().objector());

            BOOST_FOREACH(uint32_t priority, unit->get<Priorities>()) {
                output->add_priority(priority);
            }

            InvolvedShuffleLevels& levels = unit->get<InvolvedShuffleLevels>();
            output->set_need_hash(levels.size() == 1);
            output->set_need_buffer(false);

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->father()->type() == Unit::PARTIAL_EXECUTOR) {
                    continue;
                }

                if (user->father()->type() != Unit::EXTERNAL_EXECUTOR) {
                    output->set_need_buffer(true);
                }

                if (user->father()->get<InvolvedOutputs>().size() != 1) {
                    output->set_need_hash(false);
                }
            }

            if (output->need_hash()) {
                DrawPlanPass::UpdateLabel(unit, "30-partial-output", "NEED_HASH");
            }

            if (output->need_buffer()) {
                DrawPlanPass::UpdateLabel(unit, "31-partial-output", "NEED_BUFFER");
            }

            return false;
        }
    };
};

bool BuildCommonExecutorPass::BuildPartialExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

class BuildCommonExecutorPass::BuildCacheWriterExecutorPass::Impl {
public:
    static bool Run(Plan* plan) {
        if (!plan->Root()->has<JobConfig>()
                || plan->Root()->get<JobConfig>() == NULL) {
            return false;
        }

        RuleDispatcher dispatcher;
        dispatcher.AddRule(new BuildCacheWriterExecutor());
        return dispatcher.Run(plan);
    }

    class BuildCacheWriterExecutor : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::CACHE_WRITER;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor& executor = unit->get<PbExecutor>();
            executor.set_type(PbExecutor::WRITE_CACHE);

            CHECK_EQ(1u, unit->children().size());
            Unit* cache_source = unit->children().front();
            CHECK_EQ(1u, cache_source->direct_needs().size());
            Unit* need = cache_source->direct_needs().front();

            PbJobConfig* job_config = plan->Root()->get<JobConfig>().get();

            PbWriteCacheExecutor* write_cache_executor = executor.mutable_write_cache_executor();
            write_cache_executor->set_from(need->identity());
            write_cache_executor->set_tmp_data_path(job_config->tmp_data_path_output());
            int key_num = -1;
            if (need->has<CacheKeyNumber>()) {
                key_num = need->get<CacheKeyNumber>().value;
            }
            write_cache_executor->set_key_num(key_num);
            return false;
        }
    };
};


bool BuildCommonExecutorPass::BuildCacheWriterExecutorPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

