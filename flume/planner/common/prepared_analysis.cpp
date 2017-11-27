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
// Author: Wang Song <wangsong06@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/common/prepared_analysis.h"

#include <iterator>
#include <limits>
#include <list>
#include <sstream>
#include <utility>

#include "boost/foreach.hpp"
#include "boost/graph/adjacency_list.hpp"
#include "boost/graph/strong_components.hpp"
#include "boost/graph/topological_sort.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/range/algorithm.hpp"
#include "boost/tuple/tuple.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {

class PreparedAnalysis::Impl {
public:
    static bool run(Plan* plan) {
        DepthFirstDispatcher init_phase(DepthFirstDispatcher::PRE_ORDER);
        init_phase.AddRule(new CleanTags);
        init_phase.AddRule(new AddTerminalDispatcherToPriorityDag);
        init_phase.AddRule(new AddDependantShuffleExecutorToPriorityDag);
        init_phase.AddRule(new AnalyzePreparedNeedsForProcessorExecutor);
        init_phase.AddRule(new AnalyzePreparedNeedsForLogicalExecutor);
        init_phase.AddRule(new AnalyzePreparedNeedsForShuffleExecutor);
        /*
         * TODO
         * Because at runtime, broadcast data will be emitted first which
         * violates the rule of flume: high priority data gets emitted first.
         * So, additional edges from non-broadcast channels to broadcast channels
         * are required to ensure broadcast channels have a higher priority.
         * This behavior should be changed with runtime's implementations.
         */
        init_phase.AddRule(new AnalyzePreparedNeedsForTaskInput);
        init_phase.Run(plan);

        RuleDispatcher basic_analyze_phase;
        basic_analyze_phase.AddRule(new AnalyzeBasicPriorityRelation);
        basic_analyze_phase.AddRule(new AnalyzePriorityRelationForShuffleExecutor);
        basic_analyze_phase.AddRule(new AnalyzePriorityRelationForByRecordShuffleExecutor);
        /*
         * add edge from non-broadcast channels to broadcast channels
         * to ensure broadcast channels have a higher priority.
         */
        basic_analyze_phase.AddRule(new AnalyzePriorityRelationForTaskInput);
        basic_analyze_phase.AddRule(new UpdatePreparePriority);
        basic_analyze_phase.Run(plan);

        DepthFirstDispatcher best_effort_analyze_phase(DepthFirstDispatcher::POST_ORDER);
        best_effort_analyze_phase.AddRule(new AnalyzePriorityRelationForProcessorExecutor);
        best_effort_analyze_phase.AddRule(new UpdatePreparePriority);
        best_effort_analyze_phase.Run(plan);

        RuleDispatcher update_range_phase;
        update_range_phase.AddRule(new AnalyzePriorityRangeForNeeds);
        update_range_phase.AddRule(new UpdatePriorityRange);
        while (update_range_phase.Run(plan)) {}

        RuleDispatcher update_need_prepare_phase;
        update_need_prepare_phase.AddRule(new UpdatePreparedNeedsForProcessorExecutor);
        update_need_prepare_phase.Run(plan);

        return true;
    }

    struct PriorityInfo {
        Unit* unit;
        std::set<Unit*> inputs;
        uint32_t component;
    };

    typedef boost::adjacency_list<
            boost::setS, boost::vecS, boost::bidirectionalS, PriorityInfo
    > PriorityDag;
    typedef boost::graph_traits<PriorityDag>::vertex_descriptor PriorityVertex;
    typedef boost::graph_traits<PriorityDag>::vertex_iterator VertexIterator;
    typedef boost::graph_traits<PriorityDag>::edge_descriptor PriorityEdge;

    // set at father executor, map all desendant upstream leaf units to their priority
    struct PriorityMap : public std::map<Unit*, PriorityVertex> {};

    // set at sub-executor.
    // begin indicate this executor start to dispatch records to downstream at that priority.
    // end means this executor will dispatch done at the end of that priority.
    struct PriorityRange : public std::pair<uint32_t, uint32_t> {};  // [begin, end]

    // set at sub-executor and dispatchers.
    struct PriorityRangeForNeeds : public std::map<Unit*, PriorityRange> {};  // need -> range

    struct IsTaskInput {};

    struct BroadcastChannel : public std::set<Unit*> {};
    struct NonBroadcastChannel : public std::set<Unit*> {};


    class CleanTags : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<PreparedNeeds>();
            unit->clear<NonPreparedNeeds>();
            unit->clear<PreparePriority>();

            unit->clear<PriorityDag>();
            unit->clear<PriorityMap>();
            unit->clear<PriorityRange>();
            unit->clear<PriorityRangeForNeeds>();
            unit->clear<BroadcastChannel>();
            unit->clear<NonBroadcastChannel>();

            return false;
        }
    };

    class AddTerminalDispatcherToPriorityDag : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (!unit->is_leaf() || unit->has<DisablePriority>()) {
                return false;
            }

            BOOST_FOREACH(Unit* user, unit->direct_users()) {
                if (user->task() != unit->task()) {
                    continue;
                }

                if (user->type() == Unit::CHANNEL
                        && user->father()->type() == Unit::SHUFFLE_EXECUTOR) {
                    return false;
                }

                if (user->father() != unit->father()) {
                    return true;
                }
            }

            return unit->direct_users().size() == 0;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            DrawPlanPass::UpdateLabel(unit, "878-is-terminal", "IsTerminal");

            bool is_task_input = false;
            if (unit->type() == Unit::CHANNEL
                    && unit->father()->type() == Unit::EXTERNAL_EXECUTOR) {
                BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                    if (need->task() != unit->task()) {
                        is_task_input = true;
                    }
                }
            }
            Unit* executor = is_task_input ? unit->task() : unit->father();

            PriorityDag& dag = executor->get<PriorityDag>();
            PriorityVertex vertex = boost::add_vertex(dag);
            dag[vertex].unit = unit;

            PriorityMap& map = executor->get<PriorityMap>();
            map[unit] = vertex;

            return false;
        }
    };

    class AddDependantShuffleExecutorToPriorityDag : public RuleDispatcher::Rule {
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
            Unit* executor = unit->father();

            PriorityDag& dag = executor->get<PriorityDag>();
            PriorityVertex vertex = boost::add_vertex(dag);
            dag[vertex].unit = unit;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::CHANNEL) {
                    CHECK_EQ(child->direct_needs().size(), 1u);
                    boost::copy(child->direct_needs(),
                                std::inserter(dag[vertex].inputs, dag[vertex].inputs.end()));
                }
            }

            PriorityMap& map = executor->get<PriorityMap>();
            map[unit] = vertex;
            BOOST_FOREACH(Unit* desendant, unit->get<DataFlow>().nodes) {
                map[desendant] = vertex;
            }

            return false;
        }
    };

    class AnalyzePreparedNeedsForProcessorExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::PROCESS_NODE
                    && unit->father()->type() == Unit::LOGICAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const PbProcessNode& node = unit->get<PbLogicalPlanNode>().process_node();
            Unit* executor = unit->father();

            // add fixed prepared froms
            std::set<std::string> prepared_froms;
            for (int i = 0; i < node.input_size(); ++i) {
                const PbProcessNode::Input& input = node.input(i);
                if (input.is_prepared()) {
                    prepared_froms.insert(input.from());
                }
            }

            // for least_prepared_inputs
            for (int i = 0; i < node.input_size(); ++i) {
                const PbProcessNode::Input& input = node.input(i);

                // FIXME(wenxiang): may have duplicated froms
                int remain_prepared_count = node.least_prepared_inputs() - prepared_froms.size();
                if (prepared_froms.count(input.from()) == 0 && remain_prepared_count > 0) {
                    prepared_froms.insert(input.from());
                }
            }

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (prepared_froms.count(need->identity()) > 0) {
                    executor->get<PreparedNeeds>().insert(need);
                } else {
                    executor->get<NonPreparedNeeds>().insert(need);
                }
            }

            return false;
        }
    };

    class AnalyzePreparedNeedsForLogicalExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return (unit->type() == Unit::UNION_NODE || unit->type() == Unit::SINK_NODE)
                    && unit->father()->type() == Unit::LOGICAL_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->father();
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                executor->get<NonPreparedNeeds>().insert(need);
            }

            return false;
        }
    };

    class AnalyzePreparedNeedsForShuffleExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_EXECUTOR;
        }

        virtual bool IsBroadcastUnit(Unit* unit) {
            if (!unit->has<PbLogicalPlanNode>()) {
                return false;
            }
            if (unit->type() != Unit::SHUFFLE_NODE) {
                return false;
            }
            PbLogicalPlanNode node = unit->get<PbLogicalPlanNode>();
            if (!node.has_shuffle_node()) {
                return false;
            }
            PbShuffleNode::Type type = node.shuffle_node().type();
            return type == PbShuffleNode::BROADCAST;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() != Unit::SHUFFLE_NODE && child->type() != Unit::CHANNEL) {
                    continue;
                }
                if (unit->has<IsLocalDistribute>() && unit->get<PbScope>().distribute_every()) {
                    CHECK_EQ(1u, child->direct_needs().size());
                    if (IsBroadcastUnit(child)) {
                        unit->get<PreparedNeeds>().insert(child->direct_needs()[0]);
                    } else {
                        unit->get<NonPreparedNeeds>().insert(child->direct_needs()[0]);
                    }
                } else {
                    BOOST_FOREACH(Unit* need, child->direct_needs()) {
                        if (child->type() == Unit::CHANNEL || unit->has<IsLocalDistribute>()) {
                            unit->get<NonPreparedNeeds>().insert(need);
                        } else {
                            unit->get<PreparedNeeds>().insert(need);
                        }
                    }
                }
            }

            return false;
        }
    };

    class AnalyzePreparedNeedsForTaskInput : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if(unit->type() != Unit::CHANNEL) {
                return false;
            }
            bool is_task_input = false;
            if (unit->father()->type() == Unit::EXTERNAL_EXECUTOR) {
                BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                    if (need->task() != unit->task()) {
                        is_task_input = true;
                    }
                }
            }
            return is_task_input;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->father()->set<IsTaskInput>();

            if (unit->has<IsBroadcast>()) {
                unit->father()->get<BroadcastChannel>().insert(unit);
            } else {
                unit->father()->get<NonBroadcastChannel>().insert(unit);
            }
            return false;
        }
    };

    class AnalyzeBasicPriorityRelation : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<PriorityDag>() && unit->has<PriorityMap>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PriorityDag& dag = unit->get<PriorityDag>();
            PriorityMap& map = unit->get<PriorityMap>();

            VertexIterator ptr, end;
            boost::tie(ptr, end) = boost::vertices(dag);
            while (ptr != end) {
                Unit* target = dag[*ptr].unit;
                BOOST_FOREACH(Unit* upstream, target->get<DataFlow>().upstreams) {
                    if (map.count(upstream) > 0) {
                        PriorityVertex vertex = map[upstream];
                        boost::add_edge(*ptr, vertex, dag);  // target >> upstream
                    }
                }

                ++ptr;
            }

            return false;
        }
    };

    class AnalyzePriorityRelationForShuffleExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            return unit->has<PriorityDag>() && unit->has<PriorityMap>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PriorityDag& dag = unit->get<PriorityDag>();
            PriorityMap& map = unit->get<PriorityMap>();

            std::set<PriorityVertex> shuffle_nodes;
            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() == Unit::SHUFFLE_NODE) {
                    CHECK_NE(map.count(child), 0u);
                    shuffle_nodes.insert(map[child]);
                }
            }

            // shuffle node always comes first in current shuffle executor implementation
            VertexIterator ptr, end;
            for (boost::tie(ptr, end) = boost::vertices(dag); ptr != end; ++ptr) {
                if (shuffle_nodes.count(*ptr) == 0) {
                    BOOST_FOREACH(PriorityVertex shuffle_node, shuffle_nodes) {
                        boost::add_edge(*ptr, shuffle_node, dag);
                    }
                }
            }

            return false;
        }
    };

    class AnalyzePriorityRelationForByRecordShuffleExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_EXECUTOR) {
                return false;
            }

            if (!unit->has<PbScope>() || !unit->get<PbScope>().distribute_every()) {
                return false;
            }

            return unit->has<PriorityDag>() && unit->has<PriorityMap>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PriorityDag& dag = unit->get<PriorityDag>();
            PriorityMap& map = unit->get<PriorityMap>();

            std::set<PriorityVertex> broadcast_nodes;
            BOOST_FOREACH(Unit* child, unit->children()) {
                const PbLogicalPlanNode& node = child->get<PbLogicalPlanNode>();
                if (child->type() == Unit::SHUFFLE_NODE
                        && node.shuffle_node().type() == PbShuffleNode::BROADCAST) {
                    CHECK_NE(map.count(child), 0u);
                    broadcast_nodes.insert(map[child]);
                }
            }

            // broadcast shuffle node always comes first in current shuffle executor implementation
            VertexIterator ptr, end;
            for (boost::tie(ptr, end) = boost::vertices(dag); ptr != end; ++ptr) {
                if (broadcast_nodes.count(*ptr) == 0) {
                    BOOST_FOREACH(PriorityVertex shuffle_node, broadcast_nodes) {
                        boost::add_edge(*ptr, shuffle_node, dag);
                    }
                }
            }

            return false;
        }
    };

    class AnalyzePriorityRelationForProcessorExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::LOGICAL_EXECUTOR) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() != Unit::PROCESS_NODE) {
                    return false;
                }
            }

            return unit->has<PreparedNeeds>() && unit->has<NonPreparedNeeds>()
                    && unit->father()->has<PriorityDag>() && unit->father()->has<PriorityMap>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PriorityDag& dag = unit->father()->get<PriorityDag>();
            PriorityMap& map = unit->father()->get<PriorityMap>();

            std::set<PriorityVertex> prepared_upstreams;
            BOOST_FOREACH(Unit* need, unit->get<PreparedNeeds>()) {
                if (map.count(need) > 0) {
                    prepared_upstreams.insert(map[need]);
                } else {
                    BOOST_FOREACH(Unit* upstream, need->get<DataFlow>().upstreams) {
                        if (map.count(upstream) > 0) {
                            prepared_upstreams.insert(map[upstream]);
                        }
                    }
                }
            }

            std::set<PriorityVertex> non_prepared_upstreams;
            BOOST_FOREACH(Unit* need, unit->get<NonPreparedNeeds>()) {
                if (map.count(need) > 0) {
                    non_prepared_upstreams.insert(map[need]);
                } else {
                    BOOST_FOREACH(Unit* upstream, need->get<DataFlow>().upstreams) {
                        if (map.count(upstream) > 0) {
                            non_prepared_upstreams.insert(map[upstream]);
                        }
                    }
                }
            }

            // prepared upstream come first
            BOOST_FOREACH(PriorityVertex prepared, prepared_upstreams) {
                BOOST_FOREACH(PriorityVertex non_prepared, non_prepared_upstreams) {
                    boost::add_edge(non_prepared, prepared, dag);
                }
            }

            return false;
        }
    };

    class AnalyzePriorityRelationForTaskInput : public RuleDispatcher::Rule {

        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::EXTERNAL_EXECUTOR && unit->has<IsTaskInput>() &&
                unit->has<BroadcastChannel>() && unit->has<NonBroadcastChannel>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* executor = unit->task();
            PriorityMap& map = executor->get<PriorityMap>();
            PriorityDag& dag = executor->get<PriorityDag>();

            // broadcast channel comes first
            BroadcastChannel& broadcast_channel = unit->get<BroadcastChannel>();
            NonBroadcastChannel& non_broadcast_channel = unit->get<NonBroadcastChannel>();
            BOOST_FOREACH(Unit* broadcast, broadcast_channel) {
                if (map.find(broadcast) != map.end()) {
                    BOOST_FOREACH(Unit* non_broadcast, non_broadcast_channel) {
                        if (map.find(non_broadcast) != map.end()) {
                            boost::add_edge(map[non_broadcast], map[broadcast], dag);
                        }
                    }
                }
            }
            return false;
        }
    };

    class UpdatePreparePriority : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->has<PriorityDag>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PriorityDag& dag = unit->get<PriorityDag>();

            std::list<PriorityVertex> topo_order;
            try {
                boost::topological_sort(dag, std::back_inserter(topo_order));
            } catch (const boost::not_a_dag& e) {
                topo_order.clear();

                uint32_t component_num =
                    boost::strong_components(dag, boost::get(&PriorityInfo::component, dag));

                if (component_num == 1u) {
                    // All vertices are in one component
                    LOG(INFO) << "Fail to find optimizing prepare-solution for "
                              << unit->identity();
                    return false;
                }

                boost::adjacency_list<
                    boost::setS, boost::vecS, boost::bidirectionalS
                > component_dag(component_num);
                std::map< uint32_t, std::set<PriorityVertex> > component_map; // component >> vertex

                BOOST_FOREACH(const PriorityVertex& vertex, boost::vertices(dag)) {
                    component_map[dag[vertex].component].insert(vertex);
                }

                bool is_component_edge_exist = false;
                BOOST_FOREACH(const PriorityEdge& edge, boost::edges(dag)) {
                    uint32_t source_component = dag[boost::source(edge, dag)].component;
                    uint32_t target_component = dag[boost::target(edge, dag)].component;

                    if (source_component != target_component) {
                        boost::add_edge(source_component, target_component, component_dag);
                        is_component_edge_exist = true;
                    }
                }
                if (!is_component_edge_exist) {
                    // No relation between components
                    LOG(INFO) << "Fail to find optimizing prepare-solution for "
                              << unit->identity();
                    return false;
                }

                std::list<uint32_t> topo_order_component;
                try {
                    boost::topological_sort(
                            component_dag,
                            std::back_inserter(topo_order_component));
                } catch (const boost::not_a_dag& e) {
                    CHECK(false) << "This should never happen.";
                }
                BOOST_FOREACH(uint32_t component, topo_order_component) {
                    // Vertices between components are ordered
                    CHECK_NE(component_map.count(component), 0u);
                    BOOST_FOREACH(const PriorityVertex& vertex, component_map[component]) {
                        topo_order.push_back(vertex);
                    }
                }

                CHECK_EQ(topo_order.size(), boost::num_vertices(dag)) << "Wrong vertex number.";
            }

            uint32_t priority = 0;
            BOOST_FOREACH(PriorityVertex vertex, topo_order) {
                std::string label =
                        "Dispatch Priority: " + boost::lexical_cast<std::string>(priority);

                const PriorityInfo& info = dag[vertex];
                *info.unit->get<PreparePriority>() = priority;
                if (info.unit->is_leaf()) {
                    PriorityRange& range = info.unit->get<PriorityRange>();
                    range.first = priority;
                    range.second = priority;
                    DrawPlanPass::UpdateLabel(info.unit, "30-dispatch-priority", label);
                }

                BOOST_FOREACH(Unit* input, info.inputs) {
                    *input->get<PreparePriority>() = priority;

                    PriorityRange& range = input->get<PriorityRange>();
                    range.first = priority;
                    range.second = priority;

                    DrawPlanPass::UpdateLabel(input, "30-dispatch-priority", label);
                }

                ++priority;
            }

            return false;
        }
    };

    class AnalyzePriorityRangeForNeeds : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_leaf() || unit->type() <= Unit::TASK) {
                return false;
            }

            if (unit->has<PriorityRange>()) {
                return false;
            }

            BOOST_FOREACH(Unit* dispatcher, unit->get<UpstreamDispatchers>()) {
                if (!dispatcher->has<PriorityRange>()) {
                    return false;
                }
            }

            BOOST_FOREACH(Unit* executor, unit->get<LeadingExecutors>()) {
                if (!executor->has<PriorityRange>()) {
                    return false;
                }
            }

            return unit->has<PreparedNeeds>() || unit->has<NonPreparedNeeds>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const UpstreamDispatchers& dispatchers = unit->get<UpstreamDispatchers>();
            const LeadingExecutors& executors = unit->get<LeadingExecutors>();
            CollectPriorityRanges(dispatchers, executors, unit->get<PreparedNeeds>(), unit);
            CollectPriorityRanges(dispatchers, executors, unit->get<NonPreparedNeeds>(), unit);

            return false;
        }

        void CollectPriorityRanges(const UpstreamDispatchers& dispatchers,
                                   const LeadingExecutors& executors,
                                   const std::set<Unit*>& needs,
                                   Unit* unit) {
            BOOST_FOREACH(Unit* need, needs) {
                PriorityRangeForNeeds& ranges = unit->get<PriorityRangeForNeeds>();

                if (dispatchers.count(need) > 0) {
                    ranges[need] = need->get<PriorityRange>();
                } else {
                    BOOST_FOREACH(Unit* executor, executors) {
                        if (need->is_descendant_of(executor)) {
                            ranges[need] = executor->get<PriorityRange>();
                        }
                    }
                }

                CHECK_NE(ranges.count(need), 0u);
            }
        }
    };

    class UpdatePriorityRange : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->is_leaf() || unit->type() <= Unit::TASK) {
                return false;
            }

            if (unit->has<PriorityRange>()) {
                return false;
            }

            return unit->has<PriorityRangeForNeeds>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            const uint32_t kMinPriority = std::numeric_limits<uint32_t>::min();
            const uint32_t kMaxPriority = std::numeric_limits<uint32_t>::max();
            PriorityRangeForNeeds& ranges = unit->get<PriorityRangeForNeeds>();

            bool has_prepared_needs = false;
            uint32_t max_prepared_priority = kMinPriority;
            BOOST_FOREACH(Unit* need, unit->get<PreparedNeeds>()) {
                uint32_t priority = ranges[need].second;
                if (priority > max_prepared_priority) {
                    max_prepared_priority = priority;
                }
                has_prepared_needs = true;
            }

            uint32_t min_non_prepared_priority = kMaxPriority;
            uint32_t max_non_prepared_priority = kMinPriority;
            BOOST_FOREACH(Unit* need, unit->get<NonPreparedNeeds>()) {
                const PriorityRange& range = ranges[need];
                for (uint32_t priority = range.first; priority <= range.second; ++priority) {
                    if (has_prepared_needs && priority <= max_prepared_priority) {
                        continue;
                    }

                    if (priority < min_non_prepared_priority) {
                        min_non_prepared_priority = priority;
                    }

                    if (priority > max_non_prepared_priority) {
                        max_non_prepared_priority = priority;
                    }
                }
            }

            PriorityRange& range = unit->get<PriorityRange>();
            if (has_prepared_needs && max_non_prepared_priority <= max_prepared_priority) {
                range.first = max_prepared_priority;
                range.second = max_prepared_priority;
            } else {
                range.first = min_non_prepared_priority;
                range.second = max_non_prepared_priority;
            }

            std::ostringstream label;
            label << "Priority Range: " << range.first << " - " << range.second;
            DrawPlanPass::UpdateLabel(unit, "31-priority-range", label.str());

            return true;
        }
    };

    class UpdatePreparedNeedsForProcessorExecutor : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::LOGICAL_EXECUTOR) {
                return false;
            }

            BOOST_FOREACH(Unit* child, unit->children()) {
                if (child->type() != Unit::PROCESS_NODE) {
                    return false;
                }
            }

            return unit->has<PreparedNeeds>() || unit->has<NonPreparedNeeds>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            std::set<Unit*> forced_prepared_needs;
            if (unit->has<PriorityRangeForNeeds>()) {
                PriorityRangeForNeeds& ranges = unit->get<PriorityRangeForNeeds>();

                // after this priority, all the inputs can be stream processed
                uint32_t latest_prepared_priority = 0;
                BOOST_FOREACH(Unit* need, unit->get<PreparedNeeds>()) {
                    latest_prepared_priority =
                            std::max(latest_prepared_priority, ranges[need].second + 1);
                }
                BOOST_FOREACH(Unit* need, unit->get<NonPreparedNeeds>()) {
                    uint32_t earliest_priority = ranges[need].first;
                    if (earliest_priority < latest_prepared_priority) {
                        forced_prepared_needs.insert(need);
                    }
                }
            } else if (unit->has<PreparedNeeds>()) {
                forced_prepared_needs.insert(unit->get<NonPreparedNeeds>().begin(),
                                             unit->get<NonPreparedNeeds>().end());
            }

            BOOST_FOREACH(Unit* need, forced_prepared_needs) {
                unit->get<PreparedNeeds>().insert(need);
                unit->get<NonPreparedNeeds>().erase(need);
            }

            BOOST_FOREACH(Unit* need, unit->get<PreparedNeeds>()) {
                need->set<NeedPrepare>();
                DrawPlanPass::UpdateLabel(need, "32-need-prepare", "NeedPrepare");
            }

            return false;
        }
    };
};

bool PreparedAnalysis::Run(Plan* plan) {
    return Impl::run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu


