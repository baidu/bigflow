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
// Author: Guo Yezhi <guoyezhi@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>
//
// Build dot description of plan.

#include "flume/planner/common/draw_plan_pass.h"

#include <algorithm>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "toft/base/string/algorithm.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

struct DebugInfo {
    std::map<std::string, std::string> labels;
    std::vector<std::string> messages;
};

struct DotLines {
    std::vector<std::string> lines;
};

DotLines& operator+=(DotLines& dot, const std::string& line) {
    dot.lines.push_back(line);
    return dot;
}

DotLines& operator+=(DotLines& dot1, const DotLines& dot2) {
    std::copy(dot2.lines.begin(), dot2.lines.end(), std::back_inserter(dot1.lines));
    return dot1;
}

DotLines Nest(const DotLines& head, const DotLines& body, const DotLines& tail) {
    DotLines result = head;
    for (size_t i = 0; i < body.lines.size(); ++i) {
        const std::string& line = body.lines[i];
        if (line.empty()) {
            result += line;
        } else {
            result += "  " + line;
        }
    }
    result += tail;

    return result;
}

std::string EscapeLabel(const std::string& str) {
    static char kEscapeChars[] = {
        '\\', '\"', '|', '<', '>', '{', '}', ' ', '\n', '\r', '\t', '\0'
    };

    std::string result;
    result.reserve(str.size());

    size_t last = 0;
    while (last != std::string::npos) {
        size_t next = str.find_first_of(kEscapeChars, last);
        if (next != std::string::npos) {
            result += str.substr(last, next - last);
            result += '\\';
            if (str[next] == '\n') {
                result += 'n';
            } else if (str[next] == '\r') {
                result += 'r';
            } else if (str[next] == '\t') {
                result += 't';
            } else {
                result += str[next];
            }

            last = next + 1;
        } else {
            result += str.substr(last, next);
            last = next;
        }
    }

    return result;
}

std::string GenerateLabelString(Unit* unit) {
    std::vector<std::string> results;

    // Add type and identity by default
    results.push_back(std::string("<type> ") + unit->type_string());
    results.push_back(std::string("<identity> ") + unit->identity());

    // Add user labels
    typedef std::map<std::string, std::string> LabelMap;
    LabelMap& labels = unit->get<DebugInfo>().labels;
    for (LabelMap::iterator ptr = labels.begin(); ptr != labels.end(); ++ptr) {
        results.push_back("<" + EscapeLabel(ptr->first) + "> " + EscapeLabel(ptr->second));
    }

    // Add and consume messages
    std::vector<std::string>& messages = unit->get<DebugInfo>().messages;
    for (size_t i = 0; i < messages.size(); ++i) {
        if (i == 0) {
            results.push_back("");
        }
        results.push_back(EscapeLabel(messages[i]));
    }
    messages.clear();

    return "\"{" + toft::JoinStrings(results, "|") + "}\"";
}

}  // namespace

class DrawPlanPass::Impl {
public:
    static void Run(Plan* plan) {
        DepthFirstDispatcher unit_dispatcher(DepthFirstDispatcher::POST_ORDER);
        unit_dispatcher.AddRule(new DrawLeafRule());
        unit_dispatcher.AddRule(new DrawControlRule());
        unit_dispatcher.Run(plan);

        TopologicalDispatcher flow_dispatcher(false);
        flow_dispatcher.AddRule(new DrawFlowRule());
        flow_dispatcher.Run(plan);
    }

    class DrawLeafRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            UpdateLabels(unit);
            std::string line = "node_" + boost::lexical_cast<std::string>(unit) +
                " [shape=Mrecord, label=" + GenerateLabelString(unit) + "];";
            unit->get<DotLines>() += line;

            return false;
        }

    private:
        static void UpdateLabels(Unit* unit) {
            if (!unit->has<PbLogicalPlanNode>()) {
                return;
            }
            const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
            if (unit->has<ScopeLevel>()) {
                std::string level = boost::lexical_cast<std::string>(unit->get<ScopeLevel>().level);
                UpdateLabel(unit, "50-scopelevel", "ScopeLevel: " + level);
            }

            switch (message.type()) {
                case PbLogicalPlanNode::PROCESS_NODE: {
                    if (unit->type() == Unit::PROCESS_NODE) {
                        UpdateLabel(unit, "01-fn", message.process_node().processor().name());
                        int key_num = message.process_node().effective_key_num();
                        if (key_num != -1) {
                            UpdateLabel(unit, "19-keynum", "Effective Key Number: "
                                     + boost::lexical_cast<std::string>(key_num));
                        } else {
                            UpdateLabel(unit, "19-keynum", "Effective Key Number : All");
                        }
                    }

                    break;
                }
                case PbLogicalPlanNode::LOAD_NODE: {
                    if (unit->type() == Unit::LOAD_NODE) {
                        UpdateLabel(unit, "01-fn", message.load_node().loader().name());
                    }
                    break;
                }
                case PbLogicalPlanNode::SINK_NODE: {
                    if (unit->type() == Unit::SINK_NODE) {
                        UpdateLabel(unit, "01-fn", message.sink_node().sinker().name());
                    }
                    break;
                }
                case PbLogicalPlanNode::SHUFFLE_NODE: {
                    const PbShuffleNode& node = message.shuffle_node();
                    if (unit->type() != Unit::SHUFFLE_NODE) {
                        break;
                    }

                    if (node.type() == PbShuffleNode::BROADCAST) {
                        UpdateLabel(unit, "01-fn", "BROADCAST");
                    } else if (node.type() == PbShuffleNode::KEY) {
                        UpdateLabel(unit, "01-fn", "KEY:" + node.key_reader().name());
                    } else if (node.type() == PbShuffleNode::WINDOW){
                        UpdateLabel(unit, "01-fn", "WINDOW");
                    } else if (node.has_partitioner()) {
                        UpdateLabel(unit, "01-fn", "SEQUENCE:" + node.partitioner().name());
                    } else {
                        UpdateLabel(unit, "01-fn", "SEQUENCE: DISTRIBUTE-ANY");
                    }

                    if (unit->has<GroupGenerator>()) {
                        UpdateLabel(unit, "33-group-gen", "GroupGenerator");
                    }
                    break;
                }
                default: { break; }
            }

            if (message.has_objector()) {
                UpdateLabel(unit, "02-objector", message.objector().name());
            }
            if (message.has_debug_info()) {
                UpdateLabel(unit, "03-debug-info", message.debug_info());
            }
            if (message.has_is_infinite() && message.is_infinite()) {
                UpdateLabel(unit, "04-is-infinite", "IsInfinite");
            }
        }
    };

    class DrawControlRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            DotLines body;

            if (unit == plan->Root()) {
            } else if (unit->type() == Unit::PLAN) {
                body += "style=invis;";
            } else if (unit->type() == Unit::TASK || unit->type() == Unit::JOB) {
                body += "style=dotted;";
            } else {
                body += "style=solid;";
            }

            UpdateLabels(unit);
            std::string description = "text_" + boost::lexical_cast<std::string>(unit) +
                " [shape=Mrecord, style=filled, color=gray, label=" +
                GenerateLabelString(unit) + "];";
            body += description;

            for (Unit::iterator ptr = unit->begin(); ptr != unit->end(); ++ptr) {
                body += (*ptr)->move<DotLines>();
            }

            if (unit != plan->Root()) {
                DotLines head, tail;
                head += "subgraph cluster_" + boost::lexical_cast<std::string>(unit) + " {";
                tail += "}";

                unit->get<DotLines>() = Nest(head, body, tail);
            } else {
                unit->get<DotLines>() = body;
            }

            return false;
        }

    private:
        static void UpdateLabels(Unit* unit) {
            if (unit->type() != Unit::SCOPE) {
                // DEFAULT represents pure scope, otherwise it is an executor.
                return;
            }

            if (!unit->has<PbScope>()) {
                return;
            }
            const PbScope& message = unit->get<PbScope>();

            std::string scope_type;
            switch (message.type()) {
                case PbScope::INPUT: {
                    UpdateLabel(unit, "01-scope", "INPUT");
                    break;
                }
                case PbScope::GROUP: {
                    UpdateLabel(unit, "01-scope", "GROUP");
                    break;
                }
                case PbScope::WINDOW: {
                    UpdateLabel(unit, "01-scope", "WINDOW");
                    break;
                }
                case PbScope::BUCKET: {
                    std::ostringstream label;
                    label << "BUCKET: ";
                    if (message.bucket_scope().has_bucket_size()) {
                        label << message.bucket_scope().bucket_size();
                    } else {
                        label << "DEFAULT SIZE";
                    }
                    UpdateLabel(unit, "01-scope", label.str());
                    break;
                }
                default: { break; }
            }

            if (message.is_sorted()) {
                UpdateLabel(unit, "02-sorted", "SORTED");
            }

            if (message.has_concurrency()) {
                std::ostringstream label;
                label << "Concurrency: " << message.concurrency();
                UpdateLabel(unit, "03-concurrency", label.str());
            }

            if (message.has_is_infinite() && message.is_infinite()) {
                UpdateLabel(unit, "04-is-infinite", "IsInfinite");
            }

            if (message.has_is_stream() && message.is_stream()) {
                UpdateLabel(unit, "05-is-stream", "IsStream");
            }
        }
    };

    class DrawFlowRule : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            std::vector<Unit*> users = unit->direct_users();
            for (size_t i = 0; i < users.size(); ++i) {
                std::string from = "node_" + boost::lexical_cast<std::string>(unit);
                std::string to = "node_" + boost::lexical_cast<std::string>(users[i]);

                plan->Root()->get<DotLines>() += from + " -> " + to + ";";
            }

            return false;
        }
    };
};

void DrawPlanPass::UpdateLabel(Unit* unit,
                               const std::string& name, const std::string& description) {
    unit->get<DebugInfo>().labels[name] = description;
}

void DrawPlanPass::RemoveLabel(Unit* unit, const std::string& name) {
    unit->get<DebugInfo>().labels.erase(name);
}

void DrawPlanPass::Clear(Unit* unit) {
    unit->get<DebugInfo>().labels.clear();
}

void DrawPlanPass::AddMessage(Unit* unit, const std::string& message) {
    unit->get<DebugInfo>().messages.push_back(message);
}

DrawPlanPass::~DrawPlanPass() {}

void DrawPlanPass::RegisterListener(toft::Closure<void (const std::string&)>* callback) {
    m_listeners.push_back(callback);
}

bool DrawPlanPass::Run(Plan* plan) {
    Impl::Run(plan);

    DotLines head, tail;
    head += "digraph plan {";
    tail += "}";
    DotLines dot = Nest(head, plan->Root()->move<DotLines>(), tail);

    std::string dot_string = toft::JoinStrings(dot.lines, "\n");
    for (size_t i = 0; i < m_listeners.size(); ++i) {
        m_listeners[i].Run(dot_string);
    }

    // drawer should never change the plan
    return false;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

