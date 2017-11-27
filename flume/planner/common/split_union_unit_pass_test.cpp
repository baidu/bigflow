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
// Author: Pan Yuchang<panyuchang@baidu.com>

#include "flume/planner/common/split_union_unit_pass.h"

#include "boost/assign.hpp"

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace common {

using namespace boost::assign;

class SplitUnionUnitPassTest : public PassTest {
protected:
    class PbFromCheckerImpl : public TagDesc {
    public:
        PbFromCheckerImpl(std::vector<std::string> ids, bool need_set, Unit::Type type) :
                m_ids(ids), m_need_set(need_set), m_type(type) {}

        void Set(Unit* unit) {
            if (!m_need_set) {
                return;
            }
            PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
            if (m_type == Unit::UNION_NODE) {
                PbUnionNode* union_node = node.mutable_union_node();
                union_node->clear_from();
                for (size_t i = 0; i < m_ids.size(); ++i) {
                    union_node->add_from(m_ids[i]);
                }
            }
            if (m_type == Unit::PROCESS_NODE) {
                PbProcessNode* process_node = node.mutable_process_node();
                process_node->clear_input();
                for (size_t i = 0; i < m_ids.size(); ++i) {
                    PbProcessNode::Input* input = process_node->add_input();
                    input->set_from(m_ids[i]);
                }
            }
        }

        bool Test(Unit* unit) {
            CHECK(unit->has<PbLogicalPlanNode>());
            std::vector<std::string> froms = get_froms(unit);
            if (m_ids.size() != froms.size()) {
                return false;
            }
            for (size_t i = 0; i < m_ids.size(); ++i) {
                if (m_ids[i] != froms[i]) {
                    return false;
                }
            }
            return true;
        }

    private:
        std::vector<std::string> get_froms(Unit* unit) {
            std::vector<std::string> froms;
            if (!unit->has<PbLogicalPlanNode>()) {
                return froms;
            }
            PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
            switch (node.type()) {
                case PbLogicalPlanNode::UNION_NODE: {
                    for (int i = 0; i < node.union_node().from_size(); ++i) {
                        froms.push_back(node.union_node().from(i));
                    }
                    break;
                }
                case PbLogicalPlanNode::SINK_NODE: {
                    froms.push_back(node.sink_node().from());
                    break;
                }
                case PbLogicalPlanNode::PROCESS_NODE: {
                    for (int i = 0; i < node.process_node().input_size(); ++i) {
                        froms.push_back(node.process_node().input(i).from());
                    }
                    break;
                }
                case PbLogicalPlanNode::SHUFFLE_NODE: {
                    froms.push_back(node.shuffle_node().from());
                    break;
                }
                default: {
                    break;
                }
            }
            return froms;
        }

    private:
        std::vector<std::string> m_ids;
        bool m_need_set;
        Unit::Type m_type;
    };

    TagDescRef PbFromChecker(std::vector<std::string> ids, bool need_set, Unit::Type type) {
        return TagDescRef(new PbFromCheckerImpl(ids, need_set, type));
    }
};

TEST_F(SplitUnionUnitPassTest, NothingChangedForUnionTest) {
    PlanDesc p_0 = ProcessNode();
    PlanDesc u = UnionNode();
    PlanDesc u_k = UnionNode();

    PlanDesc origin = Job()[
        Task()[
            p_0,
            u
        ]
    ]
    | p_0 >> u;

    PlanDesc expected = Job()[
        Task()[
            p_0,
            u +NoTag<MustKeep>()
        ]
    ]
    | p_0 >> u;

    ExpectPass<SplitUnionUnitPass>(origin, expected);
}

TEST_F(SplitUnionUnitPassTest, SplitForUnionTest) {
    PlanDesc p_0 = ProcessNode();
    PlanDesc u = UnionNode();
    PlanDesc u_k = UnionNode();

    PlanDesc origin = Job()[
        Task()[
            p_0,
            u +PbFromChecker(list_of<std::string>(p_0.identity())(p_0.identity()),
                             true, Unit::UNION_NODE)
        ]
    ]
    | p_0 >> u;

    PlanDesc expected = Job()[
        Task()[
            p_0,
            u +NoTag<MustKeep>(),
            u_k +PbFromChecker(list_of<std::string>(p_0.identity()), false, Unit::UNION_NODE)
                +NewTagDesc<MustKeep>()
        ]
    ]
    | p_0 >> u
    | p_0 >> u_k >> u;

    ExpectPass<SplitUnionUnitPass>(origin, expected);
}

TEST_F(SplitUnionUnitPassTest, ComplicateSplitForUnionTest) {
    PlanDesc p_0 = ProcessNode();
    PlanDesc p_1 = ProcessNode();
    PlanDesc p_2 = ProcessNode();
    PlanDesc p_3 = ProcessNode();
    PlanDesc u = UnionNode();
    PlanDesc u_k_0 = UnionNode();
    PlanDesc u_k_1 = UnionNode();

    PlanDesc origin = Job()[
        Task()[
            p_0,
            p_1,
            p_2,
            p_3,
            u +PbFromChecker(list_of<std::string>(p_0.identity())(p_0.identity())
                             (p_1.identity())(p_2.identity())(p_2.identity())
                             (p_3.identity()), true, Unit::UNION_NODE)
        ]
    ]
    | p_0 >> u
    | p_1 >> u
    | p_2 >> u
    | p_3 >> u;

    PlanDesc expected = Job()[
        Task()[
            p_0,
            p_1,
            p_2,
            p_3,
            u +NoTag<MustKeep>(),
            u_k_0 +PbFromChecker(list_of<std::string>(p_0.identity()), false, Unit::UNION_NODE)
                    +NewTagDesc<MustKeep>(),
            u_k_1 +PbFromChecker(list_of<std::string>(p_2.identity()), false, Unit::UNION_NODE)
                    +NewTagDesc<MustKeep>()
        ]
    ]
    | p_0 >> u
    | p_0 >> u_k_0 >> u
    | p_1 >> u
    | p_2 >> u
    | p_2 >> u_k_1 >> u
    | p_3 >> u;

    ExpectPass<SplitUnionUnitPass>(origin, expected);
}

TEST_F(SplitUnionUnitPassTest, NothingChangedForProcessTest) {
    PlanDesc p_0 = ProcessNode();
    PlanDesc p = ProcessNode();
    PlanDesc u_k = UnionNode();

    PlanDesc origin = Job()[
        Task()[
            p_0,
            p
        ]
    ]
    | p_0 >> p;

    PlanDesc expected = Job()[
        Task()[
            p_0,
            p +NoTag<MustKeep>()
        ]
    ]
    | p_0 >> p;

    ExpectPass<SplitUnionUnitPass>(origin, expected);
}

TEST_F(SplitUnionUnitPassTest, SplitForProcessTest) {
    PlanDesc p_0 = ProcessNode();
    PlanDesc p = ProcessNode();
    PlanDesc u_k = UnionNode();

    PlanDesc origin = Job()[
        Task()[
            p_0,
            p +PbFromChecker(list_of<std::string>(p_0.identity())(p_0.identity()),
                             true, Unit::PROCESS_NODE)
        ]
    ]
    | p_0 >> p;

    PlanDesc expected = Job()[
        Task()[
            p_0,
            p +NoTag<MustKeep>(),
            u_k +PbFromChecker(list_of<std::string>(p_0.identity()), false, Unit::PROCESS_NODE)
                +NewTagDesc<MustKeep>()
        ]
    ]
    | p_0 >> p
    | p_0 >> u_k >> p;

    ExpectPass<SplitUnionUnitPass>(origin, expected);
}

TEST_F(SplitUnionUnitPassTest, ComplicateSplitForProcessTest) {
    PlanDesc p_0 = ProcessNode();
    PlanDesc p_1 = ProcessNode();
    PlanDesc p_2 = ProcessNode();
    PlanDesc p_3 = ProcessNode();
    PlanDesc p = ProcessNode();
    PlanDesc u_k_0 = UnionNode();
    PlanDesc u_k_1 = UnionNode();

    PlanDesc origin = Job()[
        Task()[
            p_0,
            p_1,
            p_2,
            p_3,
            p +PbFromChecker(list_of<std::string>(p_0.identity())(p_0.identity())
                             (p_1.identity())(p_2.identity())(p_2.identity())
                             (p_3.identity()), true, Unit::PROCESS_NODE)
        ]
    ]
    | p_0 >> p
    | p_1 >> p
    | p_2 >> p
    | p_3 >> p;

    PlanDesc expected = Job()[
        Task()[
            p_0,
            p_1,
            p_2,
            p_3,
            p +NoTag<MustKeep>(),
            u_k_0 +PbFromChecker(list_of<std::string>(p_0.identity()), false, Unit::PROCESS_NODE)
                    +NewTagDesc<MustKeep>(),
            u_k_1 +PbFromChecker(list_of<std::string>(p_2.identity()), false, Unit::PROCESS_NODE)
                    +NewTagDesc<MustKeep>()
        ]
    ]
    | p_0 >> p
    | p_0 >> u_k_0 >> p
    | p_1 >> p
    | p_2 >> p
    | p_2 >> u_k_1 >> p
    | p_3 >> p;

    ExpectPass<SplitUnionUnitPass>(origin, expected);
}
}  // namespace common
}  // namespace planner
}  // namespace flume
}  // namespace baidu

