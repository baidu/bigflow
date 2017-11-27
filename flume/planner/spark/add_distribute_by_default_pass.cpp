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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//         Wang Cong <wangcong09@baidu.com>
//

#include "flume/planner/spark/add_distribute_by_default_pass.h"

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/flags.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class AddDistributeByDefaultPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new AddDistributeByDefaultRule);
        return dispatcher.Run(plan);
    }

    class AddDistributeByDefaultRule : public RuleDispatcher::Rule {
    public:

        // we thought a global ProcessNode who has partial input edge
        // and whose effective_key_num is 0 can be distributed by the default concorrency.
        // So do the global UnionNode.
        bool Accept(Plan* plan, Unit* unit) {

            // check is global node
            if (unit->father() != plan->Root()) {
                return false;
            }

            if (unit->type() == Unit::UNION_NODE) {
                return true;
            }
            if (unit->type() != Unit::PROCESS_NODE) {
                return false;
            }
            PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
            const PbProcessNode& pn = node.process_node();
            if (pn.effective_key_num() != 0) {
                return false;
            }

            bool has_partial_input = false;
            for (int i = 0; i != pn.input_size() && !has_partial_input; ++i) {
                has_partial_input = pn.input(i).is_partial();
            }
            return has_partial_input;
        }

        Unit* AddShuffleNode(Plan* plan,
                             Unit* scope,
                             PbShuffleNode::Type type,
                             Unit* from) {
            CHECK(from->has<PbLogicalPlanNode>());

            Unit* sn = plan->NewUnit(toft::CreateCanonicalUUIDString(), true);
            sn->set_type(Unit::SHUFFLE_NODE);
            PbLogicalPlanNode& node = sn->get<PbLogicalPlanNode>();
            node.set_type(PbLogicalPlanNode::SHUFFLE_NODE);
            node.set_id(sn->identity());
            node.set_scope(scope->identity());
            node.mutable_shuffle_node()->set_type(type);
            plan->AddControl(scope, sn);
            PbEntity* objector = node.mutable_objector();
            objector->CopyFrom(from->get<PbLogicalPlanNode>().objector());
            SetShuffleNodeFrom(sn, from->identity());
            return sn;
        }

        void SetShuffleNodeFrom(Unit* sn, const std::string& from) {
            sn->get<PbLogicalPlanNode>().mutable_shuffle_node()->set_from(from);
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* father = plan->Root();
            Unit* scope = plan->NewUnit(toft::CreateCanonicalUUIDString(), false);
            plan->ReplaceControl(father, unit, scope);
            scope->set_type(Unit::SCOPE);
            PbScope& msg = scope->get<PbScope>();
            msg.set_type(PbScope::BUCKET);
            msg.set_id(scope->identity());

            std::map<std::string, Unit*> id2unit;

            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                id2unit[need->identity()] = need;
            }

            PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
            if (node.type() == PbLogicalPlanNode::PROCESS_NODE) {
                const PbProcessNode& pn = node.process_node();
                for (int i = 0; i != pn.input_size(); ++i) {
                    std::string from = pn.input(i).from();
                    CHECK_EQ(1, id2unit.count(from));
                    Unit* sn = NULL;
                    if (pn.input(i).is_partial()) {
                        sn = AddShuffleNode(plan, scope, PbShuffleNode::SEQUENCE, id2unit[from]);
                    } else {
                        sn = AddShuffleNode(plan, scope, PbShuffleNode::BROADCAST, id2unit[from]);
                    }
                    plan->ReplaceDependency(id2unit[from], unit, sn);
                    plan->ReplaceFrom(unit, from, sn->identity());
                }
            } else {
                CHECK_EQ(PbLogicalPlanNode::UNION_NODE, node.type());
                const PbUnionNode& un = node.union_node();
                for (int i = 0; i != un.from_size(); ++i) {
                    std::string from = un.from(i);
                    Unit* sn = AddShuffleNode(plan, scope, PbShuffleNode::SEQUENCE, id2unit[from]);
                    plan->ReplaceDependency(id2unit[from], unit, sn);
                    plan->ReplaceFrom(unit, from, sn->identity());
                }
            }
            unit->get<CacheKeyNumber>().value = 0;
            return true;
        }
    };
};

bool AddDistributeByDefaultPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
