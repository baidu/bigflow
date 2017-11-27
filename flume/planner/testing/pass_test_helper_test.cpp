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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/testing/pass_test_helper.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/rule_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class BaseTestRemoveRule : public RuleDispatcher::Rule {
public:
    BaseTestRemoveRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::SHUFFLE_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        std::vector<Unit*> needs = unit->direct_needs();
        CHECK_EQ(needs.size(), 1);
        Unit* need = needs.front();

        std::vector<Unit*> users = unit->direct_users();
        CHECK_EQ(users.size(), 1);
        Unit* user = users.front();

        plan->AddDependency(need, user);
        plan->ReplaceFrom(user, unit->identity(), need->identity());

        plan->RemoveUnit(unit);
        return true;
    }
};

class BaseTestAddRule : public RuleDispatcher::Rule {
public:
    BaseTestAddRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::SINK_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* sinker = plan->NewUnit(false);
        sinker->set_type(Unit::LOGICAL_EXECUTOR);
        plan->ReplaceControl(unit->father(), unit, sinker);

        PbExecutor* message = &sinker->get<PbExecutor>();
        message->set_type(PbExecutor::LOGICAL);
        *message->mutable_logical_executor()->mutable_node() = unit->get<PbLogicalPlanNode>();

        unit->set<ExecutedByFather>();
        return true;
    }
};

class PbExecutorTagImpl : public TagDesc {
public:
    virtual void Set(Unit* unit) {
        unit->set<PbExecutor>();
    }

    virtual bool Test(Unit* unit) {
        return unit->has<PbExecutor>();
    }
};

TagDescRef PbExecutorTag() {
    return TagDescRef(new PbExecutorTagImpl());
}

class ExecutedByFatherTagImpl : public TagDesc {
public:
    virtual void Set(Unit* unit) {
        unit->set<ExecutedByFather>();
    }

    virtual bool Test(Unit* unit) {
        return unit->has<ExecutedByFather>();
    }
};

TagDescRef ExecutedByFatherTag() {
    return TagDescRef(new ExecutedByFatherTagImpl());
}

} // namespace

class BaseTestRemovePass : public Pass {
public:
    ~BaseTestRemovePass() {}

    bool Run(Plan* plan) {
        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
        dispatcher.AddRule(new BaseTestRemoveRule());
        bool change = dispatcher.Run(plan);
        return change;
    }
};

class BaseTestAddPass : public Pass {
public:
    ~BaseTestAddPass() {}

    bool Run(Plan* plan) {
        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
        dispatcher.AddRule(new BaseTestAddRule());
        bool change = dispatcher.Run(plan);
        return change;
    }
};


class PassTestHelperTest : public PassTest {};

TEST_F(PassTestHelperTest, RemoveNodeMatchWithDeclaredDesc) {
    PlanDesc root = Job(), task = Task();
    PlanDesc input = InputScope(), load = LoadNode(), p1 = ProcessNode();
    PlanDesc group = GroupScope(), s1 = ShuffleNode(), p2 = ProcessNode();

    PlanDesc origin = root[
        task[
            input[
                load,
                p1
            ],
            group[
                s1,
                p2
            ]
        ]
    ]
    | load >> p1 >> s1 >> p2;

    PlanDesc expected = root[
        task[
            input[
                load,
                p1
            ],
            group[
                p2
            ]
        ]
    ]
    | load >> p1 >> p2;

    ExpectPass<BaseTestRemovePass>(origin, expected);
}

TEST_F(PassTestHelperTest, RemoveNodeMatchWithTempDesc) {
    PlanDesc l1 = LoadNode(), p1 = ProcessNode(), s1 = ShuffleNode(), p2 = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            ControlUnit()[
                l1,
                p1
            ],
            ControlUnit()[
                s1,
                p2
            ]
        ]
    ]
    | l1 >> p1 >> s1 >> p2;

    PlanDesc expected = Job()[
        Task()[
            ControlUnit()[
                l1,
                p1
            ],
            ControlUnit()[
                p2
            ]
        ]
    ]
    | l1 >> p1 >> p2;

    ExpectPass<BaseTestRemovePass>(origin, expected);
}

TEST_F(PassTestHelperTest, AddNode) {
    PlanDesc root = Job(), task = Task(), input = InputScope(), group = GroupScope();
    PlanDesc load = LoadNode(), sink = SinkNode();
    PlanDesc shuffle = ShuffleNode(), p1 = ProcessNode(), p2 = ProcessNode();

    PlanDesc origin = root[
        task[
            input[
                load,
                p1
            ],
            group[
                shuffle,
                p2
            ],
            sink
        ]
    ]
    | load >> p1 >> shuffle >> p2 >> sink;

    PlanDesc expected = root[
        task[
            input[
                load,
                p1
            ],
            group[
                shuffle,
                p2
            ],
            ControlUnit()[
                sink // +ExecutedByFatherTag()
            ] // +PbExecutorTag()
        ]
    ]
    | load >> p1 >> shuffle >> p2 >> sink;

    ExpectPass<BaseTestAddPass>(origin, expected);
}

TEST_F(PassTestHelperTest, ExplicitIdentity) {
    PlanDesc root = Job(), task = Task();
    PlanDesc input = InputScope(), load = LoadNode(), p1 = ProcessNode();
    PlanDesc group = GroupScope(), s1 = ShuffleNode(), p2 = ProcessNode();

    PlanDesc origin = root[
        task[
            ControlUnit("input")[
                load,
                p1
            ],
            group[
                s1,
                p2
            ]
        ]
    ]
    | load >> p1 >> s1 >> p2;

    Plan plan;
    origin.to_plan(&plan);

    PlanDesc good_expect = root[
        task[
            ControlUnit("input")[
                load,
                p1
            ],
            ControlUnit(group.identity())[
                s1,
                p2
            ]
        ]
    ]
    | load >> p1 >> s1 >> p2;
    EXPECT_TRUE(good_expect.is_plan(&plan));

    PlanDesc bad_expect = root[
        task[
            ControlUnit("stranger")[
                load,
                p1
            ],
            group[
                s1,
                p2
            ]
        ]
    ]
    | load >> p1 >> s1 >> p2;
    EXPECT_FALSE(bad_expect.is_plan(&plan));
}

TEST_F(PassTestHelperTest, EdgeDesc) {
    PlanDesc root = Job(), task = Task();
    PlanDesc input = InputScope(), load = LoadNode(), p1 = ProcessNode();
    PlanDesc group = GroupScope(), s1 = ShuffleNode(), s2 = ShuffleNode(), p2 = ProcessNode();

    PlanDesc origin = root[
        task[
            input[
                load,
                p1
            ],
            group[
                s1,
                s2,
                p2
            ]
        ]
    ]
    | load >> s1 >> PARTIAL >> p2
    | load >> p1 >> s2 >> PREPARED >> p2;

    Plan plan;
    origin.to_plan(&plan);
    Draw(&plan);

    PlanDesc error = root[
        task[
            input[
                load,
                p1
            ],
            group[
                s1,
                s2,
                p2
            ]
        ]
    ]
    | load >> s1 >> PREPARED >> p2
    | load >> p1 >> s2 >> PARTIAL >> p2;

    EXPECT_TRUE(origin.is_plan(&plan));
    EXPECT_FALSE(error.is_plan(&plan));
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
