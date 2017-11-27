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
// Description:

#include "flume/planner/common/promote_partial_process_pass.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class PromotePartialPorcessPassTest : public PassTest {
public:
    class PbProcessNodeTagImpl : public TagDesc {
    public:
        PbProcessNodeTagImpl(std::string from, bool is_partial) {
            PbProcessNode::Input* input = m_message.add_input();
            *input->mutable_from() = from;
            input->set_is_partial(is_partial);
        }

        virtual void Set(Unit* unit) {
            CHECK(unit->has<PbLogicalPlanNode>());
            PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
            CHECK_EQ(message.type(), PbLogicalPlanNode::PROCESS_NODE);
            message.mutable_process_node()->clear_input();
            message.mutable_process_node()->MergeFrom(m_message);
        }

        virtual bool Test(Unit* unit) {
            return true;
        }

    private:
        PbProcessNode m_message;
    };

    TagDescRef PbProcessNodeTag(std::string from, bool is_partial) {
        return TagDescRef(new PbProcessNodeTagImpl(from, is_partial));
    }
};

TEST_F(PromotePartialPorcessPassTest, SimpleNumber) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        p1,
        p2
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        p1 +NewValueTag<PartialKeyNumber>(1),
        p2 +NewValueTag<PartialKeyNumber>(1)
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

TEST_F(PromotePartialPorcessPassTest, SimpleScopeNumber) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        GroupScope()[
            p1
        ],
        p2
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        GroupScope()[
            p1 +NewValueTag<PartialKeyNumber>(2)
        ],
        p2 +NewValueTag<PartialKeyNumber>(1)
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

TEST_F(PromotePartialPorcessPassTest, NotPromoteProcess) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        GroupScope()[
            p1
        ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true),
        p2 +PbProcessNodeTag(p1.identity(), /* is_partial = */ false)
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        GroupScope()[
            p1 +NewValueTag<PartialKeyNumber>(2)
        ],
        p2 +NewValueTag<PartialKeyNumber>(1)
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

TEST_F(PromotePartialPorcessPassTest, PromoteProcess) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        GroupScope()[
            p1
        ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true),
        p2 +PbProcessNodeTag(p1.identity(), /* is_partial = */ true)
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        GroupScope()[
            p1 +NewValueTag<PartialKeyNumber>(2),
            p2 +NewValueTag<PartialKeyNumber>(1)
        ]
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

TEST_F(PromotePartialPorcessPassTest, NotPromoteProcessScope) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        GroupScope()[
            p1
        ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true),
        GroupScope()[
            p2 +PbProcessNodeTag(p1.identity(), /* is_partial = */ true)
        ]
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        GroupScope()[
            p1 +NewValueTag<PartialKeyNumber>(2)
        ],
        GroupScope()[
            p2 +NewValueTag<PartialKeyNumber>(2)
        ]
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

TEST_F(PromotePartialPorcessPassTest, PromoteProcessMultipleScope) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        GroupScope()[
            GroupScope()[
                GroupScope()[
                    p1
                ]
            ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true)
        ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true),
        p2 +PbProcessNodeTag(p1.identity(), /* is_partial = */ true)
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        GroupScope()[
            GroupScope()[
                GroupScope()[
                    p1 +NewValueTag<PartialKeyNumber>(4)
                ],
                p2 +NewValueTag<PartialKeyNumber>(1)
            ]
        ]
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

TEST_F(PromotePartialPorcessPassTest, PromoteProcessMultipleScopeComplex) {
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        GroupScope()[
            GroupScope()[
                GroupScope()[
                    p1
                ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true)
            ]
        ] +PbScopeTag(PbScope::GROUP) +PbScopeSortedTag(true),
        p2 +PbProcessNodeTag(p1.identity(), /* is_partial = */ true)
    ]
    | p1 >> p2;

    PlanDesc expected = Job()[
        GroupScope()[
            GroupScope()[
                GroupScope()[
                    p1 +NewValueTag<PartialKeyNumber>(4)
                ]
            ],
            p2 +NewValueTag<PartialKeyNumber>(1)
        ]
    ]
    | p1 >> p2;

    ExpectPass<PromotePartialProcessPass>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
