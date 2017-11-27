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

#include "flume/planner/dce/add_distribute_by_default_pass.h"

#include "thirdparty/gtest/gtest.h"

#include "flume/planner/dce/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace dce {

class AddDistributeByDefaultPassTest : public PassTest {};

TEST_F(AddDistributeByDefaultPassTest, EmptyPlan) {
    ExpectPass<AddDistributeByDefaultPass>(DefaultScope(), DefaultScope());
}

TEST_F(AddDistributeByDefaultPassTest, Normal) {
    PlanDesc l = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc sn = ShuffleNode();

    PlanDesc origin = DefaultScope()[
        InputScope()[
            l
        ],
        p + EffectiveKeyNum(0),
        GroupScope()[
            sn
        ]
    ]
    | l >> PARTIAL >> p >> sn;

    PlanDesc sn2 = ShuffleNode(PbShuffleNode::SEQUENCE);

    PlanDesc expected = DefaultScope()[
        InputScope()[
            l
        ],
        BucketScope() [
            sn2,
            p + EffectiveKeyNum(0)
        ],
        GroupScope()[
            sn
        ]
    ]
    | l >> sn2 >> PARTIAL >> p >> sn;

    ExpectPass<AddDistributeByDefaultPass>(origin, expected);
}

TEST_F(AddDistributeByDefaultPassTest, CareAboutKey) {
    PlanDesc l = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc sn = ShuffleNode();

    PlanDesc origin = DefaultScope()[
        InputScope()[
            l
        ],
        p + EffectiveKeyNum(1),
        GroupScope()[
            sn
        ]
    ]
    | l >> PARTIAL >> p >> sn;

    PlanDesc sn2 = ShuffleNode(PbShuffleNode::SEQUENCE);

    PlanDesc expected = DefaultScope()[
        InputScope()[
            l
        ],
        p + EffectiveKeyNum(1),
        GroupScope()[
            sn
        ]
    ]
    | l >> PARTIAL >> p >> sn;

    ExpectPass<AddDistributeByDefaultPass>(origin, expected);
}

TEST_F(AddDistributeByDefaultPassTest, NonPartial) {
    PlanDesc l = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc sn = ShuffleNode();

    PlanDesc origin = DefaultScope()[
        InputScope()[
            l
        ],
        p + EffectiveKeyNum(0),
        GroupScope()[
            sn
        ]
    ]
    | l >> p >> sn;


    PlanDesc expected = DefaultScope()[
        InputScope()[
            l
        ],
        p,
        GroupScope()[
            sn
        ]
    ]
    | l >> p >> sn;

    ExpectPass<AddDistributeByDefaultPass>(origin, expected);
}

TEST_F(AddDistributeByDefaultPassTest, WithPartialAndNonPartialEdge) {
    PlanDesc l1 = LoadNode();
    PlanDesc l2 = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc sn = ShuffleNode();

    PlanDesc origin = DefaultScope()[
        InputScope()[
            l1
        ],
        InputScope()[
            l2
        ],
        p + EffectiveKeyNum(0),
        GroupScope()[
            sn
        ]
    ]
    | l1 >> PARTIAL >> p >> sn
    | l2 >> p;

    PlanDesc sn2 = ShuffleNode(PbShuffleNode::SEQUENCE);
    PlanDesc sn3 = ShuffleNode(PbShuffleNode::BROADCAST);

    PlanDesc expected = DefaultScope()[
        InputScope()[
            l1
        ],
        InputScope()[
            l2
        ],
        BucketScope() [
            sn2,
            sn3,
            p
        ],
        GroupScope()[
            sn
        ]
    ]
    | l1 >> sn2 >> PARTIAL >> p >> sn
    | l2 >> sn3 >> p;

    ExpectPass<AddDistributeByDefaultPass>(origin, expected);
}

TEST_F(AddDistributeByDefaultPassTest, WithUnion) {
    PlanDesc l1 = LoadNode();
    PlanDesc l2 = LoadNode();
    PlanDesc p = ProcessNode();
    PlanDesc sn = ShuffleNode();
    PlanDesc u = UnionNode();

    PlanDesc origin = DefaultScope()[
        InputScope()[
            l1
        ],
        InputScope()[
            l2
        ],
        p + EffectiveKeyNum(0),
        u,
        GroupScope()[
            sn
        ]
    ]
    | l1 >> PARTIAL >> p >> u >> sn
    | l2 >> p;

    PlanDesc sn2 = ShuffleNode(PbShuffleNode::SEQUENCE);
    PlanDesc sn3 = ShuffleNode(PbShuffleNode::BROADCAST);
    PlanDesc sn4 = ShuffleNode(PbShuffleNode::SEQUENCE);

    PlanDesc expected = DefaultScope()[
        InputScope()[
            l1
        ],
        InputScope()[
            l2
        ],
        BucketScope() [
            sn2,
            sn3,
            p
        ],
        BucketScope() [
            sn4,
            u
        ],
        GroupScope()[
            sn
        ]
    ]
    | l1 >> sn2 >> PARTIAL >> p >> sn4 >> u >> sn
    | l2 >> sn3 >> p;

    ExpectPass<AddDistributeByDefaultPass>(origin, expected);
}


}  // namespace dce
}  // namespace planner
}  // namespace flume
}  // namespace baidu

