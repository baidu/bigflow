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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/common/build_executor_message_pass.h"

#include "boost/foreach.hpp"
#include "boost/range/algorithm.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Property;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class BuildExecutorMessagePassTest : public PassTest {
protected:
    typedef PbExecutor::Dispatcher Dispatcher;
};

TEST_F(BuildExecutorMessagePassTest, EmptyTask) {
    PlanDesc origin = Task();

    PlanDesc expected = Task();

    ExpectPass<BuildExecutorMessagePass>(origin, expected);
}

TEST_F(BuildExecutorMessagePassTest, LinearInputOutput) {
    PlanDesc l = LoadNode();
    PlanDesc cm = ChannelUnit();
    PlanDesc cr = ChannelUnit();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task() [
            LogicalExecutor()[
                l +NewTagDesc<DisablePriority>()
            ],
            ExternalExecutor()[
                cm
            ]
        ],
        Task() [
            ExternalExecutor()[
                cr
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> cm >> cr >> s;

    PlanDesc expected = Job()[
        Task() [
            LogicalExecutor()[
                l
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input_size, 0),
                Property(&PbExecutor::output, ElementsAre(l.identity())),
                Property(&PbExecutor::dispatcher_size, 1),
                Property(&PbExecutor::dispatcher, ElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, l.identity()),
                        Property(&Dispatcher::has_priority, false),
                        Property(&Dispatcher::usage_count, 1),
                        Property(&Dispatcher::need_dataset, false)
                    )
                ))
            )),
            ExternalExecutor()[
                cm
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input, ElementsAre(l.identity())),
                Property(&PbExecutor::output_size, 0),
                Property(&PbExecutor::dispatcher_size, 1),
                Property(&PbExecutor::dispatcher, ElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, cm.identity()),
                        Property(&Dispatcher::has_priority, false),
                        Property(&Dispatcher::usage_count, 0),
                        Property(&Dispatcher::need_dataset, false)
                    )
                ))
            ))
        ],
        Task() [
            ExternalExecutor()[
                cr
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input_size, 0),
                Property(&PbExecutor::output, ElementsAre(cr.identity())),
                Property(&PbExecutor::dispatcher_size, 1),
                Property(&PbExecutor::dispatcher, ElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, cr.identity()),
                        Property(&Dispatcher::has_priority, true),
                        Property(&Dispatcher::priority, 0),
                        Property(&Dispatcher::usage_count, 1),
                        Property(&Dispatcher::need_dataset, false)
                    )
                ))
            )),
            LogicalExecutor()[
                s
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input, ElementsAre(cr.identity())),
                Property(&PbExecutor::output_size, 0),
                Property(&PbExecutor::dispatcher_size, 0)
            ))
        ]
    ]
    | l >> cm >> cr >> s;

    ExpectPass<BuildExecutorMessagePass>(origin, expected);
}

TEST_F(BuildExecutorMessagePassTest, MultiInputOutput) {
    PlanDesc c0 = ChannelUnit();
    PlanDesc c1 = ChannelUnit();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        Task() [
            LogicalExecutor()[
                p0
            ]
        ],
        Task() [
            ExternalExecutor()[
                c0, c1
            ],
            LogicalExecutor()[
                p1
            ],
            LogicalExecutor()[
                p2
            ]
        ]
    ]
    | p0 >> c0
    | p0 >> c1
    | c0 >> PREPARED >> p1
    | c0 >> p2
    | c1 >> p1
    | c1 >> p2;

    PlanDesc expected = Job()[
        Task() [
            LogicalExecutor()[
                p0
            ]
        ],
        Task() [
            ExternalExecutor()[
                c0, c1
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input_size, 0),
                Property(&PbExecutor::output, UnorderedElementsAre(c0.identity(), c1.identity())),
                Property(&PbExecutor::dispatcher, UnorderedElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, c0.identity()),
                        Property(&Dispatcher::has_priority, true),
                        Property(&Dispatcher::priority, 0),
                        Property(&Dispatcher::usage_count, 2),
                        Property(&Dispatcher::need_dataset, true)
                    ),
                    AllOf(
                        Property(&Dispatcher::identity, c1.identity()),
                        Property(&Dispatcher::has_priority, true),
                        Property(&Dispatcher::priority, 1),
                        Property(&Dispatcher::usage_count, 2),
                        Property(&Dispatcher::need_dataset, false)
                    )
                ))
            )),
            LogicalExecutor()[
                p1
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input, UnorderedElementsAre(c0.identity(), c1.identity())),
                Property(&PbExecutor::output_size, 0),
                Property(&PbExecutor::dispatcher_size, 1),
                Property(&PbExecutor::dispatcher, ElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, p1.identity()),
                        Property(&Dispatcher::has_priority, true),
                        Property(&Dispatcher::priority, 0),
                        Property(&Dispatcher::usage_count, 0),
                        Property(&Dispatcher::need_dataset, false)
                    )
                ))
            )),
            LogicalExecutor()[
                p2
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input, UnorderedElementsAre(c0.identity(), c1.identity())),
                Property(&PbExecutor::output_size, 0),
                Property(&PbExecutor::dispatcher_size, 1),
                Property(&PbExecutor::dispatcher, ElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, p2.identity()),
                        Property(&Dispatcher::has_priority, true),
                        Property(&Dispatcher::priority, 0),
                        Property(&Dispatcher::usage_count, 0),
                        Property(&Dispatcher::need_dataset, false)
                    )
                ))
            ))
        ]
    ]
    | p0 >> c0
    | p0 >> c1
    | c0 >> PREPARED >> p1
    | c0 >> p2
    | c1 >> p1
    | c1 >> p2;

    ExpectPass<BuildExecutorMessagePass>(origin, expected);
}

TEST_F(BuildExecutorMessagePassTest, NestedInputOutput) {
    PlanDesc l = LoadNode();
    PlanDesc sn = ShuffleNode();
    PlanDesc p = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc origin = Job()[
        Task()[
            LogicalExecutor()[
                l
            ],
            ShuffleExecutor(PbScope::BUCKET)[
                sn,
                LogicalExecutor()[
                    p
                ]
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> sn >> p >> s;

    PlanDesc expected = Job()[
        Task()[
            LogicalExecutor()[
                l
            ],
            ShuffleExecutor(PbScope::BUCKET)[
                sn,
                LogicalExecutor()[
                    p
                ]
            ] +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::input, UnorderedElementsAre(l.identity())),
                Property(&PbExecutor::output, UnorderedElementsAre(p.identity())),
                Property(&PbExecutor::dispatcher, UnorderedElementsAre(
                    AllOf(
                        Property(&Dispatcher::identity, sn.identity()),
                        Property(&Dispatcher::has_priority, true),
                        Property(&Dispatcher::priority, 0),
                        Property(&Dispatcher::usage_count, 1),
                        Property(&Dispatcher::need_dataset, true)
                    )
                ))
            )),
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> sn >> p >> s;

    ExpectPass<BuildExecutorMessagePass>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
