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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#include "flume/planner/common/side_effect_analysis.h"

#include "boost/foreach.hpp"
#include "boost/range/algorithm.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/pass_test_helper.h"
#include "flume/planner/testing/tag_desc.h"
#include "flume/planner/unit.h"
#include "flume/proto/transfer_encoding.pb.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::_;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Property;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class SideEffectAnalysisTest : public PassTest {
protected:
};

TEST_F(SideEffectAnalysisTest, TestAll) {
    PlanDesc p1 = ProcessNode();
    PlanDesc c_p1 = ChannelUnit();
    PlanDesc sn = ShuffleNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc p3 = ProcessNode();
    PlanDesc c_p2 = ChannelUnit();
    PlanDesc sink = SinkNode();

    PbJobConfig message;
    message.set_tmp_data_path_output("/nowhere");
    JobConfig job_config;
    job_config.Assign(&message);

    PlanDesc origin = (Job()[
        Task()[
            LogicalExecutor()[
                p1
            ],
            CacheWriterExecutor()[
                c_p1
            ],
            ShuffleExecutor(PbScope::GROUP)[
                sn,
                LogicalExecutor()[
                    p2
                ],
                CacheWriterExecutor()[
                    c_p2
                ],
                LogicalExecutor() [
                    p3 + ShouldCache()
                ],
                LogicalExecutor() [
                    sink
                ]
            ]
        ]
    ])
        | p1 >> c_p1
        | p1 >> sn >> p2 >> c_p2
        | p2 >> p3 >> sink;

    PlanDesc expected = (Job()[
        Task()[
            LogicalExecutor()[
                p1 +NoTag<HasSideEffect>()
            ],
            CacheWriterExecutor()[
                c_p1 +NewTagDesc<HasSideEffect>()
            ] +NewTagDesc<HasSideEffect>()
            ,
            ShuffleExecutor(PbScope::GROUP)[
                sn +NoTag<HasSideEffect>(),
                LogicalExecutor()[
                    p2 +NoTag<HasSideEffect>()
                ] +NoTag<HasSideEffect>(),
                CacheWriterExecutor()[
                    c_p2 +NewTagDesc<HasSideEffect>()
                ] +NewTagDesc<HasSideEffect>(),
                LogicalExecutor() [
                    p3 +NewTagDesc<HasSideEffect>()
                ],
                LogicalExecutor() [
                    sink +NewTagDesc<HasSideEffect>()
                ]
            ] +NoTag<HasSideEffect>()
        ] +NoTag<HasSideEffect>()
    ] +NoTag<HasSideEffect>())
    | p1 >> c_p1
    | p1 >> sn >> p2 >> c_p2
    | p2 >> p3 >> sink;

    ExpectPass<SideEffectAnalysis>(origin, expected);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
