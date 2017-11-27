/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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
// Author: Miao Dongdong (miaodongdong@baidu.com)
//

#include "flume/planner/spark/init_rdd_info_pass.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/planner/testing/pass_test_helper.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {


using ::testing::_;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Property;


class InitRddInfoPassTest : public PassTest {};


TEST_F(InitRddInfoPassTest, MultiSetRddIndex) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc p3 = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            l
        ],
        Task()[
            p0
        ],
        Task()[
            p1
        ],
        Task()[
            p2
        ],
        Task()[
            p3
        ]
    ]
    | l >> p0 >> p1 >> p2
    | l >> p3 >> p2;

    PlanDesc expected = Job()[
        Task()[
            l
        ],
        Task()[
            p0
        ],
        Task()[
            p1
        ],
        Task()[
            p2
        ],
        Task()[
            p3
        ]
    ] +MatchTag<PbSparkJob>(
            Property(&PbSparkJob::rdd,
                     ElementsAre(
                        Property(&PbSparkRDD::rdd_index, 0),
                        Property(&PbSparkRDD::rdd_index, 1),
                        Property(&PbSparkRDD::rdd_index, 2),
                        Property(&PbSparkRDD::rdd_index, 3)
                    )
            )
    )
    | l >> p0 >> p1 >> p2
    | l >> p3 >> p2;

    ExpectPass<InitRddInfoPass>(origin, expected);
}

TEST_F(InitRddInfoPassTest, SetRddIndex) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();

    PlanDesc origin = Job()[
        Task()[
            l
        ],
        Task()[
            p0
        ],
        Task()[
            p1
        ],
        Task()[
            p2
        ]
    ]
    | l >> p0 >> p1 >> p2;

    PlanDesc expected = Job()[
        Task()[
            l
        ],
        Task()[
            p0
        ],
        Task()[
            p1
        ],
        Task()[
            p2
        ]
    ] +MatchTag<PbSparkJob>(
        Property(&PbSparkJob::rdd,
             ElementsAre(
                Property(&PbSparkRDD::rdd_index, 0),
                Property(&PbSparkRDD::rdd_index, 1),
                Property(&PbSparkRDD::rdd_index, 2),
                Property(&PbSparkRDD::rdd_index, 3)
            )
        )
    )
    | l >> p0 >> p1 >> p2;

    ExpectPass<InitRddInfoPass>(origin, expected);
}


}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
