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

#include "flume/planner/spark/build_transfer_executor_pass.h"

#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"

#include "flume/planner/spark/testing/testing_helper.h"
#include "flume/planner/spark/tags.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/util/reflection.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

using ::testing::_;
using ::testing::AllOf;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::Property;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class BuildTransferExecutorPassTest : public PassTest {
public:
    PlanDesc cache_input() {
        return ExternalExecutor() + NewTagDesc(CACHE_INPUT);
    }
};

TEST_F(BuildTransferExecutorPassTest, BuildCacheInputExecutor) {
    PlanDesc l = LoadNode();
    CacheNodeInfo cache_node_info;
    cache_node_info.cache_node_id = "123456";

    PlanDesc origin = Job()[
        Task()[
            cache_input()[
                l
            ] + NewTagDesc(cache_node_info)
        ]
    ];

    PlanDesc expected = Job()[
        Task()[
            cache_input()[
                l
            ] +MatchTag<PbSparkTask::PbCacheInput>(AllOf(
                Property(&PbSparkTask::PbCacheInput::id, l.identity()),
                Property(&PbSparkTask::PbCacheInput::cache_node_id, "123456")
            )) +MatchTag<PbExecutor>(AllOf(
                Property(&PbExecutor::type, PbExecutor::EXTERNAL),
                Property(&PbExecutor::external_executor,
                         Property(&PbExternalExecutor::id, l.identity()))
            ))
        ]
    ];

    ExpectPass<BuildTransferExecutorPass>(origin, expected);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
