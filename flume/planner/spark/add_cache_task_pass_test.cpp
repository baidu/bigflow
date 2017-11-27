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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)
//

#include "flume/planner/spark/add_cache_task_pass.h"

#include "thirdparty/gtest/gtest.h"

#include "flume/planner/spark/testing/testing_helper.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/planner/spark/tags.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class IdToCacheTaskCheckImpl : public TagDesc {
public:
    explicit IdToCacheTaskCheckImpl() {}
    virtual void Set(Unit * unit) {
    }

    virtual bool Test(Unit * unit) {
        Unit* root = unit->plan()->Root();
        if (!root->has<NodeIdToCacheTasks>()) {
            return false;
        }

        NodeIdToCacheTasks& id2task = root->get<NodeIdToCacheTasks>();

        std::map<std::string, Unit*>::iterator  search= id2task.find(unit->identity());
        if (search == id2task.end()) {
            return false;
        }
        Unit * cache_task = search->second;

        if (!cache_task->has<CacheOrigin>()) {
            return false;
        }
        CacheOrigin cache_origin = cache_task->get<CacheOrigin>();
        if(cache_origin.find(unit) != cache_origin.end()){
           return true;
        }

        return false;
    }
};

TagDescRef IdToCacheTaskCheck() {
    return TagDescRef(new IdToCacheTaskCheckImpl());
}


class AddCacheTaskPassTest : public spark::PassTest {};

TEST_F(AddCacheTaskPassTest, EmptyPlan) {
    ExpectPass<AddCacheTaskPass>(DefaultScope(), DefaultScope());
}

TEST_F(AddCacheTaskPassTest, Normal) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc c_l_cache = ChannelUnit();
    PlanDesc c_p1_cache = ChannelUnit();

    CacheKeyNumber cache_key_number = CacheKeyNumber();
    cache_key_number.value = 3;
    KeyScopes key_scope = KeyScopes();
    key_scope.resize(2);

    PlanDesc origin = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l + NewTagDescNoTest<KeyScopes>(key_scope)
                ],
                CacheWriterExecutor()[
                    c_l_cache
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            LogicalExecutor()[
                p1 + NewTagDescNoTest<CacheKeyNumber>(cache_key_number)
            ],
            CacheWriterExecutor()[
                c_p1_cache
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l >> c_l_cache
    | l >> p0 >> p1 >> c_p1_cache
    | p1 >> p2 >> s;

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l + IdToCacheTaskCheck()
                ],
                CacheWriterExecutor()[
                    c_l_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            LogicalExecutor()[
                p1 + IdToCacheTaskCheck()
            ],
            CacheWriterExecutor()[
                c_p1_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ],
        CacheTaskFrom(&l)
        + NewTagDesc<ShouldKeep>()
        + CacheEffectiveKeyNum(key_scope.size() - 1),
        CacheTaskFrom(&p1)
        + NewTagDesc<ShouldKeep>()
        + CacheEffectiveKeyNum(cache_key_number.value)
    ]
    | l >> c_l_cache
    | l >> p0 >> p1 >> c_p1_cache
    | p1 >> p2 >> s;

    ExpectPass<AddCacheTaskPass>(origin, expected);
}

TEST_F(AddCacheTaskPassTest, PartialOrigin) {
    std::string identity = "cache_from_partail_origin_upstream";
    PlanDesc l0 = LoadNode();
    PlanDesc l1 = LoadNode();
    PlanDesc p0 = ProcessNode(identity);
    PlanDesc p1 = ProcessNode(identity);
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc c_p0_cache = ChannelUnit();
    PlanDesc c_p1_cache = ChannelUnit();
    PlanDesc c_task = CacheTaskFrom(&p0,&p1);

    CacheKeyNumber cache_key_number = CacheKeyNumber();
    cache_key_number.value = 3;

    PlanDesc origin = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l0
                ]
            ],
            LogicalExecutor()[
                p0 + NewTagDescNoTest<CacheKeyNumber>(cache_key_number)
            ],
            CacheWriterExecutor()[
                c_p0_cache
            ]
        ],
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l1
                ]
            ],
            LogicalExecutor()[
                p1 + NewTagDescNoTest<CacheKeyNumber>(cache_key_number)
            ],
            CacheWriterExecutor()[
                c_p1_cache
            ]
        ],

        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ]
    ]
    | l0 >> p0 >> c_p0_cache
    | l1 >> p1 >> c_p1_cache
    | p0 >> p2
    | p1 >> p2 >> s;

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l0
                ]
            ],
            LogicalExecutor()[
                p0 + IdToCacheTaskCheck()
            ],
            CacheWriterExecutor()[
                c_p0_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
            ]
        ],
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l1
                ]
            ],
            LogicalExecutor()[
                p1 + IdToCacheTaskCheck()
            ],
            CacheWriterExecutor()[
                c_p1_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ],
        c_task
        + NewTagDesc<ShouldKeep>()
        + CacheEffectiveKeyNum(cache_key_number.value)
    ]
    | l0 >> p0 >> c_p0_cache
    | l1 >> p1 >> c_p1_cache
    | p0 >> p2
    | p1 >> p2 >> s;

    ExpectPass<AddCacheTaskPass>(origin, expected);
}

TEST_F(AddCacheTaskPassTest, NotChanged) {
    PlanDesc l = LoadNode();
    PlanDesc p0 = ProcessNode();
    PlanDesc p1 = ProcessNode();
    PlanDesc p2 = ProcessNode();
    PlanDesc s = SinkNode();

    PlanDesc c_l_cache = ChannelUnit();
    PlanDesc c_p1_cache = ChannelUnit();

    PlanDesc origin = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ],
                CacheWriterExecutor()[
                    c_l_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            LogicalExecutor()[
                p1
            ],
            CacheWriterExecutor()[
                c_p1_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ],
        CacheTaskFrom(&l),
        CacheTaskFrom(&p1)
    ]
    | l >> c_l_cache
    | l >> p0 >> p1 >> c_p1_cache
    | l >> p0 >> p1 >> p2 >> s;

    PlanDesc expected = Job() [
        Task()[
            InputScope()[
                LogicalExecutor()[
                    l
                ],
                CacheWriterExecutor()[
                    c_l_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
                ]
            ],
            LogicalExecutor()[
                p0
            ],
            LogicalExecutor()[
                p1
            ],
            CacheWriterExecutor()[
                c_p1_cache + NewTagDesc<AddCacheTaskPass::CacheTask>()
            ]
        ],
        Task()[
            LogicalExecutor()[
                p2
            ],
            LogicalExecutor()[
                s
            ]
        ],
        CacheTaskFrom(&l),
        CacheTaskFrom(&p1)
    ]
    | l >> c_l_cache
    | l >> p0 >> p1 >> c_p1_cache
    | l >> p0 >> p1 >> p2 >> s;

    ExpectPass<AddCacheTaskPass>(origin, expected);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
