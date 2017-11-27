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

#include "flume/planner/spark/add_cache_task_pass.h"

#include <map>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/flags.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class AddCacheTaskPass::Impl {
public:

    static bool Run(Plan* plan) {
        DepthFirstDispatcher add_cache_task(DepthFirstDispatcher::POST_ORDER);
        add_cache_task.AddRule(new AddCacheTaskRule);
        return add_cache_task.Run(plan);
    }

    class AddCacheTaskRule : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->father() != NULL && unit->father()->type() == Unit::CACHE_WRITER
                && !unit->has<CacheTask>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            bool is_leaf = false;
            CHECK_EQ(1u, unit->direct_needs().size());
            Unit* origin = unit->direct_needs()[0];

            NodeIdToCacheTasks& id2task = plan->Root()->get<NodeIdToCacheTasks>();
            Unit*& cache_task = id2task[origin->identity()];
            if (cache_task == NULL) {
                cache_task = plan->NewUnit(is_leaf = false);
                plan->AddControl(plan->Root(), cache_task);
                cache_task->set_type(Unit::TASK);
                cache_task->get<PbScope>().set_type(PbScope::DEFAULT); // test_helper need this.
            }

            cache_task->get<CacheOrigin>().insert(origin);

            unit->get<CacheTask>().Assign(cache_task);
            cache_task->get<ShouldKeep>();

            DrawPlanPass::UpdateLabel(cache_task,
                    "1201-cache-task", "Cache RDD for node: " + origin->identity());

            PbSparkTask &task = cache_task->get<PbSparkTask>();
            task.set_type(PbSparkTask::CACHE);
            task.set_cache_node_id(origin->identity());

            int32_t key_num = 0; // not include global key
            if (origin->has<CacheKeyNumber>()) {
                key_num = origin->get<CacheKeyNumber>().value;
            } else {
                key_num = origin->get<KeyScopes>().size() - 1;
            }
            task.set_cache_effective_key_number(key_num);

            return true;
        }
    };
};

bool AddCacheTaskPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
