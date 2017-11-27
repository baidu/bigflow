/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Author: Zhou Kai <bigflow-opensource@baidu.com>
//         Wen Xiang <bigflow-opensource@baidu.com>

#ifndef FLUME_PLANNER_DEPTH_FIRST_DISPATCHER_H_
#define FLUME_PLANNER_DEPTH_FIRST_DISPATCHER_H_

#include <vector>

#include "flume/planner/rule_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {

class DepthFirstDispatcher : public RuleDispatcher {
public:
    enum Type {
        PRE_ORDER = 0,
        POST_ORDER,
    };

    explicit DepthFirstDispatcher(Type type, bool only_apply_one_target = false);

    virtual bool Run(Plan* plan);

private:
    void AddUnitByPreorder(Unit* unit, std::vector<Unit*>* order);
    void AddUnitByPostorder(Unit* unit, std::vector<Unit*>* order);

    Type m_type;
    bool m_only_apply_one_target;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_DEPTH_FIRST_DISPATCHER_H_
