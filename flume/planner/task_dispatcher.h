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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//         Pan Yuchang <bigflow-opensource@baidu.com>

#ifndef FLUME_PLANNER_TASK_DISPATCHER_H_
#define FLUME_PLANNER_TASK_DISPATCHER_H_

#include <set>
#include <utility>
#include <vector>

#include "flume/planner/rule_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {

typedef std::set<std::pair<Unit*, Unit*> > DependencySet;

class TaskDispatcher : public RuleDispatcher {
public:
    enum Type {
        NONE_ORDER = 0,
        PRE_TOPOLOGY_ORDER = 1,
        POST_TOPOLOGY_ORDER = 2,
    };
    TaskDispatcher();
    explicit TaskDispatcher(Type type);

    virtual bool Run(Plan* plan);

private:
    void FindAllTasksByTopology(Plan* plan, std::vector<Unit*>* tasks);
    void FindAllTasks(Unit* unit, std::vector<Unit*>* tasks);
    void AddTaskRelations(Unit* unit, DependencySet* task_relations);
    bool AddTaskByDiff(const std::set<Unit*>& set_in,
                        const std::set<Unit*>& set_not_in,
                        std::vector<Unit*>* tasks);

private:
    Type m_type;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_TASK_DISPATCHER_H_
