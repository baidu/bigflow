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
//         Pan Yuchang <panyuchang@baodi.com>

#include "flume/planner/task_dispatcher.h"

#include <vector>

#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

TaskDispatcher::TaskDispatcher() : m_type(NONE_ORDER) {
}

TaskDispatcher::TaskDispatcher(Type type) : m_type(type) {
}

bool TaskDispatcher::Run(Plan* plan) {
    std::vector<Unit*> tasks;
    FindAllTasks(plan->Root(), &tasks);
    if (m_type != NONE_ORDER) {
        FindAllTasksByTopology(plan, &tasks);
    }
    bool changed = false;
    for (size_t i = 0; i < tasks.size(); ++i) {
        changed |= Dispatch(plan, tasks[i]);
    }
    return changed;
}

void TaskDispatcher::FindAllTasksByTopology(Plan* plan, std::vector<Unit*>* tasks) {
    std::vector<Unit*> available_tasks;
    DependencySet task_relations;
    AddTaskRelations(plan->Root(), &task_relations);
    std::set<Unit*> in_set;
    std::set<Unit*> out_set;
    while (true) {
        in_set.clear();
        out_set.clear();
        for (DependencySet::iterator it = task_relations.begin();
                    it != task_relations.end(); ++it) {
            Unit* first = it->first;
            Unit* second = it->second;
            std::vector<Unit*>::iterator res = std::find(
                available_tasks.begin(), available_tasks.end(), first);
            if (res == available_tasks.end()) {
                out_set.insert(first);
                in_set.insert(second);
            }
        }
        if (AddTaskByDiff(out_set, in_set, &available_tasks)) {
            break;
        }
    }
    in_set.clear();
    out_set.clear();
    for (DependencySet::iterator it = task_relations.begin();
                it != task_relations.end(); ++it) {
        Unit* first = it->first;
        Unit* second = it->second;
        out_set.insert(first);
        in_set.insert(second);
    }
    AddTaskByDiff(in_set, out_set, &available_tasks);

    std::reverse(available_tasks.begin(), available_tasks.end());
    for (size_t i = 0; i < tasks->size(); ++i) {
        Unit* task = (*tasks)[i];
        std::vector<Unit*>::iterator res = std::find(
            available_tasks.begin(), available_tasks.end(), task);
        if (res == available_tasks.end()) {
            available_tasks.push_back(task);
        }
    }
    tasks->clear();
    tasks->resize(available_tasks.size());
    std::copy(available_tasks.begin(), available_tasks.end(), tasks->begin());

    if (m_type == PRE_TOPOLOGY_ORDER) {
        std::reverse(tasks->begin(), tasks->end());
    }
}

void TaskDispatcher::FindAllTasks(Unit* unit, std::vector<Unit*>* tasks) {
    if (unit->type() == Unit::TASK) {
        tasks->push_back(unit);
    } else {
        for (Unit::iterator ptr = unit->begin(); ptr != unit->end(); ++ptr) {
            FindAllTasks(*ptr, tasks);
        }
    }
}

void TaskDispatcher::AddTaskRelations(Unit* unit, DependencySet* task_relations) {
    if (unit->is_leaf()) {
        for (size_t i = 0; i < unit->direct_users().size(); ++i) {
            Unit* child = unit->direct_users()[i];
            if (unit->task() == child->task()) {
                continue;
            }
            task_relations->insert(std::make_pair(unit->task(), child->task()));
        }
    } else {
        for (Unit::iterator child = unit->begin(); child != unit->end(); ++child) {
            AddTaskRelations(*child, task_relations);
        }
    }
}

bool TaskDispatcher::AddTaskByDiff(const std::set<Unit*>& set_in,
                                    const std::set<Unit*>& set_not_in,
                                    std::vector<Unit*>* tasks) {
    std::set<Unit*> res_set;
    std::set_difference(set_in.begin(), set_in.end(),
                        set_not_in.begin(), set_not_in.end(),
                        std::inserter(res_set, res_set.end()));
    for (std::set<Unit*>::iterator it = res_set.begin(); it != res_set.end(); ++it) {
        tasks->push_back(*it);
    }
    return res_set.size() == 0;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
