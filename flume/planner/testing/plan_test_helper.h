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
// Author: Zhang Yuncong <zhangyuncong@baidu>
//         Pan Yuchang(BDG)<panyuchang@baidu.com>
// Description:

#ifndef FLUME_PLANNER_TESTING_PLAN_TEST_HELPER_H_
#define FLUME_PLANNER_TESTING_PLAN_TEST_HELPER_H_

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "boost/make_shared.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/shared_ptr.hpp"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "flume/core/logical_plan.h"
#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/load_logical_plan_pass.h"
#include "flume/planner/pass.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_draw_helper.h"
#include "flume/planner/unit.h"
#include "flume/runtime/session.h"
#include "flume/util/reflection.h"

#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace flume {
namespace planner {

namespace filter {

class IFilter {
public:
    virtual bool Filte(Unit* unit) const = 0;
    virtual ~IFilter() {}
};

#define FLUME_FILTER baidu::flume::planner::filter
#define FLUME_REGISTER_FILTER(FilterName) \
template<typename Param>\
FLUME_FILTER::Filter FilterName(const Param& param) {\
    return FLUME_FILTER::make_filter<FilterName##Filter>(param);\
}\
template<typename Param1, typename Param2>\
FLUME_FILTER::Filter FilterName(const Param1& param1, const Param2& param2) {\
    return FLUME_FILTER::make_filter<FilterName##Filter>(param1, param2);\
}

#define FLUME_REGISTER_EQUAL_FILTER(FilterName, ParamType, EqualFilterName) \
static struct FilterName##FilterGenerator {\
public:\
} EqualFilterName;\
inline FLUME_FILTER::Filter operator==(FilterName##FilterGenerator gen, ParamType param) {\
    EqualFilterName = gen;\
    return FilterName(param);\
}\
inline FLUME_FILTER::Filter operator==(ParamType param, FilterName##FilterGenerator gen) {\
    EqualFilterName = gen;\
    return FilterName(param);\
}\
inline FLUME_FILTER::Filter operator!=(FilterName##FilterGenerator gen, ParamType param) {\
    EqualFilterName = gen;\
    return !FilterName(param);\
}\
inline FLUME_FILTER::Filter operator!=(ParamType param, FilterName##FilterGenerator gen) {\
    EqualFilterName = gen;\
    return !FilterName(param);\
}

class Filter {
public:
    explicit Filter(boost::shared_ptr<IFilter> impl) : m_impl(impl) {}
    bool Filte(Unit* unit) const {
        CHECK_NOTNULL(m_impl.get());
        return m_impl->Filte(unit);
    }
private:
    boost::shared_ptr<IFilter> m_impl;
};

template<typename IFilterType>
Filter make_filter() {
    return Filter(boost::make_shared<IFilterType>());
}

template<typename IFilterType, typename Param>
Filter make_filter(Param param) {
    return Filter(boost::make_shared<IFilterType>(param));
}

template<typename IFilterType, typename Param1, typename Param2>
Filter make_filter(Param1 param1, Param2 param2) {
    return Filter(boost::make_shared<IFilterType>(param1, param2));
}

class InvalidFilter : public IFilter {
public:
    bool Filte(Unit* unit) const {
        CHECK(0) << "Invalid Filter";
        return false;
    }
};

class TypeIsFilter : public IFilter {
public:
    explicit TypeIsFilter(Unit::Type type) : m_type(type) {
    }
    bool Filte(Unit* unit) const {
        return unit->type() == m_type;
    }
private:
    Unit::Type m_type;
};

class AndFilter : public IFilter {
public:
    AndFilter(const Filter& f1, const Filter& f2)
            :m_filter_1(f1), m_filter_2(f2) {
    }
    bool Filte(Unit* unit) const {
        return m_filter_1.Filte(unit) && m_filter_2.Filte(unit);
    }
private:
    Filter m_filter_1;
    Filter m_filter_2;
};

class OrFilter : public IFilter {
public:
    OrFilter(const Filter& f1, const Filter& f2)
            :m_filter_1(f1), m_filter_2(f2) {
    }
    bool Filte(Unit* unit) const {
        return m_filter_1.Filte(unit) || m_filter_2.Filte(unit);
    }
private:
    Filter m_filter_1;
    Filter m_filter_2;
};

class NotFilter : public IFilter {
public:
    explicit NotFilter(const Filter& filter)
            :m_filter(filter) {
    }
    bool Filte(Unit* unit) const {
        return !m_filter.Filte(unit);
    }
private:
    Filter m_filter;
};


class FatherIsFilter : public IFilter {
public:
    explicit FatherIsFilter(Unit* father) : m_father(father) {
    }

    bool Filte(Unit* unit) const {
        return unit->father() == m_father;
    }
private:
    Unit* m_father;
};

class IdIsFilter : public IFilter {
public:
    explicit IdIsFilter(std::string id) : m_id(id) {}

    bool Filte(Unit* unit) const {
        return unit->identity() == m_id;
    }
private:
    std::string m_id;
};

class HasNeedFilter : public IFilter {
public:
    explicit HasNeedFilter(Unit* need) : m_need(need) {}

    bool Filte(Unit* unit) const {
        std::vector<Unit*> needs = unit->direct_needs();
        return std::find(needs.begin(), needs.end(), m_need) != needs.end();
    }
private:
    Unit* m_need;
};

class HasUserFilter : public IFilter {
public:
    explicit HasUserFilter(Unit* user) : m_user(user) {}

    bool Filte(Unit* unit) const {
        std::vector<Unit*> users = unit->direct_users();
        return std::find(users.begin(), users.end(), m_user) != users.end();
    }
private:
    Unit* m_user;
};

class UserSizeIsFilter : public IFilter {
public:
    explicit UserSizeIsFilter(uint32_t size) : m_size(size) {}
    bool Filte(Unit* unit) const {
        return unit->direct_users().size() == m_size;
    }
private:
    uint32_t m_size;
};

class NeedSizeIsFilter : public IFilter {
public:
    explicit NeedSizeIsFilter(uint32_t size) : m_size(size) {}
    bool Filte(Unit* unit) const {
        return unit->direct_needs().size() == m_size;
    }
private:
    uint32_t m_size;
};

class ChildSizeIsFilter : public IFilter {
public:
    explicit ChildSizeIsFilter(uint32_t size) : m_size(size) {}
    bool Filte(Unit* unit) const {
        return unit->size() == m_size;
    }
private:
    uint32_t m_size;
};

class TaskIsFilter : public IFilter {
public:
    explicit TaskIsFilter(Unit* task): m_task(task) {}
    virtual bool Filte(Unit* unit) const {
        return unit->task() == m_task;
    }
private:
    Unit* m_task;
};

class IsLeafFilter : public IFilter {
public:
    bool Filte(Unit* unit) const {
        return unit->is_leaf();
    }
};

FLUME_REGISTER_FILTER(HasUser);
FLUME_REGISTER_FILTER(HasNeed);
FLUME_REGISTER_FILTER(And);
FLUME_REGISTER_FILTER(Or);
FLUME_REGISTER_FILTER(Not);

inline Filter operator&& (const Filter& f1, const Filter& f2) {
    return And(f1, f2);
}

inline Filter operator|| (const Filter& f1, const Filter& f2) {
    return Or(f1, f2);
}

inline Filter operator! (const Filter& f1) {  // NOLINT
    return Not(f1);
}

FLUME_REGISTER_FILTER(TypeIs);
FLUME_REGISTER_EQUAL_FILTER(TypeIs, Unit::Type, type);
FLUME_REGISTER_FILTER(TaskIs);
FLUME_REGISTER_EQUAL_FILTER(TaskIs, Unit*, task);
FLUME_REGISTER_FILTER(IdIs);
FLUME_REGISTER_EQUAL_FILTER(IdIs, std::string, id);
FLUME_REGISTER_FILTER(FatherIs);
FLUME_REGISTER_EQUAL_FILTER(FatherIs, Unit*, father);
FLUME_REGISTER_FILTER(UserSizeIs);
FLUME_REGISTER_EQUAL_FILTER(UserSizeIs, uint32_t, user_size);
FLUME_REGISTER_FILTER(NeedSizeIs);
FLUME_REGISTER_EQUAL_FILTER(NeedSizeIs, uint32_t, need_size);
FLUME_REGISTER_FILTER(ChildSizeIs);
FLUME_REGISTER_EQUAL_FILTER(ChildSizeIs, uint32_t, child_size);

static Filter invalid = make_filter<InvalidFilter>();
static Filter is_leaf = make_filter<IsLeafFilter>();


}  // namespace filter

inline std::multiset<std::string> AsSet(const core::LogicalPlan::Node* s1,
                                        const core::LogicalPlan::Node* s2 = NULL,
                                        const core::LogicalPlan::Node* s3 = NULL,
                                        const core::LogicalPlan::Node* s4 = NULL) {
    std::multiset<std::string> results;

    const core::LogicalPlan::Node* params[] = {s1, s2, s3, s4};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            results.insert(params[i]->identity());
        }
    }

    return results;
}

inline std::multiset<std::string> AsSet(const Unit* s1,
                                        const Unit* s2 = NULL,
                                        const Unit* s3 = NULL,
                                        const Unit* s4 = NULL) {
    std::multiset<std::string> results;

    const Unit* params[] = {s1, s2, s3, s4};
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(params); ++i) {
        if (params[i] != NULL) {
            results.insert(params[i]->identity());
        }
    }

    return results;
}

static std::multiset<std::string> kEmptySet;

class PlanVisitor {
public:
    explicit PlanVisitor(Plan* plan) : m_plan(plan) {
        m_units.insert(plan->Root());
    }

    PlanVisitor(Plan* plan, Unit* unit) : m_plan(plan) {
        if (unit) {
            m_units.insert(unit);
        }
    }

    PlanVisitor(Plan* plan, std::set<Unit*> unit) : m_plan(plan), m_units(unit) {}

    Unit* unit() const { return *m_units.begin(); }
    const std::set<Unit*>& all_units() { return m_units; }

    std::vector<PlanVisitor> all() {
        std::set<Unit*>::iterator it = m_units.begin();
        std::vector<PlanVisitor> result;
        for (; it != m_units.end(); ++it) {
            result.push_back(PlanVisitor(m_plan, *it));
        }
        return result;
    }

    Plan* plan() const { return m_plan; }

    Unit* operator->() const {
        return *m_units.begin();
    }

    PlanVisitor operator[](const std::string& identity) const {
        std::vector<Unit*> children = (*m_units.begin())->children();
        for (size_t i = 0; i < children.size(); ++i) {
            if (children[i]->identity() == identity) {
                return PlanVisitor(m_plan, children[i]);
            }
        }

        LOG(INFO) << "Find no child has identity: " << identity;
        return PlanVisitor(m_plan, NULL);
    }

    std::multiset<std::string> user_set() const {
        std::multiset<std::string> results;

        std::vector<Unit*> users = (*m_units.begin())->direct_users();
        for (size_t i = 0; i < users.size(); ++i) {
            results.insert(users[i]->identity());
        }

        return results;
    }

    std::multiset<std::string> need_set() const {
        std::multiset<std::string> results;

        std::vector<Unit*> needs = (*m_units.begin())->direct_needs();
        for (size_t i = 0; i < needs.size(); ++i) {
            results.insert(needs[i]->identity());
        }

        return results;
    }

    std::multiset<std::string> child_set() const {
        std::multiset<std::string> results;

        std::vector<Unit*> needs = (*m_units.begin())->children();
        for (size_t i = 0; i < needs.size(); ++i) {
            results.insert(needs[i]->identity());
        }

        return results;
    }

    // find all units in all the successors of this visitor
    // very high costs
    PlanVisitor Find(const filter::Filter& where) const {
        PlanVisitor visitor(m_plan, NULL);
        std::set<Unit*>::iterator it = m_units.begin();
        for (; it != m_units.end(); ++it) {
            std::set<Unit*> children((*it)->begin(), (*it)->end());
            std::set<Unit*>::iterator child_iter = children.begin();
            for (; child_iter != children.end(); ++child_iter) {
                if (where.Filte(*child_iter)) {
                    visitor.m_units.insert(*child_iter);
                }
            }
            std::set<Unit*> find_in_child
                = PlanVisitor(m_plan, children).Find(where).m_units;
            std::copy(
                find_in_child.begin(),
                find_in_child.end(),
                std::insert_iterator<std::set<Unit*> >(
                    visitor.m_units,
                    visitor.m_units.begin()
                )
            );
        }
        return visitor;
    }

    // Find all node in all successors, very high costs
    PlanVisitor Find(const std::string& identity) const {
        return Find(filter::IdIs(identity));
    }

    PlanVisitor Find(const core::LogicalPlan::Node* node) const {
        CHECK_NOTNULL(node);
        return (*this).Find(node->identity());
    }

    PlanVisitor Find(const Unit* node) const {
        CHECK_NOTNULL(node);
        return (*this).Find(node->identity());
    }

    PlanVisitor Find(const core::LogicalPlan::Scope* scope) const {
        CHECK_NOTNULL(scope);
        return (*this).Find(scope->identity());
    }

    // use the object as an condition.
    // eg.
    //     PlanVisitor visitor;
    //     if (visitor.Find(node)) { ... }
    operator void*() const {
        return m_units.size() ? (*m_units.begin()) : NULL;
    }

    PlanVisitor operator[](const core::LogicalPlan::Node* node) const {
        return (*this)[node->identity()];
    }

    PlanVisitor operator[](const Unit* node) const {
        return (*this)[node->identity()];
    }

    PlanVisitor operator[](const core::LogicalPlan::Scope* scope) const {
        return (*this)[scope->identity()];
    }

private:
    Plan* m_plan;
    std::set<Unit*> m_units;
};

class PlanTest : public ::testing::Test {
public:
    typedef DataFlowAnalysis::Info Info;

    PlanTest();

    template<class PassType>
    bool ApplyPass();

    void InitFromLogicalPlan(core::LogicalPlan* logical_plan);
    void AddTasksOf(int32_t task_number);
    PlanVisitor GetPlanVisitor();
    Plan* GetPlan();
    Unit* GetPlanRoot();
    Unit* GetTask(int index);
    Unit* AddLeafBelowTask(int task_index, std::string leaf_name, Unit::Type type);
    Unit* AddLeaf(Unit* father, std::string leaf_name, Unit::Type type);
    Unit* AddNodeBelowTask(int task_index, std::string node_name, Unit::Type type);
    Unit* AddNode(Unit* father, std::string node_name, Unit::Type type);
    Unit* AddNodeBelowTask(int task_index, std::string node_name, Unit::Type type, bool is_leaf);
    Unit* AddNode(Unit* father, std::string node_name, Unit::Type type, bool is_leaf);
    void SetProcess(Unit* unit);
    void SetProcess(Unit* unit, const std::vector<std::string>& froms);
    void SetShuffle(Unit* unit, std::string from);
    void AddInput(Unit* input, Unit* to, const std::string& id, bool is_partial);
    bool IsNodeContain(Unit* father, Unit* node);
    std::set<std::string> GetFromIds(Unit* unit, Unit::Type type);
    std::vector<Unit*> GetTypeNodes(Unit* father, Unit::Type type);
    void AddDependency(Unit* u1, Unit* u2);

private:
    void AddDefaultLogicalPlanNode(Unit* unit, Unit::Type type);
    void UpdateInputOutput(Info::DependencyArray* depends_array, Unit* u1, Unit* u2);

private:
    bool m_is_first_draw;
    std::string m_dir_path;
    toft::scoped_ptr<PlanDraw> m_drawer;

    core::LogicalPlan m_logical_plan;
    toft::scoped_ptr<Plan> m_plan;
    std::vector<Unit*> m_tasks;
    runtime::Session m_session;
};

template<class PassType>
bool PlanTest::ApplyPass() {
    Plan* plan = m_plan.get();
    if (m_is_first_draw) {
        m_drawer->Draw(plan);
        m_is_first_draw = false;
    }
    std::string pass_name = flume::Reflection<Pass>::TypeName<PassType>();
    toft::scoped_ptr<Pass> deleter;
    deleter.reset(flume::Reflection<Pass>::New(pass_name));
    Pass* pass = deleter.get();
    LOG(INFO) << "Applying pass " << pass_name;
    bool flag = pass->Run(plan);
    m_drawer->Draw(plan);
    return flag;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_TESTING_PLAN_TEST_HELPER_H_
