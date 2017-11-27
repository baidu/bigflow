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

#ifndef FLUME_PLANNER_UNIT_H_
#define FLUME_PLANNER_UNIT_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/operators.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "glog/logging.h"
#include "toft/base/unordered_set.h"

namespace baidu {
namespace flume {
namespace planner {

class Plan;
class DcePlannerImpl;

class Unit {
public:
    friend class Plan;

    enum Type {
        // no type
        GENERAL = 0,

        // top units
        PLAN,
        JOB,

        TASK, // executors below, TASK should always be the first executor type
        SCOPE,  // no specific executor
        EXTERNAL_EXECUTOR, // backend specific executor
        LOGICAL_EXECUTOR, // run user fn for logical plan node
        SHUFFLE_EXECUTOR,  // do scope-speicific staffs
        CACHE_WRITER,
        CACHE_READER,
        PARTIAL_EXECUTOR, // execute within fixed buffer
        CREATE_EMPTY_RECORD_EXECUTOR,
        FILTER_EMPTY_RECORD_EXECUTOR,

        STREAM_EXTERNAL_EXECUTOR,
        STREAM_LOGICAL_EXECUTOR,
        STREAM_SHUFFLE_EXECUTOR,

        DUMMY, // leafs below, DUMMY should always be the first leaf type
        CHANNEL,
        LOAD_NODE,
        SINK_NODE,
        PROCESS_NODE,
        UNION_NODE,
        SHUFFLE_NODE,

        EMPTY_GROUP_FIXER,

        TYPE_COUNT // indicated the count of the Unit::Type
    };

    typedef std::unordered_set<Unit*> Children;
    typedef Children::const_iterator iterator;

    Unit(Plan* plan, bool is_leaf);

    virtual ~Unit();

    Plan* plan();

    // clone node, not include it's controlflow and dataflow.
    Unit* clone();

    // get the task which the unit belonged to. return NULL if there is no task.
    Unit* task();

    bool is_leaf() const;

    bool is_discard() const;

    Type type() const;
    void set_type(Type type);
    const char* type_string() const ;

    // ancestor
    Unit* father() const;
    bool is_descendant_of(Unit* ancestor);

    // children accessor
    bool empty() const;
    std::size_t size() const;
    iterator begin() const;
    iterator end() const;

    // source identity for leaf unit
    std::string identity() const;
    void set_identity(const std::string& identity);
    std::string change_identity();

    std::vector<Unit*> children() const;
    std::vector<Unit*> direct_users() const;
    std::vector<Unit*> direct_needs() const;

    template<typename T>
    bool has() const;

    // add attribute T with default value
    template<typename T>
    void set();

    template<typename T>
    void set(const T& value);

    template<typename T>
    T& get();

    template<typename T>
    const T& get() const;

    template<typename T>
    T move();

    template<typename T>
    void clear();

private:
    struct BaseInfo {
        virtual ~BaseInfo() {}
        virtual BaseInfo* clone() { return new BaseInfo(); }
    };

    template<typename T>
    struct InfoProxy : public BaseInfo {
        static const int ID;
        T info;

        virtual BaseInfo* clone() {
            InfoProxy* proxy = new InfoProxy<T>();
            proxy->info = this->info;
            return proxy;
        }
    };

    typedef boost::ptr_map<const void*, BaseInfo> InfoMap;

    const bool m_is_leaf;
    bool m_is_discard;
    Type m_type;
    Unit* m_father;
    Children m_childs;
    std::string m_identity;
    InfoMap m_infos;
    Plan* m_plan;
};

template<typename T>
const int Unit::InfoProxy<T>::ID = 0;

template<typename T>
bool Unit::has() const {
    const void* id = &InfoProxy<T>::ID;
    return m_infos.find(id) != m_infos.end();
}

template<typename T>
void Unit::set() {
    if (!has<T>()) {
        const void* id = &InfoProxy<T>::ID;
        m_infos.insert(id, new InfoProxy<T>());
    }
}

template<typename T>
void Unit::set(const T& value) {
    this->get<T>() = value;
}

template<typename T>
T& Unit::get() {
    set<T>();  // NOLINT
    const T& info = static_cast<const Unit*>(this)->get<T>();
    return const_cast<T&>(info);
}

template<typename T>
const T& Unit::get() const {
    const void* id = &InfoProxy<T>::ID;
    InfoMap::const_iterator it = m_infos.find(id);
    CHECK(it != m_infos.end());
    const InfoProxy<T>* proxy = static_cast<const InfoProxy<T>*>(it->second);
    return proxy->info;
}

template<typename T>
void Unit::clear() {
    const void* id = &InfoProxy<T>::ID;
    m_infos.erase(id);
}

template<typename T>
T Unit::move() {
    T result = get<T>();
    clear<T>();
    return result;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_UNIT_H_
