/***************************************************************************
 *
 * Copyright (c) 4 Baidu, Inc. All Rights Reserved.
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

#ifndef FLUME_PLANNER_COMMON_UTIL_H_
#define FLUME_PLANNER_COMMON_UTIL_H_

#include <iterator>

#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

// a helper class for tag behaves like a pointer
// example:
//
//      struct Origin : public Pointer<Unit> {};
//
template<typename T>
class Pointer {
public:
    Pointer() : m_ptr(NULL) {}

    void Assign(T* arg) {
        this->m_ptr = arg;
    }

    T* get() const {
        CHECK_NOTNULL(m_ptr);
        return m_ptr;
    }

    operator T*() {
        return get();
    }

    bool operator==(const Pointer& arg) const {
        return this->m_ptr == arg.m_ptr;
    }

    bool operator==(T* arg) const {
        return this->m_ptr == arg;
    }

    T& operator*() const {
        return *m_ptr;
    }

    T* operator->() const {
        return get();
    }

    bool is_null() const {
        return m_ptr == NULL;
    }

private:
    T* m_ptr;
};

// a helper class for tag behaves like a number
// example:
//
//      struct Count : public Value<int> {};
//
template<typename T>
class Value {
public:
    Value() : m_value(T()) {}

    T& operator*() {
        return m_value;
    }

    T operator*() const {
        return m_value;
    }

    bool operator==(const T& other) {
        return m_value == other.m_value;
    }

private:
    T m_value;
};

template<typename Container>
std::insert_iterator<Container> Inserter(Container& container) {  // NOLINT(runtime/references)
    return std::insert_iterator<Container>(container, container.end());
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_COMMON_UTIL_H_

