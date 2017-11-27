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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Description: A utils class to testify whether a raw pointer is an instance of target
// type.

#ifndef FLUME_UTIL_TYPE_GUARD_H
#define FLUME_UTIL_TYPE_GUARD_H

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace util {

// In flume, data structures are passed between executors as void*. However, we loss all
// type informations in compilation, which is error prone. TypeGuard is used to make sure
// we get right type from raw pointer(void*);
//
//      class A : public TypeGuard<A> {
//          // ...
//      };
//
//      A a;
//      void* ptr = a.ToPointer();
//      A* a_ = A::Cast(ptr);

template<typename T>
class TypeGuard {
public:
    static T* Cast(void* ptr) {
        T* result = static_cast<T*>(ptr);
        DCHECK_EQ(result->m_magic, &s_type_identity) << "Given pointer has wrong type: " << ptr;
        return result;
    }

public:
    TypeGuard() : m_magic(&s_type_identity) {}

    void* ToPointer() const {
        return const_cast<T*>(static_cast<const T*>(this));
    }

private:
    static int s_type_identity;

private:
    const void* const m_magic /*= &s_type_identity*/;
};

template<typename T>
int TypeGuard<T>::s_type_identity = 0;

} // namespace util
} // namespace flume
} // namespace baidu

#endif  // FLUME_UTIL_TYPE_GUARD_H
