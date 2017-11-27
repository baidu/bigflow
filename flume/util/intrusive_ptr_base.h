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
// Author: Liu Cheng <liucheng02@baidu.com>
//
// IntrusivePtrBase is base class of intrusive ptr classes; it mainly implements
// intrusive_ptr_add_ref and intrusive_ptr_release;
// Copy and assign are disallowed

#ifndef  FLUME_UTIL_INTRUSIVE_PTR_BASE_H_
#define  FLUME_UTIL_INTRUSIVE_PTR_BASE_H_

#include "boost/checked_delete.hpp"
#include "boost/detail/atomic_count.hpp"
#include "boost/intrusive_ptr.hpp"
#include "glog/logging.h"

#include "toft/base/uncopyable.h"

#include "flume/util/reusable_object_pool.h"

namespace baidu {
namespace flume {
namespace util {

// Construct IntrusivePtrBase with a deleter allows releasing objects thread-safely
template<class T>
class IntrusivePtrBase : public Reusable {
public:
    IntrusivePtrBase() : m_ref_count(0) {}

    virtual void Reuse() {
        boost::checked_delete(static_cast<T*>(this));
    }

    friend void intrusive_ptr_add_ref(IntrusivePtrBase<T>* s) {
        CHECK(NULL != s);
        CHECK_LE(0, s->m_ref_count);
        ++s->m_ref_count;
    }

    friend void intrusive_ptr_release(IntrusivePtrBase<T>* s) {
        CHECK_LT(0, s->m_ref_count);
        CHECK(NULL != s);
        --s->m_ref_count;
        if (s->m_ref_count == 0) {
            s->Reuse();
        }
    }

    boost::intrusive_ptr<T> self() {
        return boost::intrusive_ptr<T>(static_cast<T*>(this));
    }

    boost::intrusive_ptr<const T> self() const {
        return boost::intrusive_ptr<const T>(static_cast<const T *>(this));
    }

    int ref_count() const {
        return m_ref_count;
    }

private:
    // should be modifiable even from const intrusive_ptr objects
    mutable boost::detail::atomic_count m_ref_count;
    TOFT_DECLARE_UNCOPYABLE(IntrusivePtrBase);
};

}  // namespace util
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_UTIL_INTRUSIVE_PTR_BASE_H_

