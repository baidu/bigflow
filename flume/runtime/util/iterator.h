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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Interface.

#ifndef FLUME_RUNTIME_UTIL_ITERATOR_H_
#define FLUME_RUNTIME_UTIL_ITERATOR_H_

#include "toft/base/string/string_piece.h"
#include "flume/util/reusable_object_pool.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace internal {

class Iterator {
public:
    virtual ~Iterator() {}

    virtual bool HasNext() const = 0;
    virtual toft::StringPiece NextValue() = 0;
    virtual void Reset() = 0;
    virtual void Done() = 0;
};

template<typename ContainerType>
class StlIterator : public Iterator {
public:
    typedef typename ContainerType::const_iterator PtrType;

    explicit StlIterator(const ContainerType& arg)
            : m_begin(arg.begin()), m_end(arg.end()), m_ptr(m_begin) {}

    virtual bool HasNext() const {
        return m_ptr != m_end;
    }

    virtual toft::StringPiece NextValue() {
        return toft::StringPiece(*m_ptr++);
    }

    virtual void Reset() {
        m_ptr = m_end;
    }

    virtual void Done() {
        m_begin = m_end;
    }

private:
    PtrType m_begin;
    PtrType m_end;
    PtrType m_ptr;
};

class ReusableIterator : public Iterator, public util::Reusable {};

}  // namespace internal
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_UTIL_ITERATOR_H_
