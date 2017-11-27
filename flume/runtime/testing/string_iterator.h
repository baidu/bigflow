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
//
// Interface.

#ifndef FLUME_RUNTIME_TESTING_STRING_ITERATOR_H_
#define FLUME_RUNTIME_TESTING_STRING_ITERATOR_H_

#include <list>
#include <string>

#include "boost/ptr_container/ptr_vector.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/closure.h"
#include "toft/base/scoped_array.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"

namespace baidu {
namespace flume {
namespace runtime {

class StringIterator : public core::Iterator {
public:
    StringIterator() : m_ptr(m_records.end()) {}

    StringIterator& Add(const std::string& record) {
        m_records.push_back(record);
        m_ptr = m_records.begin();
        return *this;
    }

    virtual bool HasNext() const {
        return m_ptr != m_records.end();
    }

    virtual void* NextValue() {
        return &(*m_ptr++);
    }

    virtual void Reset() {
        m_ptr = m_records.begin();
    }

    virtual void Done() {
        IteratorDone();
    }

    MOCK_METHOD0(IteratorDone, void());

private:
    std::list<std::string> m_records;
    std::list<std::string>::iterator m_ptr;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_STRING_ITERATOR_H_
