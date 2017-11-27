/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

#ifndef BIGFLOW_PYTHON_COMMON_ITERATOR_H
#define BIGFLOW_PYTHON_COMMON_ITERATOR_H

#include <memory>
#include "flume/core/iterator.h"

namespace baidu {
namespace bigflow {
namespace python {

// Convert a pair of std forward iterators [beginIter, endIter) to a Flume iterator
template<typename StdForwardIterator>
class Iterator : public flume::core::Iterator {
public:
    Iterator(StdForwardIterator begin, StdForwardIterator end)
            : _cur(begin), _begin(begin), _end(end) {
    }

    virtual bool HasNext() const {
        return _cur != _end;
    }

    virtual void* NextValue() {
        return &*_cur++;
    }

    virtual void Reset() {
        _cur = _begin;
    }

    virtual void Done() { }
private:
    StdForwardIterator _cur;
    StdForwardIterator _begin;
    StdForwardIterator _end;
};

template<typename Iter>
std::auto_ptr<flume::core::Iterator> iterator(Iter begin, Iter end) {
    return std::auto_ptr<flume::core::Iterator>(new Iterator<Iter>(begin, end));
}

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif // #ifndef BIGFLOW_PYTHON_COMMON_ITERATOR_H
