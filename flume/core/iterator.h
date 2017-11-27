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
// Interface to iterate a collection of records.

#ifndef FLUME_CORE_ITERATOR_H_
#define FLUME_CORE_ITERATOR_H_

namespace baidu {
namespace flume {
namespace core {

class Iterator {
public:
    virtual ~Iterator() {}

    virtual bool HasNext() const = 0;
    virtual void* NextValue() = 0;
    virtual void Reset() = 0;
    virtual void Done() = 0;
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_ITERATOR_H_
