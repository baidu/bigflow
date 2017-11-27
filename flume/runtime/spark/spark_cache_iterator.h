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

//
// Created by zhangyuncong on 2017/9/19.
//

#ifndef BLADE_FLUME_RUNTIME_SPARK_SPARK_CACHE_ITERATOR_H
#define BLADE_FLUME_RUNTIME_SPARK_SPARK_CACHE_ITERATOR_H

#include "boost/shared_ptr.hpp"
#include "flume/runtime/kv_iterator.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class CacheIterator : public KVIterator {
public:
    class Impl;

    CacheIterator();

    virtual bool Next();

    virtual void Reset();

    virtual void Done();

    virtual const std::vector<toft::StringPiece>& Keys() const;

    virtual void* Value() const;

    virtual toft::StringPiece ValueStr() const;

    virtual void Put(const std::vector<std::string>& keys, const std::string& value);
private:
    boost::shared_ptr<Impl> m_impl;
};


}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu


#endif //BLADE_FLUME_RUNTIME_SPARK_SPARK_CACHE_ITERATOR_H
