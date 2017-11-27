/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)

#ifndef FLUME_RUNTIME_COMMON_CACHE_ITERATOR_H_
#define FLUME_RUNTIME_COMMON_CACHE_ITERATOR_H_

#include <map>
#include <string>
#include <vector>

#include "boost/shared_ptr.hpp"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/runtime/common/cache_manager.h"
#include "flume/runtime/kv_iterator.h"

namespace baidu {
namespace flume {
namespace runtime {

class CacheIterator : public KVIterator {
public:
    class Impl;

    CacheIterator(CacheManager::Reader* cache_reader,
                  const flume::core::Entity<flume::core::Objector>& objector);

    virtual bool Next();

    virtual void Reset();

    virtual void Done();

    virtual const std::vector<toft::StringPiece>& Keys() const;

    virtual void* Value() const;

    virtual toft::StringPiece ValueStr() const;

private:
    boost::shared_ptr<Impl> m_impl;
};


}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_CACHE_ITERATOR_H_
