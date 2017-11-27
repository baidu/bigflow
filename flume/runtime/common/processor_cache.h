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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//
// ProcessorCache is to minimize the new and delete operations of all kinds of Processors.

#ifndef FLUME_RUNTIME_COMMON_PROCESSOR_CACHE_H_
#define FLUME_RUNTIME_COMMON_PROCESSOR_CACHE_H_

#include <string>
#include <vector>

#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/processor.h"
#include "flume/runtime/util/iterator.h"
#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

class ProcessorCache {
public:
    class Instance;

    ProcessorCache();

    virtual ~ProcessorCache() {}

    virtual void Initialize(uint32_t capacity);

    virtual Instance* Lookup(const std::string& id,
                             const std::vector<toft::StringPiece>& keys);

    virtual Instance* Create(const std::string& id, const std::vector<toft::StringPiece>& keys,
                             const core::Entity<core::Processor>& entity);
};

class ProcessorCache::Instance {
public:
    virtual ~Instance() {}

    virtual core::Processor* GetProcessor(core::Emitter* emitter) = 0;

    virtual void DeleteProcessor() = 0;

    virtual void Release() = 0;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_PROCESSOR_CACHE_H_
