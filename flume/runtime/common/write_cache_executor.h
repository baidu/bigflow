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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#ifndef FLUME_RUNTIME_DCE_WRITE_CACHE_EXECUTOR_H_
#define FLUME_RUNTIME_DCE_WRITE_CACHE_EXECUTOR_H_

#include "toft/base/scoped_ptr.h"

#include "flume/runtime/common/cache_manager.h"
#include "flume/runtime/common/executor_base.h"

namespace baidu {
namespace flume {
namespace runtime {

class WriteCacheExecutor : public ExecutorCore {
public:
    WriteCacheExecutor(CacheManager* cache_manager) : m_cache_manager(cache_manager) {}

    virtual void Setup(const PbExecutor& executor, ExecutorBase* base);

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys);

    virtual void EndGroup();

private:
    CacheManager* m_cache_manager;
    CacheManager::Writer* m_writer;
    int m_effective_key_num;
    std::vector<toft::StringPiece> m_keys;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_DCE_WRITE_CACHE_EXECUTOR_H_
