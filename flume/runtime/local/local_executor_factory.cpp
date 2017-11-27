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

#include "flume/runtime/local/local_executor_factory.h"

#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/core/loader.h"
#include "flume/runtime/common/write_cache_executor.h"
#include "flume/runtime/local/local_input_executor.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace local {

LocalExecutorFactory::LocalExecutorFactory(Backend* backend)
    : m_dataset_manager(NULL),
    m_backend(backend) {}

void LocalExecutorFactory::Initialize(const PbLocalJob& message,
                                      DatasetManager* dataset_manager) {
    for (int i = 0; i < message.input_size(); ++i) {
        m_inputs[message.input(i).id()] = message.input(i);
    }
    m_dataset_manager = dataset_manager;
}

Executor* LocalExecutorFactory::CreateExecutor(const PbExecutor& message, uint32_t scope_level,
                                               const std::vector<Executor*>& childs) {
    if (message.type() == PbExecutor::EXTERNAL) {
        const PbExternalExecutor& external = message.external_executor();
        CHECK_EQ(1u, m_inputs.count(external.id()));

        toft::scoped_ptr<LocalInputExecutor> executor(new LocalInputExecutor());
        executor->Initialize(m_inputs[external.id()], message, childs);
        return executor.release();
    }

    if (message.type() == PbExecutor::WRITE_CACHE) {
        CHECK_NOTNULL(m_backend);
        CacheManager* cache_manager = m_backend->GetCacheManager();
        toft::scoped_ptr<WriteCacheExecutor> executor_core(new WriteCacheExecutor(cache_manager));
        CHECK(message.has_write_cache_executor());

        typedef ExecutorImpl<WriteCacheExecutor> Materlizer;
        toft::scoped_ptr<Materlizer> executor(new Materlizer(executor_core.release()));
        executor->Initialize(message, childs, scope_level, m_dataset_manager);
        return executor.release();
    }

    return ExecutorFactory::CreateCommonExecutor(message, scope_level, childs, m_dataset_manager);
}

}  // namespace local
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
