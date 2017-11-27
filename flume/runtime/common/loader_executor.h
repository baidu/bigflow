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
// Author: Wen Xiang <wenxiang@baidu.com>

#ifndef FLUME_RUNTIME_COMMON_LOADER_EXECUTOR_H_
#define FLUME_RUNTIME_COMMON_LOADER_EXECUTOR_H_

#include <map>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/string/string_piece.h"

#include "flume/core/loader.h"
#include "flume/core/objector.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

class LoaderExecutor : public Executor, public core::Emitter {
public:
    LoaderExecutor();
    virtual ~LoaderExecutor();

    void Initialize(const PbExecutor& message, DatasetManager* dataset_manager);

    // implements Executor
    virtual void Setup(const std::map<std::string, Source*>& sources);
    virtual Source* GetSource(const std::string& id, unsigned scope_level);
    virtual void BeginGroup(const toft::StringPiece& key);
    virtual void FinishGroup();

    // implements Emitter
    virtual bool Emit(void *object);
    virtual void Done();

private:
    PbExecutor m_message;
    boost::scoped_ptr<internal::DispatcherManager> m_dispatcher_manager;
    Dispatcher* m_output;
    boost::scoped_ptr<core::Loader> m_loader;

    std::vector<toft::StringPiece> m_keys;
    bool m_is_done;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_LOADER_EXECUTOR_H_
