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
// Created by zhangyuncong on 2017/9/5.
//

#ifndef BLADE_SPARK_CACHE_MANAGER_H
#define BLADE_SPARK_CACHE_MANAGER_H


#include <map>
#include <string>
#include <vector>

#include <functional>
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/typeof/typeof.hpp"
#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/base/closure.h"

#include "flume/runtime/common/cache_manager.h"
#include "flume/util/path_util.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class SparkCacheManager : public CacheManager {
public:
    class Iterator;
    class Reader;
    class Writer;

    typedef std::function<void(const toft::StringPiece&, const toft::StringPiece&)> Emitter;

    // Construct an FileCacheManager
    SparkCacheManager(const Emitter& emitter, const flume::PbSparkJob::PbSparkJobInfo& job_message);

    virtual ~SparkCacheManager() {
        Shutdown();
        LOG(INFO) << "SparkCacheManager Destroying...";
    }

    virtual CacheManager::Reader* GetReader(const std::string& node_id);
    virtual CacheManager::Writer* GetWriter(const std::string& node_id);

    virtual void Shutdown();

private:

    std::map<std::string, boost::shared_ptr<CacheManager::Writer> > m_writers;
    std::map<std::string, uint32_t> m_cache_node_to_task_id;
    Emitter m_emitter;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SPARK_CACHE_MANAGER_H_
