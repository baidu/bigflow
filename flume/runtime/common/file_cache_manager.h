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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)

#ifndef FLUME_RUNTIME_COMMON_FILE_CACHE_MANAGER_H_
#define FLUME_RUNTIME_COMMON_FILE_CACHE_MANAGER_H_

#include <map>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/typeof/typeof.hpp"
#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/common/cache_manager.h"
#include "flume/util/path_util.h"

namespace baidu {
namespace flume {
namespace runtime {

class FileCacheManager : public CacheManager {
public:
    class Iterator;
    class Reader;
    class Writer;

    // Construct an FileCacheManager
    // @ param [in] tmp_data_path : tmp data path config
    // @ param [in] task_id : if you will use writer, this param is needed
    //                        if you will only use reader, this param will be ignored
    FileCacheManager(const std::string& tmp_data_path,
                     const std::string& task_id)
                     : m_tmp_data_path(tmp_data_path),
                     m_task_id(task_id) {
        LOG(INFO) << "FileCacheManager Construct with tmp_data_path = "
                  << flume::util::HideToftPathPassword(tmp_data_path)
                  << ", task_id = "
                  << task_id;
    }

    virtual ~FileCacheManager() {
        Shutdown();
        LOG(INFO) << "FileCacheManager Destroying...";
    }

    static std::string GetCachedNodePath(const std::string& tmp_data_path, const std::string& node_id) {
        return tmp_data_path + "/" + node_id;
    }

    virtual CacheManager::Reader* GetReader(const std::string& node_id);
    virtual CacheManager::Writer* GetWriter(const std::string& node_id);

    virtual void Shutdown();

private:
    std::string m_tmp_data_path;
    std::string m_task_id;

    std::map<std::string, boost::shared_ptr<CacheManager::Reader> > m_readers;
    std::map<std::string, boost::shared_ptr<CacheManager::Writer> > m_writers;

};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_FILE_CACHE_MANAGER_H_
