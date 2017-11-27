/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/runtime/io/compression_file_factory.h"

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace util {

static pthread_rwlock_t s_rwlock = PTHREAD_RWLOCK_INITIALIZER;

Guard::Guard() {
    pthread_rwlock_wrlock(&s_rwlock);
}

Guard::~Guard() {
    pthread_rwlock_unlock(&s_rwlock);
}

}  // namespace util

CompressionFileFactory::Registry& CompressionFileFactory::GetRegistry() {
    static CompressionFileFactory::Registry s_registry;
    return s_registry;
}

std::vector<std::string> CompressionFileFactory::GetCompressions() {
    util::Guard guard;
    const CompressionFileFactory::Registry& registry = GetRegistry();
    typedef CompressionFileFactory::Registry::const_iterator Iterator;

    std::vector<std::string> result;
    for (Iterator iter = registry.begin(); iter != registry.end(); ++iter) {
        result.push_back(iter->first);
    }

    return result;
}

toft::File* CompressionFileFactory::CompressionFile(
        toft::File* file,
        const std::string& compression_ext) {
    CHECK_NOTNULL(file);
    if (compression_ext.empty()) {
        return file;
    }
    util::Guard guard;
    const CompressionFileFactory::Registry& registry = GetRegistry();
    CompressionFileFactory::Registry::const_iterator it = registry.find(compression_ext);
    if (it == registry.end()) {
        LOG(ERROR) << "Cannot get registry: " + compression_ext;
        CHECK(false);
    } else {
        return it->second(file);
    }
}

}   // namespace runtime
}   // namespace flume
}   // namespace baidu
