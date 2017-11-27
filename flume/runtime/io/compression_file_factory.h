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
//
// Compression File Factory.

#ifndef FLUME_RUNTIME_IO_COMPRESSION_FILE_FACTORY_H_
#define FLUME_RUNTIME_IO_COMPRESSION_FILE_FACTORY_H_

#include <map>

#include "glog/logging.h"
#include "toft/storage/file/file.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace util {

class Guard {
public:
    Guard();
    ~Guard();
};

}  // namespace util

class CompressionFileFactory {
    TOFT_DECLARE_UNCOPYABLE(CompressionFileFactory);

public:
    typedef std::map<std::string, toft::File*(*)(toft::File*)> Registry;

    CompressionFileFactory() {}
    virtual ~CompressionFileFactory() {}

    static toft::File* CompressionFile(toft::File* file, const std::string& compression_ext);
    static std::vector<std::string> GetCompressions();

    template<typename T>
    static bool Register(const std::string& compression_ext) {
        util::Guard guard;
        Registry& registry = GetRegistry();
        size_t count = registry.count(compression_ext);
        if (count == 0u) {
            registry[compression_ext] = &Helper<T>::From;
        }
        return true;
    }

private:
    template<typename T>
    struct Helper {
        static toft::File* From(toft::File* file) { return T::From(file); }
    };
    static Registry& GetRegistry();
};

#define COMPRESSION_FILE_FACTORY_REGISTER(class_name, \
                                          compression_ext) \
    static bool TOFT_PP_JOIN(g_object_registry_##class_name, __LINE__)( \
        ::baidu::flume::runtime::CompressionFileFactory::Register<class_name> \
        (compression_ext))

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_IO_COMPRESSION_FILE_FACTORY_H_
