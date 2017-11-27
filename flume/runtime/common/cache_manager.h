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

#ifndef FLUME_RUNTIME_COMMON_CACHE_MANAGER_H_
#define FLUME_RUNTIME_COMMON_CACHE_MANAGER_H_

#include <map>
#include <string>
#include <vector>

#include "toft/base/string/string_piece.h"

namespace baidu {
namespace flume {
namespace runtime {

class CacheManager {
public:
    class Iterator {
    public:
        virtual bool Next() = 0;
        virtual void Reset() = 0;
        virtual void Done() = 0;

        virtual const std::vector<toft::StringPiece>& Keys() = 0;
        virtual const toft::StringPiece& Value() = 0;
        virtual ~Iterator() {}
    };

    class Reader {
    public:
        virtual void GetSplits(std::vector<std::string>* splits) const = 0;
        virtual CacheManager::Iterator* Read(const std::string& split) = 0;
        virtual ~Reader() {}
    };

    class Writer {
    public:
        virtual bool BeginKeys(const std::vector<toft::StringPiece>& keys) = 0;
        virtual bool Write(const toft::StringPiece& value) = 0;
        virtual bool EndKeys() = 0;
        virtual ~Writer() {}
    };

    CacheManager(){}

    virtual ~CacheManager();

    virtual void Shutdown() = 0;

    virtual Reader* GetReader(const std::string& node_id) = 0;
    virtual Writer* GetWriter(const std::string& node_id) = 0;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_CACHE_MANAGER_H_
