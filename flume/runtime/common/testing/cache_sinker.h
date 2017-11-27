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
//
// cache sinker for test

#ifndef FLUME_RUNTIME_CACHE_SINKER_H_
#define FLUME_RUNTIME_CACHE_SINKER_H_

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"

#include "flume/core/sinker.h"

namespace baidu {
namespace flume {
namespace runtime {

class CacheSinker : public core::Sinker {
public:
    virtual ~CacheSinker() {}

    virtual void Setup(const std::string& config);

    virtual void Open(const std::vector<toft::StringPiece>& keys) {
        m_impl->Open(keys);
    }

    virtual void Sink(void* object) {
        m_impl->Sink(object);
    }

    virtual void Close() {
        m_impl->Close();
    }

private:
    toft::scoped_ptr<CacheSinker> m_impl;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_CACHE_SINKER_H_
