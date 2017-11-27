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
//
// scoped_ptr for flume object.

#ifndef FLUME_RUNTIME_UTILS_OBJECT_HOLDER_H_
#define FLUME_RUNTIME_UTILS_OBJECT_HOLDER_H_

#include "glog/logging.h"

#include "flume/core/objector.h"

namespace baidu {
namespace flume {
namespace runtime {

class ObjectHolder {
public:
    explicit ObjectHolder(core::Objector* objector)
            : m_objector(objector), m_has_object(false), m_object(NULL) {}

    ~ObjectHolder() { Clear(); }

    void* Get() const{
        DCHECK_EQ(true, m_has_object);
        return m_object;
    }

    void Clear() {
        if (m_has_object) {
            m_objector->Release(m_object);
        }
        m_object = NULL;
        m_has_object = false;
    }

    void Reset(void *object) {
        if (m_has_object) {
            m_objector->Release(m_object);
        }
        m_object = object;
        m_has_object = true;
    }

    void* Reset(const toft::StringPiece& binary) {
        if (m_has_object) {
            m_objector->Release(m_object);
        }
        m_has_object = true;

        m_object = m_objector->Deserialize(binary.data(), binary.size());
        return m_object;
    }

private:
    core::Objector* m_objector;

    // NULL is legal value
    bool m_has_object;
    void* m_object;
};


}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_UTILS_OBJECT_HOLDER_H_
