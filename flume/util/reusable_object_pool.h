/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Liu Cheng <liucheng02@baidu.com>
//
// A object pool to cache resuable objects.

#ifndef  FLUME_RUNTIME_COMMON_REUSABLE_OBJECT_POOL_H_
#define  FLUME_RUNTIME_COMMON_REUSABLE_OBJECT_POOL_H_

#include <vector>

namespace baidu {
namespace flume {
namespace util {

class Reusable {
public:
    virtual void Reuse() {
        // The default implement of reuse is delete object, releasing resources
        delete this;
    }
    virtual ~Reusable() {}
};

template <typename T>
class ReusableObjectPool {
public:
    static const int kMaxCacheObject = 32;

    static ReusableObjectPool* Instance() {
        static ReusableObjectPool pool;
        return &pool;
    }

    T* GetObject() {
        T* t = NULL;
        if (m_count == 0) {
            t = new T();
        } else {
            t = m_objects[m_count - 1];
            m_count--;
        }
        return t;
    }

    void ReleaseObject(T* t) {
        if (m_count < kMaxCacheObject) {
            m_objects[m_count] = t;
            m_count++;
        } else {
            delete t;
        }
    }

private:
    ReusableObjectPool() : m_count(0) {}
    ~ReusableObjectPool() {
        for (int i = 0; i < m_count; ++i) {
            delete m_objects[i];
        }
    }

private:
    T* m_objects[kMaxCacheObject];
    int m_count;
};

}  // namespace util
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_REUSABLE_OBJECT_POOL_H_
