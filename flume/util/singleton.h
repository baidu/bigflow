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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)
//
// Description: Flume Singleton

// Usage:
// public MySingleton : public Singleton<MySingleton>{
// friend class Singleton<MySingleton>;
// public:
//     std::string my_method() { return something;}
// private:
//     MySingleton() {// do some initialize operations}
// };
//
// MySingleton& instance = MySingleton::GetInstance();

#ifndef FLUME_UTIL_SINGLETON_H
#define FLUME_UTIL_SINGLETON_H

#include "toft/base/scoped_ptr.h"
#include "boost/thread/mutex.hpp"
#include "boost/thread/lock_guard.hpp"
#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace util {

template<typename T>
class Singleton {
public:
    static T& GetInstance() {
        if (m_instance.get() == NULL) {
            boost::mutex::scoped_lock guard(m_lock);
            if (m_instance.get() == NULL) {
                m_instance.reset(new T());
                CHECK_NOTNULL(m_instance.get());
            }
        }
        return *m_instance.get();
    }
private:
    static toft::scoped_ptr<T> m_instance;
    static boost::mutex m_lock;
};

template<typename T>
toft::scoped_ptr<T> Singleton<T>::m_instance;

template<typename T>
boost::mutex Singleton<T>::m_lock;

} // namespace util
} // namespace flume
} // namespace baidu

#endif  // FLUME_UTIL_SINGLETON_H
