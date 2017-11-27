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

 $Id$
 *
 **************************************************************************/

 /**
 * @file timer_queue.h
 * @author gaoran(gaoran@baidu.com)
 * @date 2017/01/10 10:55:13
 * @version $Revision$
 * @brief
 *
 **/
#ifndef FLUME_CORE_TIMER_QUEUE_H
#define FLUME_CORE_TIMER_QUEUE_H

#include <vector>

#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "boost/lexical_cast.hpp"

namespace baidu {
namespace flume {

namespace core {

struct WindowKey {
    //WindowKey() {}

    WindowKey(const std::vector<toft::StringPiece>& keys, uint64_t window_id)
        : second(window_id) {
        for (size_t i = 0; i != keys.size(); ++i) {
            real_first.push_back(keys[i].as_string());
            first.push_back(toft::StringPiece(real_first.back()));
        }
    }

    WindowKey(const WindowKey& key) : real_first(key.real_first), second(key.second) {
        for (size_t i = 0; i != real_first.size(); ++i) {
            first.push_back(toft::StringPiece(real_first[i]));
        }
    }

    WindowKey& operator=(const WindowKey& key) {
        this->real_first = key.real_first;
        this->second = key.second;
        first.clear();
        for (size_t i = 0; i != real_first.size(); ++i) {
            first.push_back(toft::StringPiece(real_first[i]));
        }
        return *this;
    }

    std::string encode() {
        std::string window_key_str;
        window_key_str.clear();
        window_key_str.append(second, sizeof(uint64_t));
        uint32_t real_first_size = real_first.size();
        window_key_str.append(real_first_size, sizeof(uint32_t));
        for (size_t i = 0; i < real_first.size(); ++i) {
            uint32_t first_size = real_first[i].size();
            window_key_str.append((char*)&first_size, sizeof(uint32_t));
            window_key_str.append(real_first[i].data(), first_size);
        }

        return window_key_str;
    }

    WindowKey(const std::string& window_key_str) {
        const char* data = window_key_str.data();
        uint32_t len = window_key_str.size();
        uint32_t size;
        CHECK(len < sizeof(size));

        memcpy(&size, data, sizeof(size)); //vector size
        data += sizeof(size);
        len -= sizeof(size);
        uint64_t second = size;
        std::vector<toft::StringPiece> first;
        uint32_t real_first_size;
        memcpy(&real_first_size, data, sizeof(real_first_size)); //vector size
        data += sizeof(real_first_size);
        len -= sizeof(real_first_size);

        for (uint32_t i = 0; i < real_first_size; ++i) {
            uint32_t id_size;
            CHECK(len < sizeof(id_size));
            memcpy(&id_size, data, sizeof(id_size));
            data += sizeof(id_size);
            len -= sizeof(id_size);

            CHECK(len < id_size);
            std::string id(data, id_size); //child executor id
            data += id_size;
            len -= id_size;
            first.push_back(toft::StringPiece(id));
            real_first.push_back(id);
        }
    }

    std::vector<toft::StringPiece> first;
    uint64_t second;
    std::vector<std::string> real_first;
};

bool operator< (const WindowKey& a, const WindowKey& b);

typedef std::pair<uint64_t,  WindowKey> Timer;

class TimerQueue {
public:
    virtual ~TimerQueue() {}
    virtual void push(Timer &node) = 0;
    virtual void pop() = 0;
    virtual bool empty() = 0;
    virtual Timer top() = 0;
};

}
}
}
#endif  // FLUME_CORE_TIMER_QUEUE_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
