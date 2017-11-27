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
// Author: Zhang jianwei <zhangjianwei@baidu.com>
//

#ifndef FLUME_CORE_ENVIRONMENT_H_
#define FLUME_CORE_ENVIRONMENT_H_

#include <string>

namespace baidu {
namespace flume {
namespace core {

class Environment {
public:
    virtual ~Environment() {}

    virtual void Setup(const std::string& config) = 0;

    // execuate when task setup.
    virtual void do_setup() = 0;

    // execuate when task cleanup.
    virtual void do_cleanup() = 0;

};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_ENVIRONMENT_H_
