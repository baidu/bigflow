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
// Author: Zhang Jianwei <zhangjianwei@baidu.com>
//
// empty environment

#ifndef BIGFLOW_EMPTY_ENVIRONMENT_H_
#define BIGFLOW_EMPTY_ENVIRONMENT_H_

#include "flume/core/environment.h"

namespace baidu {
namespace flume {
namespace core {

class EmptyEnvironment : public Environment {
public:
    virtual void Setup(const std::string& config);

    // execuate when task setup.
    virtual void do_setup();

    // execuate when task cleanup.
    virtual void do_cleanup();
};

}  // namespace core
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_EMPTY_ENVIRONMENT_H_
