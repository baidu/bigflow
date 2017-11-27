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
/**
* @created:     2015/06/23
* @filename:    transform_fn.h
* @author:      bigflow-opensource@baidu.com
* @brief:       transform functor implementation
*/

#ifndef BIGFLOW_PYTHON_TRANSFORM_FN_H_
#define BIGFLOW_PYTHON_TRANSFORM_FN_H_
#include <vector>

#include "glog/logging.h"
#include "boost/python.hpp"

#include "bigflow_python/functors/functor.h"

namespace baidu {
namespace bigflow {
namespace python {

class TransformInitializerFn : public Functor {
public:
    TransformInitializerFn() {}
    virtual ~TransformInitializerFn() {}
    virtual void Setup(const std::string& config) = 0;
    virtual void call(void* object, flume::core::Emitter* emitter) = 0;
};

class TransformerFn : public Functor {
public:
    TransformerFn() {}
    virtual ~TransformerFn() {}
    virtual void Setup(const std::string& config) = 0;
    virtual void call(void* object, flume::core::Emitter* emitter) = 0;
};

class TransformFinalizerFn : public Functor {
public:
    TransformFinalizerFn() {}
    virtual ~TransformFinalizerFn() {}
    virtual void Setup(const std::string& config) = 0;
    virtual void call(void* object, flume::core::Emitter* emitter) = 0;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_TRANSFORM_FN_H_
