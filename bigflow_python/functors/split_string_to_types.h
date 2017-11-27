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
/**
* @created:     2016/11/03
* @filename:    split_string_to_types.h
* @author:      zhenggonglin(bigflow-opensource@baidu.com)
*/

#ifndef BIGFLOW_PYTHON_SPLIT_STRING_TO_TYPES_H
#define BIGFLOW_PYTHON_SPLIT_STRING_TO_TYPES_H

#include "boost/python.hpp"
#include "glog/logging.h"

#include "toft/base/string/algorithm.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/functors/functor.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/python_interpreter.h"

#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class SplitStringToTypes: public Functor {
    public:
        SplitStringToTypes();
        virtual void Setup(const std::string& config);
        virtual void call(void* object, flume::core::Emitter* emitter);

    private:
        std::string _sep;
        std::vector<boost::python::object> _fields_type;
        bool _ignore_overflow;
        bool _ignore_illegal_line;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_SPLIT_STRING_TO_TYPES_H
