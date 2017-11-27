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
* @created:     2015/06/22
* @filename:    select_elements_processor.cpp
* @author:      fangjun02@baidu.com
* @brief:       select emements processor implementation
*/

#include "bigflow_python/processors/select_elements_processor.h"

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void SelectElementsProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    if (fns.size() > 0) {
        CHECK_EQ(fns.size() ,1);
        _key_fn.reset(new PyFunctorCaller(fns[0]));
    }
    boost::python::object conf = CPickleSerde().loads(config);
    _num = boost::python::extract<int64_t>(conf["num"]);
    _order = boost::python::extract<bool>(conf["order"]);
    _compare.reset(new Compare(_order));
    _tuple = boost::python::make_tuple(boost::python::object());
}

void SelectElementsProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                          ProcessorContext* context) {
    _context = context;
    _done_fn.reset(toft::NewPermanentClosure(context, &ProcessorContext::done));
    _objects.clear();
    const std::vector<boost::python::object>& side_input_list =
        context->python_side_inputs();

    if (!side_input_list.empty()) {
        _num = boost::python::extract<int64_t>(side_input_list[0]);
    }
}

void SelectElementsProcessor::process(void* object) {
    boost::python::object record = *static_cast<boost::python::object*>(object);
    boost::python::object key;
    if (_key_fn.get() != NULL) {
        tuple_set_item(_tuple, 0, record);
        key = (*_key_fn)(_done_fn.get(), &_tuple);
    } else {
        key = record;
    }
    boost::python::tuple node = boost::python::make_tuple(key, record);
    _objects.push_back(node);
    std::push_heap(_objects.begin(), _objects.end(), *_compare);
    if (static_cast<int64_t>(_objects.size()) > _num) {
        std::pop_heap(_objects.begin(), _objects.end(), *_compare);
        _objects.pop_back();
    }
}

void SelectElementsProcessor::end_group() {
    //std::string str = boost::python::extract<std::string>(boost::python::str(_objects[0]));
    for (int i = std::min<int64_t>(_num, _objects.size()) - 1; i >= 0; i--) {
        boost::python::object record = tuple_get_item(_objects[i], 1);
        _context->emit(&record);
    }
}

} // namespace python
} // namespace bigflow
} // namespace baidu
