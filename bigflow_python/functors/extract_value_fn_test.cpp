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
* @created:     2015/06/18
* @author:      zhangyuncong (bigflow-opensource@baidu.com)
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/extract_value_fn.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "boost/iterator/indirect_iterator.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "bigflow_python/common/iterator.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/python_interpreter.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class ExtractValueFnTest : public ::testing::Test {
protected:
    ExtractValueFnTest() : _fn(&_real_fn){
    }
    virtual void SetUp() {
        PythonInterpreter::Instance();  // init env
        _real_fn.Setup("");
    }

    virtual void TearDown() {
    }

    void call(const boost::python::object& obj) {
        boost::python::tuple tp = boost::python::make_tuple(obj);
        _fn(&_emitter, &tp);
    }

    ExtractValueFn _real_fn;
    PyFunctorCaller _fn;
    flume::MockEmitter<boost::python::object> _emitter;
};

TEST_F(ExtractValueFnTest, TestNormal) {
    try {
        testing::InSequence in_sequence;
        EXPECT_CALL(_emitter, EmitValue(boost::python::object(2)));
        EXPECT_CALL(_emitter, EmitValue(boost::python::make_tuple("2", "3")));
        boost::python::tuple tp1 = boost::python::make_tuple(1, 2);
        boost::python::tuple tp2 = boost::python::make_tuple(1, "2", "3");
        boost::python::list l1;
        l1.append(1);
        l1.append(2);
        l1.append(3);
        boost::python::list expect;
        expect.append(2);
        expect.append(3);
        EXPECT_CALL(_emitter, EmitValue(expect));
        call(tp1);
        call(tp2);
        call(l1);
    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
