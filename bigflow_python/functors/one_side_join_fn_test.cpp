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
* @filename:    python_impl_functor.h
* @author:      zhangyuncong (zhangyuncong@baidu.com)
* @brief:       OneSideJoin
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/one_side_join_fn.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "boost/iterator/indirect_iterator.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "bigflow_python/common/iterator.h"
#include "bigflow_python/common/python.h""
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/python_interpreter.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

#define None
#define EXPECT_EMIT_TUPLE(left, right) \
    EXPECT_CALL(_emitter, \
        EmitValue( \
            boost::python::make_tuple( \
                boost::python::object(left),\
                boost::python::object(right) \
            ) \
        )\
    )

class OneSideJoinFnTest : public ::testing::Test {
protected:
    OneSideJoinFnTest() : _fn(&_real_fn){
    }
    virtual void SetUp() {
        PythonInterpreter::Instance();  // init env
        _real_fn.Setup("");
    }

    virtual void TearDown() {
    }

    void call(boost::python::object left, boost::python::object right) {
        boost::python::list list;
        list.append(left);
        list.append(right);
        boost::python::tuple args(list);
        _fn(&_emitter, &args);
    }

    OneSideJoinFn _real_fn;
    PyFunctorCaller _fn;
    flume::MockEmitter<boost::python::object> _emitter;
};

TEST_F(OneSideJoinFnTest, TestNormal) {
    try {
        testing::InSequence in_sequence;
        std::vector<boost::python::object> side_input_vec;
        side_input_vec.push_back(boost::python::object(1));
        side_input_vec.push_back(boost::python::object("2"));
        side_input_vec.push_back(boost::python::object(3L));
        std::auto_ptr<flume::core::Iterator> it =
                baidu::bigflow::python::iterator(side_input_vec.begin(), side_input_vec.end());
        SideInput side_input(it.get());

        EXPECT_EMIT_TUPLE(0, 1);
        EXPECT_EMIT_TUPLE(0, "2");
        EXPECT_EMIT_TUPLE(0, 3L);

        call(boost::python::object(0), boost::python::object(boost::ref(side_input)));

        EXPECT_EMIT_TUPLE("0", 1);
        EXPECT_EMIT_TUPLE("0", "2");
        EXPECT_EMIT_TUPLE("0", 3L);
        call(boost::python::object("0"), boost::python::object(boost::ref(side_input)));
    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(OneSideJoinFnTest, TestEmpty) {
    try {
        std::vector<boost::python::object> side_input_vec;
        std::auto_ptr<flume::core::Iterator> it =
                baidu::bigflow::python::iterator(side_input_vec.begin(), side_input_vec.end());
        SideInput side_input(it.get());

        EXPECT_EMIT_TUPLE(0, None);
        call(boost::python::object(0), boost::python::object(boost::ref(side_input)));
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
