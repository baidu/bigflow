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
* @brief:       FullJoin
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/full_join_fn.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "boost/iterator/indirect_iterator.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "bigflow_python/common/iterator.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
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

// class Tricky : public flume::core::Emitter{
// public:
//     Tricky(flume::core::Emitter* emitter): _emitter(emitter){
//     }

//     bool Emit(void* object) {
//         PyObject* o = static_cast<PyObject*>(object);
//         boost::python::object obj((boost::python::handle<>(o)));
//         return _emitter->Emit(&obj);
//     }

//     void Done() {
//         _emitter->Done();
//     }

// private:
//     flume::core::Emitter* _emitter;
// };

class FullJoinFnTest : public ::testing::Test {
protected:
    FullJoinFnTest()
            : _init(&_init_fn),
            _transform(&_transform_fn),
            _finalize(&_finalize_fn),
            // _tricky(&_emitter),
            // _emitter_delegator(&_tricky),
            _emitter_delegator(&_emitter),
            _emitter_object(boost::python::object(boost::ref(_emitter_delegator)) ){
    }

    virtual void SetUp() {
        _init_fn.Setup("");
        _transform_fn.Setup("");
        _finalize_fn.Setup("");
    }

    virtual void TearDown() {
    }

    FullJoinInitializeFn _init_fn;
    FullJoinTransformFn _transform_fn;
    FullJoinFinalizeFn _finalize_fn;

    PyFunctorCaller _init;
    PyFunctorCaller _transform;
    PyFunctorCaller _finalize;

    flume::MockEmitter<boost::python::object> _emitter;
//    Tricky _tricky;
    EmitterDelegator _emitter_delegator;
    boost::python::object _emitter_object;
};

TEST_F(FullJoinFnTest, TestNormal) {
    try {
        testing::InSequence in_sequence;
        boost::python::object none;

        boost::python::object left(0);
        std::vector<boost::python::object> side_input_vec;
        side_input_vec.push_back(boost::python::object(1));
        side_input_vec.push_back(boost::python::object("2"));
        side_input_vec.push_back(boost::python::object(3L));
        std::auto_ptr<flume::core::Iterator> it =
                baidu::bigflow::python::iterator(side_input_vec.begin(), side_input_vec.end());
        SideInput side_input(it.get());

        toft::scoped_ptr<toft::Closure<void ()> > done_fn(
                toft::NewPermanentClosure(&_emitter_delegator, &EmitterDelegator::done));

        boost::python::tuple args = boost::python::make_tuple(_emitter_object,
                boost::python::object(boost::ref(side_input)));
        boost::python::object left_is_empty = _init(done_fn.get(), &args);
        ASSERT_TRUE(left_is_empty);

        EXPECT_EMIT_TUPLE("0", 1);
        EXPECT_EMIT_TUPLE("0", "2");
        EXPECT_EMIT_TUPLE("0", 3L);

        args = boost::python::make_tuple(
            left_is_empty,
            _emitter_object,
            boost::python::object("0"),
            boost::python::object(boost::ref(side_input))
        );
        left_is_empty = _transform(done_fn.get(), &args);
        ASSERT_FALSE(left_is_empty);

        args = boost::python::make_tuple(left_is_empty, _emitter_object,
                boost::python::object(boost::ref(side_input)));
        ASSERT_EQ(none, _finalize(done_fn.get(), &args));

    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(FullJoinFnTest, TestEmpty) {
    try {
        testing::InSequence in_sequence;
        boost::python::object none;

        boost::python::object left(0);
        std::vector<boost::python::object> side_input_vec;
        side_input_vec.push_back(boost::python::object(1));
        side_input_vec.push_back(boost::python::object("2"));
        side_input_vec.push_back(boost::python::object(3L));
        std::auto_ptr<flume::core::Iterator> it =
                baidu::bigflow::python::iterator(side_input_vec.begin(), side_input_vec.end());
        SideInput side_input(it.get());

        toft::scoped_ptr<toft::Closure<void ()> > done_fn(
                toft::NewPermanentClosure(&_emitter_delegator, &EmitterDelegator::done));

        boost::python::tuple args = boost::python::make_tuple(_emitter_object,
                boost::python::object(boost::ref(side_input)));
        boost::python::object left_is_empty = _init(done_fn.get(), &args);
        ASSERT_TRUE(left_is_empty);

        args = boost::python::make_tuple(left_is_empty, _emitter_object, boost::python::object(boost::ref(side_input)));

        EXPECT_EMIT_TUPLE(None, 1);
        EXPECT_EMIT_TUPLE(None, "2");
        EXPECT_EMIT_TUPLE(None, 3L);

        ASSERT_EQ(none, _finalize(done_fn.get(), &args));

    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

int main(int argc, char** argv) {
    baidu::bigflow::python::PythonInterpreter::Instance();  // init env
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
