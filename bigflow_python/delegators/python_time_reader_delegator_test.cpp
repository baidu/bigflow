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
* @created:     2016/11/10
* @author:      zhangyuncong (bigflow-opensource@baidu.com)
*/

#include "bigflow_python/delegators/python_time_reader_delegator.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "boost/iterator/indirect_iterator.hpp"
#include "boost/python.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "bigflow_python/common/iterator.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/functors/function.h"
#include "bigflow_python/python_interpreter.h"
#include "flume/core/entity.h"
#include "flume/core/testing/mock_emitter.h"
#include "flume/proto/entity.pb.h"

namespace baidu {
namespace bigflow {
namespace python {

class ToIntFunctor : public Function<1> {
public:
    boost::python::object operator()(boost::python::object object) {
        return boost::python::long_(object);
    }
};

class PythonTimeReaderDelegatorTest : public ::testing::Test {
protected:
    PythonTimeReaderDelegatorTest() {
    }
    virtual void SetUp() {
        PythonInterpreter::Instance();  // init env
    }

    virtual void TearDown() {
    }

    template<typename TimeReaderFunctor>
    PythonTimeReaderDelegator* create(const std::string& config = "") {
        flume::core::Entity<Functor> fn
                = flume::core::Entity<Functor>::template Of<TimeReaderFunctor>(config);

        std::string fn_str = fn.ToProtoMessage().SerializeAsString();
        toft::scoped_ptr<PythonTimeReaderDelegator> ptr;
        ptr.reset(new PythonTimeReaderDelegator);
        ptr->Setup(fn_str);
        return ptr.release();
    }
};

TEST_F(PythonTimeReaderDelegatorTest, TestNormal) {
    try {
        toft::scoped_ptr<PythonTimeReaderDelegator> time_reader;
        time_reader.reset(create<ToIntFunctor>());
        boost::python::str ten("10");
        CHECK_EQ(10u, time_reader->get_timestamp(&ten));

        boost::python::str big("123456789101112");
        CHECK_EQ(123456789101112ULL, time_reader->get_timestamp(&big));

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
