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
* @filename:    flatmap_processor_test.cpp
* @author:      bigflow-opensource@baidu.com
* @brief:       unittest for flatmap processor
*/

#include "bigflow_python/processors/filter_processor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/python_args.h"

#include "flume/core/iterator.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class TestFilterFn : public Functor {
public:
    virtual void setup(const std::string& config) {
    }

    virtual void call(void* object, flume::core::Emitter* emitter) {
        PythonArgs* args = static_cast<PythonArgs*>(object);
        CHECK_NOTNULL(args->args);
        boost::python::object input = (*args->args)[0];
        boost::python::object result = input < boost::python::object(10);
        emitter->Emit(&result);
    }
};

class FilterProcessorTest : public ::testing::Test {
protected:
    FilterProcessorTest() {}
    virtual ~FilterProcessorTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

TEST_F(FilterProcessorTest, TestFilter) {
    try {
        const char* kKeys[] = {"split1", "split2"};
        std::vector<flume::core::Iterator*> vec_iter;
        boost::python::object one(1);
        boost::python::object two(2);
        boost::python::object thirty(30);

        flume::MockEmitter<boost::python::object> emitter;
        testing::InSequence inseq;
        EXPECT_CALL(emitter, EmitValue(one));
        EXPECT_CALL(emitter, EmitValue(two));

        std::vector<Functor*> fns;
        TestFilterFn fn;
        fns.push_back(&fn);
        ProcessorContext context(&emitter, vec_iter);

        FilterProcessor impl;
        impl.setup(fns, "");
        impl.begin_group(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
                        &context);
        impl.process(&one);
        impl.process(&two);
        impl.process(&thirty);
        impl.end_group();
    } catch(...) {
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
