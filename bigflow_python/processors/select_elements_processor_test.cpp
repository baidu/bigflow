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
* @filename:    select_elements_processor_test.cpp
* @author:      fangjun02@baidu.com
* @brief:       unittest for select elements processor
*/

#include "bigflow_python/processors/select_elements_processor.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class MaxElementTest : public ::testing::Test {
protected:
    MaxElementTest() {}
    virtual ~MaxElementTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }
};

TEST_F(MaxElementTest, TestMaxElements) {
    try {
        boost::python::dict d;
        d["num"] = 3;
        d["order"] = false;
        boost::python::object dump = CPickleSerde().dumps(d);
        std::string config = boost::python::extract<std::string>(boost::python::str(dump));

        flume::MockEmitter<boost::python::object> emitter;
        boost::python::object one(1);
        boost::python::object two(2);
        boost::python::object three(3);
        EXPECT_CALL(emitter, EmitValue(one));
        EXPECT_CALL(emitter, EmitValue(two));
        EXPECT_CALL(emitter, EmitValue(three));

        const char* kKeys[] = {"split1", "split2"};
        std::vector<flume::core::Iterator*> vec_iter;

        boost::python::object ob1(5);
        boost::python::object ob2(3);
        boost::python::object ob3(1);
        boost::python::object ob4(2);
        boost::python::object ob5(4);

        std::vector<Functor*> fns;
        ProcessorContext context(&emitter, vec_iter);

        SelectElementsProcessor impl;
        impl.setup(fns, config);
        impl.begin_group(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
                        &context);
        impl.process(&ob1);
        impl.process(&ob2);
        impl.process(&ob3);
        impl.process(&ob4);
        impl.process(&ob5);
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
