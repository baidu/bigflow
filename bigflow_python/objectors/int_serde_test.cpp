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
// Author: Pan Yunhong <panyunhong@baidu.com>
//

#include "bigflow_python/python_interpreter.h"

#include <iostream>

#include "gtest/gtest.h"

#include "bigflow_python/objectors/int_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class IntSerdeTest : public ::testing::Test {
protected:
    IntSerdeTest() {}
    virtual ~IntSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

TEST_F(IntSerdeTest, TestCppAndPythonImplementationConsistent) {
    IntSerde iserde;
    char buffer[32];
    uint32_t serialize_size = 0;
    int64_t a = 123456;
    boost::python::object long_obj(a);

    serialize_size = iserde.serialize(&long_obj, buffer, 32);
    ASSERT_GT(serialize_size, 0);
    boost::python::str buf_obj(buffer, serialize_size);

    boost::python::object int_serde_pyobj =
        boost::python::import("bigflow.serde").attr("IntSerde")();
    boost::python::object int_deser_obj = int_serde_pyobj.attr("deserialize")(buf_obj);
    int64_t deser_val = boost::python::extract<int64_t>(int_deser_obj);
    EXPECT_EQ(deser_val, a);
}

TEST_F(IntSerdeTest, TestIntSerde) {
    try {
        IntSerde iserde;
        char buffer[32];
        uint32_t serialize_size = 0;
        int64_t a = 123456;
        boost::python::object long_obj(a);

        serialize_size = iserde.serialize(&long_obj, buffer, 32);
        ASSERT_GT(serialize_size, 0);

        void* deserialized = iserde.deserialize(buffer, serialize_size);
        boost::python::object* deserialize_obj = static_cast<boost::python::object*>(deserialized);
        int64_t ori_num = boost::python::extract<int64_t>(*deserialize_obj);
        EXPECT_EQ(ori_num, a);

        iserde.release(deserialized);
    } catch (...) {
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
