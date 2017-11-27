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

#include "bigflow_python/objectors/bool_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class BoolSerdeTest : public ::testing::Test {
protected:
    BoolSerdeTest() {}
    virtual ~BoolSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

TEST_F(BoolSerdeTest, TestCppAndPythonImplementationConsistent) {
    try {
        BoolSerde bserde;
        char buffer[8];
        uint32_t serialize_size = 0;
        bool b1 = true;
        bool b2 = false;
        boost::python::object bool_obj1 = boost::python::object(boost::python::handle<>(Py_True));
        boost::python::object bool_obj2 = boost::python::object(boost::python::handle<>(Py_False));

        serialize_size = bserde.serialize(&bool_obj1, buffer, 8);
        ASSERT_GT(serialize_size, 0);
        boost::python::str buf_obj(buffer, serialize_size);

        boost::python::object bool_serde_pyobj =
            boost::python::import("bigflow.serde").attr("BoolSerde")();
        boost::python::object bool_deser_obj = bool_serde_pyobj.attr("deserialize")(buf_obj);

        bool actual_val = boost::python::extract<bool>(bool_deser_obj);
        EXPECT_EQ(actual_val, b1);

        serialize_size = bserde.serialize(&bool_obj2, buffer, 8);
        ASSERT_GT(serialize_size, 0);
        buf_obj = boost::python::str(buffer, serialize_size);
        bool_deser_obj = bool_serde_pyobj.attr("deserialize")(buf_obj);

        actual_val = boost::python::extract<bool>(bool_deser_obj);
        EXPECT_EQ(actual_val, b2);
    } catch (...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(BoolSerdeTest, TestBoolSerde) {
    try {
        BoolSerde bserde;
        char buffer[8];
        uint32_t serialize_size = 0;
        bool b1 = true;
        bool b2 = false;
        boost::python::object bool_obj1 = boost::python::object(boost::python::handle<>(Py_True));
        boost::python::object bool_obj2  = boost::python::object(boost::python::handle<>(Py_False));

        serialize_size = bserde.serialize(&bool_obj1, buffer, 8);
        ASSERT_GT(serialize_size, 0);

        void* deserialized = bserde.deserialize(buffer, serialize_size);
        boost::python::object* deserialize_obj = static_cast<boost::python::object*>(deserialized);
        bool ori_val = boost::python::extract<bool>(*deserialize_obj);
        EXPECT_EQ(ori_val, b1);

        serialize_size = bserde.serialize(&bool_obj2, buffer, 8);
        ASSERT_GT(serialize_size, 0);
        deserialized = bserde.deserialize(buffer, serialize_size);
        deserialize_obj = static_cast<boost::python::object*>(deserialized);
        ori_val = boost::python::extract<bool>(*deserialize_obj);
        EXPECT_EQ(ori_val, b2);

        bserde.release(deserialized);
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
