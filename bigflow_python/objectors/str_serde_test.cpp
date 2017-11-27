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
// Author: Pan Yunhong <bigflow-opensource@baidu.com>
//

#include "bigflow_python/python_interpreter.h"

#include <iostream>

#include "gtest/gtest.h"

#include "bigflow_python/objectors/str_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class StrSerdeTest : public ::testing::Test {
protected:
    StrSerdeTest() {}
    virtual ~StrSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

TEST_F(StrSerdeTest, TestCppAndPythonImplementationConsistent) {
    StrSerde sserde;
    char buffer[128];
    uint32_t serialize_size = 0;
    std::string str = "abcdefg";
    boost::python::str str_obj(str.data(), str.size());

    serialize_size = sserde.serialize(&str_obj, buffer, 128);
    ASSERT_GT(serialize_size, 0);
    boost::python::str buf_obj(buffer, serialize_size);

    boost::python::object str_serde_pyobj =
        boost::python::import("bigflow.serde").attr("StrSerde")();
    boost::python::object str_deser_obj = str_serde_pyobj.attr("deserialize")(buf_obj);
    std::string deser_val = boost::python::extract<std::string>(str_deser_obj);
    EXPECT_EQ(deser_val, str);
}

TEST_F(StrSerdeTest, TestStrSerde) {
    try {
        StrSerde sserde;
        char buffer[128];
        uint32_t serialize_size = 0;
        std::string str = "abcdefg";
        boost::python::str str_obj(str.data(), str.size());

        serialize_size = sserde.serialize(&str_obj, buffer, 128);
        ASSERT_GT(serialize_size, 0);
        void* deserialized = sserde.deserialize(buffer, serialize_size);
        boost::python::object* deserialize_obj = static_cast<boost::python::object*>(deserialized);
        std::string ori_str = boost::python::extract<std::string>(*deserialize_obj);
        EXPECT_EQ(ori_str, str);

        sserde.release(deserialized);
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
