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
#include <iomanip>

#include "gtest/gtest.h"

#include "bigflow_python/objectors/float_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class FloatSerdeTest : public ::testing::Test {
protected:
    FloatSerdeTest() {}
    virtual ~FloatSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }

};

TEST_F(FloatSerdeTest, TestCppAndPythonImplementationConsistent) {
    FloatSerde fserde;
    char buffer[32];
    uint32_t serialize_size = 0;
    double f = 3.1415926;
    boost::python::object float_obj(f);

    serialize_size = fserde.serialize(&float_obj, buffer, 32);
    ASSERT_GT(serialize_size, 0);

    boost::python::str buf_obj(buffer, serialize_size);
    boost::python::object float_serde_pyobj =
        boost::python::import("bigflow.serde") \
            .attr("FloatSerde")();
    boost::python::object float_deser_obj = float_serde_pyobj.attr("deserialize")(buf_obj);
    double deser_val = boost::python::extract<double>(float_deser_obj);
    EXPECT_EQ(deser_val, f);

    // test if python serialize and cpp deserialize the same
    boost::python::object float_ser_buf =
        float_serde_pyobj.attr("serialize")(float_obj);
    std::string float_serialize_buf =
        boost::python::extract<std::string>(float_ser_buf);
    boost::python::object* deser_result =
        static_cast<boost::python::object*>(
            fserde.deserialize(
                float_serialize_buf.c_str(), float_serialize_buf.length()
            )
        );
    double result_float = boost::python::extract<double>(*deser_result);
    fserde.release(deser_result);
    EXPECT_EQ(result_float, f);

}

TEST_F(FloatSerdeTest, TestFloatSerde) {
    try {
        FloatSerde fserde;
        char buffer[32];
        uint32_t serialize_size = 0;
        double f = 3.1415926535;
        boost::python::object float_obj(f);

        serialize_size = fserde.serialize(&float_obj, buffer, 32);
        ASSERT_GT(serialize_size, 0);

        void* deserialized = fserde.deserialize(buffer, serialize_size);
        boost::python::object* deserialize_obj = static_cast<boost::python::object*>(deserialized);
        double ori_val = boost::python::extract<double>(*deserialize_obj);
        LOG(INFO) << std::fixed << std::setprecision(10) << "ori val = " << ori_val;
        LOG(INFO) << std::fixed << std::setprecision(10) << "f = " << f;
        ASSERT_DOUBLE_EQ(ori_val, f);

        fserde.release(deserialized);
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
