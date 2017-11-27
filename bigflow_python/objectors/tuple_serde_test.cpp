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

#include "bigflow_python/objectors/tuple_serde.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class TupleSerdeTest : public ::testing::Test {
protected:
    TupleSerdeTest() {}
    virtual ~TupleSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }
    std::string build_types_tuple_as_string() {
        boost::python::object str_type = boost::python::str().attr("__class__");
        boost::python::object int_type = boost::python::object(0).attr("__class__");
        boost::python::object tuple_serde(
                boost::python::import("bigflow.serde").attr("TupleSerde")(str_type, int_type)
        );
        return boost::python::extract<std::string>(CPickleSerde().dumps(tuple_serde));
    }
    boost::python::object build_types_tuple() {
        boost::python::object str_type = boost::python::str().attr("__class__");
        boost::python::object int_type = boost::python::object(0).attr("__class__");
        boost::python::object tuple_serde(
                boost::python::import("bigflow.serde").attr("TupleSerde")(str_type, int_type)
        );
        return tuple_serde;
    }
    boost::python::object build_tuple_args(const std::string& str, const int64_t int_args) {
        boost::python::tuple args = new_tuple(2);
        tuple_set_item(args, 0, boost::python::str(str));
        tuple_set_item(args, 1, boost::python::object(int_args));
        return args;
    }
};

TEST_F(TupleSerdeTest, TestCppAndPythonImplementationConsistent) {
    try {
        std::string tuple_serde_str = build_types_tuple_as_string();
        TupleSerde* objector = new TupleSerde();
        objector->setup(tuple_serde_str);

        const static int BUFFER_SIZE = 128;
        char *serialize_buf = new char[BUFFER_SIZE];
        std::string expect_str = "abcdefg";
        int64_t expect_int = 12346;
        boost::python::object args = build_tuple_args(expect_str, expect_int);
        uint32_t serialize_size = objector->serialize(&args, serialize_buf, BUFFER_SIZE);
        EXPECT_TRUE(serialize_size > 0);
        boost::python::str buf_obj(serialize_buf, serialize_size);

        boost::python::object tuple_serde_pyobj = build_types_tuple();
        boost::python::object tuple_deser_obj = tuple_serde_pyobj.attr("deserialize")(buf_obj);

        EXPECT_EQ(tuple_len(tuple_deser_obj), 2);
        boost::python::object item = tuple_get_item(tuple_deser_obj, 0);
        std::string actual_str = boost::python::extract<std::string>(item);
        EXPECT_EQ(actual_str, expect_str);

        item = tuple_get_item(tuple_deser_obj, 1);
        int64_t actual_int = boost::python::extract<int64_t>(item);
        EXPECT_EQ(actual_int, expect_int);

        delete objector;
    } catch (...) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

TEST_F(TupleSerdeTest, TestNormalSerde) {
    try {
        std::string tuple_serde_str = build_types_tuple_as_string();
        TupleSerde* objector = new TupleSerde();
        objector->setup(tuple_serde_str);

        const static int BUFFER_SIZE = 128;
        char *serialize_buf = new char[BUFFER_SIZE];
        std::string expect_str = "abcdefg";
        int64_t expect_int = 12346;
        boost::python::object args = build_tuple_args(expect_str, expect_int);
        uint32_t serialize_size = objector->serialize(&args, serialize_buf, BUFFER_SIZE);
        EXPECT_TRUE(serialize_size > 0);

        void *deserialized_obj = objector->deserialize(serialize_buf, serialize_size);
        boost::python::object* tuple_obj = static_cast<boost::python::object*>(deserialized_obj);
        boost::python::object element = tuple_get_item(*tuple_obj, 0);
        std::string element_str = boost::python::extract<std::string>(element);
        EXPECT_EQ(element_str, expect_str);
        element = tuple_get_item(*tuple_obj, 1);
        int64_t element_int = boost::python::extract<int64_t>(element);
        EXPECT_EQ(element_int, expect_int);

        objector->release(deserialized_obj);
        delete objector;
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
