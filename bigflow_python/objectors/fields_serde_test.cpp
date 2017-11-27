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

#include "bigflow_python/objectors/fields_serde.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class FieldsSerdeTest : public ::testing::Test {
protected:
    FieldsSerdeTest() {}
    virtual ~FieldsSerdeTest() {}

    virtual void SetUp() {
        PythonInterpreter::Instance();
    }

    virtual void TearDown() {
    }
    std::string build_fields_serde_as_string() {
        //{'websites': str, 'clicknum': int}
        boost::python::object str_type = boost::python::str().attr("__class__");
        boost::python::object int_type = boost::python::object(0).attr("__class__");
        boost::python::dict dict_args;
        dict_args["websites"] = str_type;
        dict_args["clicknum"] = int_type;

        boost::python::object fields_dict_serde_obj(
                boost::python::import("bigflow.future.fields").attr("FieldsDictSerde")(dict_args)
        );
        return boost::python::extract<std::string>(CPickleSerde().dumps(fields_dict_serde_obj));
    }
};

TEST_F(FieldsSerdeTest, TestNormalSerde) {
    try {
        std::string serde_str = build_fields_serde_as_string();
        FieldsSerde* serde = new FieldsSerde();
        serde->setup(serde_str);

        const static int BUFFER_SIZE = 64;
        char *serialize_buf = new char[BUFFER_SIZE];
        boost::python::dict input_data;
        std::string ws = "a,b,c";
        int64_t cn = 64;
        input_data["websites"] = ws;
        input_data["clicknum"] = cn;

        uint32_t serialize_size = serde->serialize(&input_data, serialize_buf, BUFFER_SIZE);
        EXPECT_TRUE(serialize_size > 0);

        void *deserialized_obj = serde->deserialize(serialize_buf, serialize_size);
        boost::python::dict* dict_obj = static_cast<boost::python::dict*>(deserialized_obj);
        boost::python::list items = dict_obj->items();
        int64_t items_len = list_len(items);

        for (int64_t i = 0; i < items_len; ++i) {
            boost::python::object t = items[i][0];
            std::string key = boost::python::extract<std::string>(t);
            if (key == "websites") {
                std::string s = boost::python::extract<std::string>(items[i][1]);
                EXPECT_EQ(s, ws);
            } else {
                int64_t n = boost::python::extract<int64_t>(items[i][1]);
                EXPECT_EQ(n, cn);
            }
        }
        serde->release(deserialized_obj);
        delete serde;
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
