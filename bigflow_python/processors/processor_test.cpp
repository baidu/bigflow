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
// Author: Zhang Yuncong(bigflow-opensource@baidu.com)

#include "processor.h"

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "boost/iterator/indirect_iterator.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "bigflow_python/common/iterator.h"
#include "bigflow_python/python_interpreter.h"

#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

class ProcessorContextTest : public ::testing::Test {
protected:
    ProcessorContextTest(){
    }
    virtual void SetUp() {
        PythonInterpreter::Instance(); // force setup env
        _inputs.clear();
        _side_inputs.clear();
        _inputs.push_back(boost::python::object(1));
        _inputs.push_back(boost::python::long_(2));
        _inputs.push_back(boost::python::str("3"));

        _iterator0 = baidu::bigflow::python::iterator(
            _inputs.begin(),
            _inputs.end() - 1
        ); // 1, 2
        _iterator1 = baidu::bigflow::python::iterator(
            _inputs.begin(),
            _inputs.end()
        );  // 1, 2, "3"

        _iterator2 = baidu::bigflow::python::iterator(
            _inputs.begin(),
            _inputs.begin() + 1
        ); // 1

        _side_inputs.push_back(_iterator0.get());
        _side_inputs.push_back(_iterator1.get());
        _side_inputs.push_back(_iterator2.get());
        _types.push_back(PCOLLECTION_TYPE);
        _types.push_back(PCOLLECTION_TYPE);
        _types.push_back(POBJECT_TYPE);

        _context.reset(new ProcessorContext(&_emitter, _side_inputs, _types));
    }

    virtual void TearDown() {
    }

    std::vector<boost::python::object> _inputs;
    std::auto_ptr<flume::core::Iterator> _iterator0;
    std::auto_ptr<flume::core::Iterator> _iterator1;
    std::auto_ptr<flume::core::Iterator> _iterator2;
    std::vector<flume::core::Iterator*> _side_inputs;
    flume::MockEmitter<> _emitter;
    toft::scoped_ptr<ProcessorContext> _context;
    std::vector<PbDatasetType> _types;

};

TEST_F(ProcessorContextTest, TestPythonSideInput) {
    try {
        const std::vector<boost::python::object>& side_inputs = _context->python_side_inputs();
        ASSERT_EQ(3, side_inputs.size());
        boost::python::dict global;
        global["side_input"] = side_inputs[0];
        boost::python::object side_input = boost::python::eval("[i for i in side_input]", global);
        boost::python::list expect;
        expect.append(1);
        expect.append(2);
        ASSERT_EQ(2, boost::python::len(side_input));
        ASSERT_EQ(expect, side_input);

        global["side_input"] = side_inputs[1];
        side_input = boost::python::eval("[i for i in side_input]", global);
        expect.append("3");
        ASSERT_EQ(3, boost::python::len(side_input));
        ASSERT_EQ(expect, side_input);

        ASSERT_EQ(boost::python::object(1), side_inputs[2]);
    } catch(boost::python::error_already_set&) {
        PyErr_Print();
        ASSERT_TRUE(false);
    }
}

class TestProcessor : public Processor {
public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config) { }
    virtual void begin_group(const std::vector<toft::StringPiece>& keys, ProcessorContext*) { }
    virtual void process(void* object) { }
    virtual void end_group() { }
};

TEST(ProcessorTest, TestCompile) {
    try {
        PythonInterpreter::Instance(); // force setup env
        ProcessorImpl<TestProcessor> processor;
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
