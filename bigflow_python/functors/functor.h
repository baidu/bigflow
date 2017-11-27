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
// Author: Pan Yunhong (panyunhong@baidu.com)
//

#ifndef FUNCTOR_H_
#define FUNCTOR_H_

#include <string>
#include <vector>

#include "flume/core/emitter.h"

namespace baidu {
namespace bigflow {
namespace python {

// This is the base class of functors
// A functor can be used to process the data in transforms.
//
// If you want to define a new functor, there are 3 things you should do.
// 1. define a Functor by implement a subclass of this class
//    Eg. class MyFunctor : public Functor { ... };
// 2. register this functor in register.cpp
//    Eg. reg.add<Functor, MyFunctor>("MyFunctor");
// 3. define a Functor with same name in python:
//    Eg. class MyFunctor(entity.Functor):
//            def get_entity_config(self):
//                return 'Config of this functor, will be passed to the Setup method.'
// After doing this, you can use this MyFunctor() as a normal functor to process data, such as:
//    p.map(MyFunctor())
//
// There is a special functor, named PythonImplFunctor,
// used to convert a python function to a functor, is defined in python_impl_functor.h(.cpp).
class Functor {
public:
    // Setup for a functor
    virtual void Setup(const std::string& config){}

    // Call the functor
    // @param object: object is the input params of the functor.
    //                It's always a `PythonArgs*` object currently.
    //                more types may be supported in the future.
    // @param emitter: is used to emit data to downstream by calling emitter->Emit.
    //         Multiple records could be emitted in one single call(object, emitter)
    //         by calling emitter->Emit multiple times.
    virtual void call(void* object, flume::core::Emitter* emitter) = 0;
    virtual ~Functor() {}
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // FUNCTOR_H_
