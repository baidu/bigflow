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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include <fstream>  // NOLINT(readability/streams)
#include <iostream>

#include "bigflow_python/bucket_partitioner.h"
#include "flume/core/entity.h"
#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/sinker.h"
#include "flume/runtime/io/io_format.h"
#include "flume/util/reflection.h"
#include "bigflow_python/common/barshal_objector.h"
#include "bigflow_python/delegators/python_io_delegator.h"
#include "bigflow_python/delegators/python_key_reader_delegator.h"
#include "bigflow_python/delegators/python_objector_delegator.h"
#include "bigflow_python/delegators/python_partitioner_delegator.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/functors/cartesian_fn.h"
#include "bigflow_python/functors/extract_value_fn.h"
#include "bigflow_python/functors/full_join_fn.h"
#include "bigflow_python/functors/one_side_join_fn.h"
#include "bigflow_python/functors/python_impl_functor.h"
#include "bigflow_python/functors/serde_wrapper_fn.h"
#include "bigflow_python/functors/split_string_to_types.h"
#include "bigflow_python/functors/kv_serde_fn.h"
#include "bigflow_python/objectors/bool_serde.h"
#include "bigflow_python/objectors/chain_serde.h"
#include "bigflow_python/objectors/cpickle_objector.h"
#include "bigflow_python/objectors/dict_serde.h"
#include "bigflow_python/objectors/fields_serde.h"
#include "bigflow_python/objectors/float_serde.h"
#include "bigflow_python/objectors/int_serde.h"
#include "bigflow_python/objectors/list_serde.h"
#include "bigflow_python/objectors/optional_serde.h"
#include "bigflow_python/objectors/set_serde.h"
#include "bigflow_python/objectors/str_serde.h"
#include "bigflow_python/objectors/tuple_serde.h"
#include "bigflow_python/processors/accumulate_processor.h"
#include "bigflow_python/processors/combine_processor.h"
#include "bigflow_python/processors/filter_processor.h"
#include "bigflow_python/processors/flatmap_processor.h"
#include "bigflow_python/processors/flatten_processor.h"
#include "bigflow_python/processors/get_last_key_processor.h"
#include "bigflow_python/processors/pipe_processor.h"
#include "bigflow_python/processors/processor.h"
#include "bigflow_python/processors/reduce_processor.h"
#include "bigflow_python/processors/select_elements_processor.h"
#include "bigflow_python/processors/sort_optimize_processor.h"
#include "bigflow_python/processors/take_processor.h"
#include "bigflow_python/processors/transform_processor.h"
#include "bigflow_python/processors/count_processors.h"
#include "boost/shared_ptr.hpp"
#include "flume/runtime/common/memory_dataset.h"
#include "boost/python.hpp"
#include "toft/base/scoped_ptr.h"
#include "bigflow_python/delegators/python_time_reader_delegator.h"
#include "bigflow_python/processors/python_environment.h"
#include "flume/core/time_reader.h"
#include "flume/core/environment.h"
#include "bigflow_python/proto/python_resource.pb.h"
#include "flume/flume.h"

namespace baidu {
namespace bigflow {
namespace python {

namespace {

// a simple impl, just to show the registered items to stdout
class ReflectionRegister {
public:
    template<typename Base, typename T>
    void add(const std::string& key) {
        using flume::Reflection;
        std::string name = Reflection<Base>::template TypeName<T>();
        std::cout << key << "|" << name << std::endl;
    }

    void show() {}
};

}  // namespace

void register_classes() {
    using flume::core::Processor;
    using flume::core::KeyReader;
    using flume::core::Partitioner;
    using flume::core::Objector;
    using flume::core::Loader;
    using flume::core::Sinker;
    using flume::runtime::TextInputFormat;
    using flume::runtime::TextOutputFormat;
    using flume::runtime::SequenceFileAsBinaryInputFormat;
    using flume::runtime::SequenceFileAsBinaryOutputFormat;
    using flume::runtime::TextStreamInputFormat;
    using flume::runtime::SequenceStreamInputFormat;
    using flume::runtime::EmptyOutputFormat;
    using flume::runtime::RecordObjector;
    using flume::runtime::TextInputFormatWithUgi;
    using flume::runtime::SequenceFileAsBinaryInputFormatWithUgi;
    using flume::core::Environment;
    using flume::core::TimeReader;

    ReflectionRegister reflection;
    // Existience of following mentions makes Flume Reflection mechnism work.
    reflection.add<TimeReader, PythonTimeReaderDelegator>("PythonTimeReaderDelegator");
    reflection.add<Environment, PythonEnvironment>("PythonEnvironment");
    reflection.add<Processor, PythonProcessorDelegator>("PythonProcessorDelegator");
    reflection.add<Processor, PythonFromRecordProcessor>("PythonFromRecordProcessor");
    reflection.add<Processor, PythonKVFromRecordProcessor>("PythonKVFromRecordProcessor");
    reflection.add<Processor, PythonToRecordProcessor>("PythonToRecordProcessor");
    reflection.add<Processor, PythonKVToRecordProcessor>("PythonKVToRecordProcessor");
    reflection.add<KeyReader, PythonKeyReaderDelegator>("PythonKeyReaderDelegator");
    reflection.add<KeyReader, StrKeyReaderDelegator>("StrKeyReaderDelegator");
    reflection.add<Partitioner, PythonPartitionerDelegator>("PythonPartitionerDelegator");
    reflection.add<Partitioner, BucketPartitioner>("BucketPartitioner");
    reflection.add<Objector, BarshalObjector>("BarshalObjector");
    reflection.add<Objector, PythonObjectorDelegator>("PythonObjectorDelegator");
    reflection.add<Loader, PythonLoaderDelegator>("PythonLoaderDelegator");
    reflection.add<Sinker, PythonSinkerDelegator>("PythonSinkerDelegator");
    reflection.add<Objector, EncodePartitionObjector>("EncodePartitionObjector");
    reflection.add<Objector, RecordObjector>("RecordObjector");
    reflection.add<Loader, TextInputFormat>("TextInputFormat");
    reflection.add<Loader, TextInputFormatWithUgi>("TextInputFormatWithUgi");
    reflection.add<Sinker, TextOutputFormat>("TextOutputFormat");
    reflection.add<Loader, SequenceFileAsBinaryInputFormat>("SequenceFileAsBinaryInputFormat");
    reflection.add<Loader, SequenceFileAsBinaryInputFormatWithUgi>(
            "SequenceFileAsBinaryInputFormatWithUgi");
    reflection.add<Sinker, SequenceFileAsBinaryOutputFormat>("SequenceFileAsBinaryOutputFormat");
    reflection.add<Loader, TextStreamInputFormat>("TextStreamInputFormat");
    reflection.add<Loader, SequenceStreamInputFormat>("SequenceStreamInputFormat");
    reflection.add<Sinker, EmptyOutputFormat>("EmptyOutputFormat");
    reflection.add<Processor, ProcessorImpl<FlatMapProcessor> >("FlatMapProcessor");
    reflection.add<Processor, ProcessorImpl<FlattenProcessor> >("FlattenProcessor");
    reflection.add<Processor, ProcessorImpl<FilterProcessor> >("FilterProcessor");
    reflection.add<Processor, ProcessorImpl<CombineProcessor, 0> >("CombineProcessor");
    reflection.add<Processor, ProcessorImpl<CountProcessor> >("CountProcessor");
    reflection.add<Processor, ProcessorImpl<SumProcessor> >("SumProcessor");
    reflection.add<Processor, ProcessorImpl<ReduceProcessor> >("ReduceProcessor");
    reflection.add<Processor, ProcessorImpl<AccumulateProcessor> >("AccumulateProcessor");
    reflection.add<Processor, ProcessorImpl<TakeProcessor> >("TakeProcessor");
    reflection.add<Processor, ProcessorImpl<SelectElementsProcessor> >("SelectElementsProcessor");
    reflection.add<Processor, ProcessorImpl<TransformProcessor> >("TransformProcessor");
    reflection.add<Processor, ProcessorImpl<GetLastKeyProcessor> >("GetLastKeyProcessor");
    reflection.add<Processor, ProcessorImpl<SetKeyToValueProcessor> >("SetKeyToValueProcessor");
    reflection.add<Processor, SetValueNoneProcessor>("SetValueNoneProcessor");
    reflection.add<Processor, ProcessorImpl<PipeProcessor> >("PipeProcessor");

    reflection.add<Functor, SplitStringToTypes>("SplitStringToTypes");
    reflection.add<Functor, SerdeWrapperFn>("SerdeWrapper");
    //reflection.add<Functor, KVSerdeFn>("KVSerdeFn");
    reflection.add<Functor, KVSerializeFn>("KVSerializeFn");
    reflection.add<Functor, KVDeserializeFn>("KVDeserializeFn");
    reflection.add<Functor, PythonImplFunctor>("PythonImplFunctor");
    reflection.add<Functor, ExtractValueFn>("ExtractValueFn");
    reflection.add<Functor, FullJoinInitializeFn>("FullJoinInitializeFn");
    reflection.add<Functor, FullJoinTransformFn>("FullJoinTransformFn");
    reflection.add<Functor, FullJoinFinalizeFn>("FullJoinFinalizeFn");
    reflection.add<Functor, OneSideJoinFn>("OneSideJoinFn");
    reflection.add<Functor, CartesianFn>("CartesianFn");
    reflection.add<Objector, PythonObjectorImpl<StrSerde> >("StrSerde");
    reflection.add<Objector, PythonObjectorImpl<IntSerde> >("IntSerde");
    reflection.add<Objector, PythonObjectorImpl<BoolSerde> >("BoolSerde");
    //reflection.add<Objector, PythonObjectorImpl<ProtobufSerde> >("ProtobufSerde");
    reflection.add<Objector, PythonObjectorImpl<FieldsSerde> >("FieldsDictSerde");
    reflection.add<Objector, PythonObjectorImpl<OptionalSerde> >("Optional");
    reflection.add<Objector, PythonObjectorImpl<TupleSerde> >("TupleSerde");
    reflection.add<Objector, PythonObjectorImpl<ChainSerde> >("ChainSerde");
    reflection.add<Objector, PythonObjectorImpl<ListSerde> >("ListSerde");
    reflection.add<Objector, PythonObjectorImpl<DictSerde> >("DictSerde");
    reflection.add<Objector, PythonObjectorImpl<SetSerde> >("SetSerde");
    reflection.add<Objector, PythonObjectorImpl<FloatSerde> >("FloatSerde");
    reflection.add<Objector, PythonObjectorImpl<CPickleObjector> >("CPickleSerde");

   // #define ADD_CPP_TYPE_SERDE(cpp_type) \
   // reflection.add<Objector, bigflow::ObjectorOf<cpp_type>::type >("CppTypeSerde_" #cpp_type)

    //ADD_CPP_TYPE_SERDE(uint64_t);

    //#undef ADD_CPP_TYPE_SERDE

    reflection.show();
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

