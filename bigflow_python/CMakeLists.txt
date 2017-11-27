ADD_SUBDIRECTORY("proto")

if(APPLE)
  SET(CMAKE_EXE_LINKER_FLAGS "-lpthread -L/usr/local/opt/openssl/lib -lssl -lcrypto -ldl -lz -L${PYTHON_INSTALL_DIR}/lib -lpython2.7")
ELSE()
  SET(CMAKE_EXE_LINKER_FLAGS "-lpthread -lssl -lcrypto -ldl -lz -L${PYTHON_INSTALL_DIR}/lib -lpython2.7")
ENDIF()

cc_library(bigflow_python-register SRCS ../bigflow_python/register.cpp DEPS bigflow_python-proto-python_resource flume-util-reflection flume-runtime-io-io_format flume-core-entity bigflow_python-processors-transform_processor bigflow_python-processors-take_processor bigflow_python-processors-sort_optimize_processor bigflow_python-processors-select_elements_processor bigflow_python-processors-reduce_processor bigflow_python-processors-python_environment bigflow_python-processors-pipe_processor bigflow_python-processors-get_last_key_processor bigflow_python-processors-flatten_processor bigflow_python-processors-flatmap_processor bigflow_python-processors-filter_processor bigflow_python-processors-combine_processor bigflow_python-processors-accumulate_processor bigflow_python-objectors-tuple_serde bigflow_python-objectors-str_serde bigflow_python-objectors-set_serde bigflow_python-objectors-optional_serde bigflow_python-objectors-list_serde bigflow_python-objectors-int_serde  bigflow_python-objectors-float_serde bigflow_python-objectors-dict_serde bigflow_python-objectors-cpickle_objector bigflow_python-objectors-chain_serde bigflow_python-objectors-bool_serde bigflow_python-functors-split_string_to_types bigflow_python-functors-serde_wrapper_fn bigflow_python-functors-python_impl_functor bigflow_python-functors-one_side_join_fn bigflow_python-functors-kv_serde_fn bigflow_python-functors-full_join_fn bigflow_python-functors-extract_value_fn bigflow_python-functors-cartesian_fn bigflow_python-delegators-python_time_reader_delegator bigflow_python-delegators-python_processor_delegator bigflow_python-delegators-python_partitioner_delegator  bigflow_python-delegators-python_objector_delegator bigflow_python-delegators-python_key_reader_delegator bigflow_python-delegators-python_io_delegator bigflow_python-common-barshal_objector bigflow_python-processors-count_processors bigflow_python-bucket_partitioner LINK_ALL_SYMBOLS "true")

cc_library(bigflow_python-python_client SRCS ../bigflow_python/python_client.cpp DEPS flume-util-reflection flume-runtime-task flume-runtime-local-local_executor_factory flume-runtime-local-local_backend flume-runtime-io-io_format flume-planner-local-local_planner flume-flume flume-core-entity bigflow_python-register bigflow_python-proto-python_resource bigflow_python-bucket_partitioner)

cc_library(bigflow_python-python_interpreter LINK_ALL_SYMBOLS "true" SRCS ../bigflow_python/python_interpreter.cpp DEPS toft flume-runtime-spark-flags flume-runtime-io-io_format flume-runtime-backend bigflow_python-python bigflow_python-delegators-python_processor_delegator bigflow_python-delegators-python_objector_delegator bigflow_python-proto-processor bigflow_python-delegators-python_key_reader_delegator flume-runtime-local-flags)

cc_library(bigflow_python-python DEPS boost )
TARGET_LINK_LIBRARIES(bigflow_python-python python -lutil)

cc_library(bigflow_python-bigflow SRCS ../bigflow_python/bigflow.cpp DEPS flume-flume)

cc_library(bigflow_python-bucket_partitioner SRCS ../bigflow_python/bucket_partitioner.cpp DEPS toft glog gflags bigflow_python-proto-entity_config)

cc_library(bigflow_python-processors-processor SRCS ../bigflow_python/processors/processor.cpp DEPS bigflow_python-python_interpreter bigflow_python-proto-processor bigflow_python-delegators-python_processor_delegator bigflow_python-common-python)

cc_test(bigflow_python-processors-processor_test SRCS ../bigflow_python/processors/processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-processor)

cc_library(bigflow_python-processors-python_environment SRCS ../bigflow_python/processors/python_environment.cpp DEPS glog gflags boost)

cc_library(bigflow_python-processors-flatmap_processor SRCS ../bigflow_python/processors/flatmap_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-flatmap_processor_test SRCS ../bigflow_python/processors/flatmap_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-flatmap_processor)

cc_library(bigflow_python-processors-filter_processor SRCS ../bigflow_python/processors/filter_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-filter_processor_test SRCS ../bigflow_python/processors/filter_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-filter_processor)

cc_library(bigflow_python-processors-accumulate_processor SRCS ../bigflow_python/processors/accumulate_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-accumulate_processor_test SRCS ../bigflow_python/processors/accumulate_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-accumulate_processor)

cc_library(bigflow_python-processors-combine_processor SRCS ../bigflow_python/processors/combine_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-combine_processor_test SRCS ../bigflow_python/processors/combine_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-combine_processor)

cc_library(bigflow_python-processors-reduce_processor SRCS ../bigflow_python/processors/reduce_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-reduce_processor_test SRCS ../bigflow_python/processors/reduce_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-reduce_processor)

cc_library(bigflow_python-processors-take_processor SRCS ../bigflow_python/processors/take_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_library(bigflow_python-processors-sort_optimize_processor SRCS ../bigflow_python/processors/sort_optimize_processor.cpp DEPS bigflow_python-processors-processor)

cc_test(bigflow_python-processors-take_processor_test SRCS ../bigflow_python/processors/take_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-take_processor)

cc_library(bigflow_python-processors-transform_processor SRCS ../bigflow_python/processors/transform_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-transform_processor_test SRCS ../bigflow_python/processors/transform_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-transform_processor)

cc_library(bigflow_python-processors-select_elements_processor SRCS ../bigflow_python/processors/select_elements_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-select_elements_processor_test SRCS ../bigflow_python/processors/select_elements_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-select_elements_processor)

cc_library(bigflow_python-processors-flatten_processor SRCS ../bigflow_python/processors/flatten_processor.cpp DEPS bigflow_python-processors-processor bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-processors-flatten_processor_test SRCS ../bigflow_python/processors/flatten_processor_test.cpp DEPS gtest gmock glog gflags bigflow_python-processors-flatten_processor)

cc_library(bigflow_python-processors-get_last_key_processor SRCS ../bigflow_python/processors/get_last_key_processor.cpp DEPS bigflow_python-processors-processor)

cc_library(bigflow_python-processors-pipe_processor SRCS ../bigflow_python/processors/pipe_processor.cpp DEPS toft bigflow_python-processors-processor)

cc_library(bigflow_python-processors-count_processors SRCS ../bigflow_python/processors/count_processors.cpp DEPS bigflow_python-common-python)

cc_library(bigflow_python-serde-cpickle_serde SRCS ../bigflow_python/serde/cpickle_serde.cpp DEPS boost)

cc_library(bigflow_python-delegators-python_processor_delegator SRCS ../bigflow_python/delegators/python_processor_delegator.cpp DEPS toft glog gflags bigflow_python-serde-cpickle_serde bigflow_python-common-python)

cc_library(bigflow_python-delegators-python_key_reader_delegator SRCS ../bigflow_python/delegators/python_key_reader_delegator.cpp DEPS glog gflags bigflow_python-serde-cpickle_serde thrift toft)

cc_library(bigflow_python-delegators-python_time_reader_delegator SRCS ../bigflow_python/delegators/python_time_reader_delegator.cpp DEPS glog gflags flume-runtime-backend flume-proto-entity flume-core-entity bigflow_python-serde-cpickle_serde bigflow_python-python_interpreter bigflow_python-python bigflow_python-functors-py_functor_caller)

cc_test(bigflow_python-delegators-python_time_reader_delegator_test SRCS ../bigflow_python/delegators/python_time_reader_delegator_test.cpp DEPS gtest gmock glog gflags bigflow_python-delegators-python_time_reader_delegator)

cc_library(bigflow_python-delegators-python_partitioner_delegator SRCS ../bigflow_python/delegators/python_partitioner_delegator.cpp DEPS glog gflags bigflow_python-serde-cpickle_serde)

cc_library(bigflow_python-delegators-python_objector_delegator SRCS ../bigflow_python/delegators/python_objector_delegator.cpp DEPS glog gflags bigflow_python-serde-cpickle_serde)

cc_library(bigflow_python-delegators-python_io_delegator SRCS ../bigflow_python/delegators/python_io_delegator.cpp DEPS glog gflags bigflow_python-serde-cpickle_serde)

#cc_library(bigflow_python-delegators-python_parquet_io_delegator SRCS ../bigflow_python/delegators/python_parquet_io_delegator.cpp DEPS glog gflags flume-runtime-io-parquet_io_format bigflow_python-serde-cpickle_serde bigflow_python-delegators-python_processor_delegator bigflow_python-common-python bigflow_python-adapters-python_arrow_adapter)

#cc_library(bigflow_python-adapters-python_arrow_adapter SRCS ../bigflow_python/adapters/builtin_converter.cpp DEPS svn-baidu-bigflow-third-party-arrow-arrow glog gflags boost bigflow_python-python bigflow_python-common-python)

#cc_test(bigflow_python-adapters-python_arrow_adapter_test SRCS ../bigflow_python/adapters/builtin_converter_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-adapters-python_arrow_adapter)

cc_library(bigflow_python-objectors-str_serde SRCS ../bigflow_python/objectors/str_serde.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-int_serde SRCS ../bigflow_python/objectors/int_serde.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-bool_serde SRCS ../bigflow_python/objectors/bool_serde.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-float_serde SRCS ../bigflow_python/objectors/float_serde.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-list_serde SRCS ../bigflow_python/objectors/list_serde.cpp DEPS bigflow_python-python_interpreter bigflow_python-objectors-serde_impl_wrapper)

cc_library(bigflow_python-objectors-tuple_serde SRCS ../bigflow_python/objectors/tuple_serde.cpp DEPS bigflow_python-python_interpreter bigflow_python-objectors-serde_impl_wrapper)

cc_library(bigflow_python-objectors-chain_serde SRCS ../bigflow_python/objectors/chain_serde.cpp DEPS bigflow_python-python_interpreter bigflow_python-objectors-serde_impl_wrapper)

#cc_library(bigflow_python-objectors-idl_packet_serde SRCS ../bigflow_python/objectors/packet.idl.cpp ../bigflow_python/objectors/idl_packet_serde.cpp DEPS svn-public-uconv-uconv svn-public-mcpack-mcpack svn-public-idlcompiler-idlcompiler svn-public-bslext-bslext bigflow_python-python_interpreter bigflow_python-objectors-serde_impl_wrapper)

cc_library(bigflow_python-objectors-serde_impl_wrapper SRCS ../bigflow_python/objectors/serde_impl_wrapper.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-dict_serde SRCS ../bigflow_python/objectors/dict_serde.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-set_serde SRCS ../bigflow_python/objectors/set_serde.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-objectors-optional_serde SRCS ../bigflow_python/objectors/optional_serde.cpp DEPS flume-core-entity bigflow_python-python_interpreter)

cc_test(bigflow_python-objectors-str_serde_test SRCS ../bigflow_python/objectors/str_serde_test.cpp DEPS gtest gmock glog gflags flume-core-entity bigflow_python-objectors-str_serde)

cc_test(bigflow_python-objectors-int_serde_test SRCS ../bigflow_python/objectors/int_serde_test.cpp DEPS gtest gmock glog gflags flume-core-entity bigflow_python-objectors-int_serde)

cc_test(bigflow_python-objectors-bool_serde_test SRCS ../bigflow_python/objectors/bool_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-objectors-bool_serde)

cc_test(bigflow_python-objectors-float_serde_test SRCS ../bigflow_python/objectors/float_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-objectors-float_serde)

cc_test(bigflow_python-objectors-tuple_serde_test SRCS ../bigflow_python/objectors/tuple_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-python_client bigflow_python-common-python bigflow_python-register)

cc_test(bigflow_python-objectors-set_serde_test SRCS ../bigflow_python/objectors/set_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-python_client bigflow_python-objectors-set_serde bigflow_python-objectors-cpickle_objector bigflow_python-common-python)

cc_test(bigflow_python-objectors-fields_serde_test SRCS ../bigflow_python/objectors/fields_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-python_client bigflow_python-common-python)

#cc_test(bigflow_python-objectors-idl_packet_serde_test SRCS ../bigflow_python/objectors/idl_packet_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-python_client bigflow_python-common-python)

cc_test(bigflow_python-objectors-chain_serde_test SRCS ../bigflow_python/objectors/chain_serde_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-python_client bigflow_python-common-python)

cc_library(bigflow_python-objectors-cpickle_objector SRCS ../bigflow_python/objectors/cpickle_objector.cpp DEPS bigflow_python-python_interpreter)

cc_library(bigflow_python-functors-split_string_to_types SRCS ../bigflow_python/functors/split_string_to_types.cpp DEPS glog gflags boost)

cc_library(bigflow_python-functors-python_impl_functor SRCS ../bigflow_python/functors/python_impl_functor.cpp DEPS bigflow_python-common-python)

cc_library(bigflow_python-functors-py_functor_caller SRCS ../bigflow_python/functors/py_functor_caller.cpp DEPS toft boost)

cc_library(bigflow_python-functors-one_side_join_fn SRCS ../bigflow_python/functors/one_side_join_fn.cpp DEPS glog gflags boost)

cc_test(bigflow_python-functors-one_side_join_fn_test SRCS ../bigflow_python/functors/one_side_join_fn_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-functors-py_functor_caller bigflow_python-functors-one_side_join_fn bigflow_python-common-python)

cc_library(bigflow_python-functors-cartesian_fn SRCS ../bigflow_python/functors/cartesian_fn.cpp DEPS bigflow_python-delegators-python_processor_delegator)

cc_test(bigflow_python-functors-cartesian_fn_test SRCS ../bigflow_python/functors/cartesian_fn_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-functors-py_functor_caller bigflow_python-functors-cartesian_fn bigflow_python-common-python)

cc_library(bigflow_python-functors-full_join_fn SRCS ../bigflow_python/functors/full_join_fn.cpp DEPS glog gflags boost)

cc_test(bigflow_python-functors-full_join_fn_test SRCS ../bigflow_python/functors/full_join_fn_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-functors-py_functor_caller bigflow_python-functors-full_join_fn bigflow_python-common-python)

cc_library(bigflow_python-functors-extract_value_fn SRCS ../bigflow_python/functors/extract_value_fn.cpp DEPS bigflow_python-common-python)

cc_test(bigflow_python-functors-extract_value_fn_test SRCS ../bigflow_python/functors/extract_value_fn_test.cpp DEPS gtest gmock glog gflags bigflow_python-python_interpreter bigflow_python-functors-py_functor_caller bigflow_python-functors-extract_value_fn bigflow_python-common-python bigflow_python-python)

cc_library(bigflow_python-functors-serde_wrapper_fn SRCS ../bigflow_python/functors/serde_wrapper_fn.cpp DEPS bigflow_python-common-python)

cc_library(bigflow_python-functors-kv_serde_fn SRCS ../bigflow_python/functors/kv_serde_fn.cpp DEPS bigflow_python-common-python)

cc_library(bigflow_python-common-python SRCS ../bigflow_python/common/python.cpp DEPS toft boost)

cc_library(bigflow_python-common-barshal_objector SRCS ../bigflow_python/common/barshal_objector.cpp DEPS bigflow_python-python_interpreter)

cc_test(bigflow_python-common-barshal_objector_test SRCS ../bigflow_python/common/barshal_objector_test.cpp DEPS gtest gmock glog gflags bigflow_python-common-barshal_objector)

IF(NOT APPLE)
  cc_binary(bigflow_python-rpc-bigflow_server SRCS ../bigflow_python/rpc/bigflow_server.cpp DEPS toft gflags flume-util-config_util flume-util-reflection flume-runtime-io-gzip_file bigflow_python-register flume-runtime-spark-spark_backend flume-runtime-platform-hadoop_client flume-runtime-local-local_backend flume-runtime-backend flume-flume bigflow_python-python_interpreter bigflow_python-python_client bigflow_python-proto-service bigflow_python-proto-pipeline_proto bigflow_python-objectors-cpickle_objector bigflow_python-bigflow leveldb flume-runtime-counter brpc protobuf)
  TARGET_LINK_LIBRARIES(bigflow_python-rpc-bigflow_server -L$ENV{LIB_JVM_PATH} -ljvm)
ENDIF()
#cc_library(bflpyrt LINK_ALL_SYMBOLS "true"  SRCS ../bigflow_python/bigflow_python_runtime.cpp DEPS python  bigflow_python-register bigflow_python-python_interpreter flume-runtime-spark-jni-jni SHARED)
cc_library(bflpyrt  LINK_ALL_SYMBOLS "true" SRCS ../bigflow_python/bigflow_python_runtime.cpp ALL_SYMBOLS_DEPS toft flume-util-reflection_test_helper flume-util-reflection_dlopen_test_helper flume-runtime-spark-spark_task flume-runtime-spark-spark_backend flume-runtime-spark-jni-jni_buffer flume-runtime-spark-jni-jni_task flume-runtime-io-gzip_file flume-runtime-local-local_backend  bigflow_python-register bigflow_python-python_interpreter DEPS flume-runtime-spark-jni-jni SHARED)

if(APPLE)
  TARGET_LINK_LIBRARIES(bflpyrt -L/usr/local/opt/openssl/lib -lssl -L$ENV{JAVA_HOME}/jre/lib/server -ljvm)
ENDIF()

ADD_CUSTOM_TARGET(release
  COMMAND cd ${PROJECT_SOURCE_DIR}/bigflow_python &&
  export PATH=${PROTOBUF_INSTALL_DIR}/bin:$ENV{PATH} &&
  export BINARY_DIR=${PROJECT_BINARY_DIR}/ &&
  sh prepare.sh && sh release.sh
)
