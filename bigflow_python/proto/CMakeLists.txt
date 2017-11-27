INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
proto_library(bigflow_python-proto-python_resource SRCS python_resource.proto DEPS glog gflags protobuf)

proto_library(bigflow_python-proto-processor SRCS processor.proto DEPS glog gflags flume-proto-entity protobuf)

proto_library(bigflow_python-proto-entity_config SRCS entity_config.proto DEPS glog gflags protobuf)

proto_library(bigflow_python-proto-pipeline_proto SRCS pipeline.proto DEPS glog gflags DEPS flume-proto-config protobuf)

proto_library(bigflow_python-proto-service SRCS service.proto DEPS glog gflags bigflow_python-proto-pipeline_proto bigflow_python-proto-python_resource flume-proto-logical_plan flume-proto-config protobuf)
