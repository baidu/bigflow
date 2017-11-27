INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
proto_library(flume-proto-entity SRCS entity.proto DEPS glog gflags protobuf)

proto_library(flume-proto-logical_plan SRCS logical_plan.proto DEPS flume-proto-entity protobuf)

proto_library(flume-proto-split SRCS split.proto DEPS glog gflags protobuf)

proto_library(flume-proto-transfer_encoding SRCS transfer_encoding.proto DEPS glog gflags protobuf)

proto_library(flume-proto-config SRCS config.proto DEPS glog gflags protobuf)

proto_library(flume-proto-physical_plan SRCS physical_plan.proto DEPS flume-proto-transfer_encoding flume-proto-logical_plan flume-proto-entity flume-proto-config protobuf)

proto_library(flume-proto-custom_split SRCS custom_split.proto DEPS flume-proto-split protobuf)
