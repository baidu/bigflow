WORK_ROOT=".."
protoc "${WORK_ROOT}/../bigflow_python/proto/service.proto" --grpc_out="${WORK_ROOT}/../build64_release/bigflow_python/proto/" --cpp_out="${WORK_ROOT}/../build64_release/bigflow_python/proto/" --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` --proto_path="${WORK_ROOT}/.."
