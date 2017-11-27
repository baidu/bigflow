export WORK_ROOT=$(cd `dirname "$0"`;pwd)
PROTOC="protoc"

gen_proto() {
    $PROTOC "${WORK_ROOT}/../$1" --python_out="${WORK_ROOT}/python/" --proto_path="${WORK_ROOT}/.."
}

# Flume proto
gen_proto "flume/proto/entity.proto"
echo "gen entity_pb2.py"

gen_proto "flume/proto/logical_plan.proto"
echo "gen logical_plan_pb2.py"

gen_proto "flume/proto/config.proto"
echo "gen config_pb2.py"

# Flume proto -- gen __init__.py
touch $WORK_ROOT/python/flume/__init__.py
touch  $WORK_ROOT/python/flume/proto/__init__.py

# Bigflow
gen_proto "bigflow_python/proto/pipeline.proto"
echo "gen pipeline_pb2.py"

gen_proto "bigflow_python/proto/entity_config.proto"
echo "gen entity_config_pb2.proto"

# Bigflow proto -- gen __init__.py
touch $WORK_ROOT/python/bigflow_python/__init__.py
touch $WORK_ROOT/python/bigflow_python/proto/__init__.py

# Bigflow Python
gen_proto "bigflow_python/proto/python_resource.proto"
echo "gen python_resource_pb2.py"

gen_proto "bigflow_python/proto/service.proto"
echo "gen service_pb2.py"

gen_proto "bigflow_python/proto/processor.proto"
echo "gen processor_pb2.py"

gen_proto "bigflow_python/proto/types.proto"
echo "gen types_pb2.py"

## gen bigpipe idl packet for logagent
#IDL_MCY=${WORK_ROOT}/../svn/public/idlcompiler/output/bin/mcy
#gen_idl_packet() {
#    local dir_path="${WORK_ROOT}/../$1"
#    local file_name=$2
#    echo "$dir_path/$file_name"
#    $IDL_MCY  "$dir_path/$file_name" --language cxx --compack --astyle
#    mv $file_name.h $file_name.cpp $dir_path
#}
#
#gen_idl_packet "bigflow_python/objectors/" "packet.idl"
#echo "gen_idl_packet done"
