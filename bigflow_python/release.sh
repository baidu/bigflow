#!/bin/bash

WORK_ROOT=`dirname "$0"`
WORK_ROOT=`cd $WORK_ROOT;pwd`
VERSION="1.0.0.0"

DATE=`date '+%Y_%m_%d_%H_%M_%S'`
# if publish, delete `_$DATE`, BIGFLOW_VERSION=${VERSION}
BIGFLOW_VERSION=${VERSION}_$DATE

binary_dir=${BINARY_DIR:-${WORK_ROOT}}

cp ${binary_dir}/bigflow_python/bigflow_python-rpc-bigflow_server ${WORK_ROOT}/python/flume/worker
cp ${binary_dir}/bigflow_python/libbflpyrt.so ${WORK_ROOT}/python/flume/

#TODO auto find path
cp  /usr/local/lib/libiconv.so.* ${WORK_ROOT}/python/python_runtime/lib/

find ${WORK_ROOT}/python -name '.flume-*' | xargs rm -rf
find ${WORK_ROOT}/python -name 'BUILD' | xargs rm -rf
find ${WORK_ROOT}/python -name '.tmp' | xargs rm -rf
find ${WORK_ROOT}/python -name 'entity-*' | xargs rm -rf
find ${WORK_ROOT}/python -name 'bigflow_python_*.tar.gz' | xargs rm -rf
find . -regex '.*core\.[0-9]+' | xargs rm -f

mkdir -p ${WORK_ROOT}/output
rm -f ${WORK_ROOT}/output/bigflow_python.tar.gz

cd ${WORK_ROOT}/python/bigflow/bin
CUR=$PWD
source ./set-env.sh

../../flume/worker show_registered_class_name > ${WORK_ROOT}/python/bigflow/names.cfg
cat  ${WORK_ROOT}/python/bigflow/names.cfg | awk -F '|' '{print $1"=\""$2"\""}' > ${WORK_ROOT}/python/bigflow/core/entity_names.py

git log -1 --pretty="%H" > ${WORK_ROOT}/python/commit-version

cd ${WORK_ROOT}

echo "bigflow_version = \"${BIGFLOW_VERSION}\"" > ${WORK_ROOT}/python/bigflow/version.py

tar czf ${WORK_ROOT}/output/bigflow_python.tar.gz -C ${WORK_ROOT}/python .

