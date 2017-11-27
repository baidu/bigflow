#!/bin/bash

source "$CUR/bigflow-env.sh"

# set Bigflow Python
BIGFLOW_PYTHON_HOME=${BIGFLOW_PYTHON_HOME-$CUR/../..}
# echo "BIGFLOW_PYTHON_HOME: ${BIGFLOW_PYTHON_HOME}" >&2

BIGFLOW_SERVER_PATH=${BIGFLOW_SERVER_PATH-$BIGFLOW_PYTHON_HOME/flume/worker}
# echo "BIGFLOW_SERVER_PATH: ${BIGFLOW_SERVER_PATH}" >&2

BIGFLOW_LOG_FILE=${BIGFLOW_LOG_FILE-""}

if [ -z ${BIGFLOW_LOG_FILE_BACKEND+x} ]; then
    if [ -z ${BIGFLOW_LOG_FILE} ]; then
        BIGFLOW_LOG_FILE_BACKEND=${PWD}/log/bigflow_backend.$$
    else
        BIGFLOW_LOG_FILE_BACKEND=${BIGFLOW_LOG_FILE}_backend
    fi
fi

if [[ "true" = "${BIGFLOW_PTYPE_GET_ON_STR}" ]]; then
    # echo "BIGFLOW_PTYPE_GET_ON_STR enabled" >&2
    export BIGFLOW_PTYPE_GET_ON_STR
fi

BIGFLOW_AUTO_UPDATE=${BIGFLOW_AUTO_UPDATE-true}
if [[ "true" != "${BIGFLOW_AUTO_UPDATE}" ]]; then
    echo "BIGFLOW_AUTO_UPDATE disabled" >&2
fi

BIGFLOW_IGNORE_CURL_ERROR=${BIGFLOW_IGNORE_CURL_ERROR-true}
if [[ "true" != "${BIGFLOW_IGNORE_CURL_ERROR}" ]]; then
    echo "BIGFLOW_IGNORE_CURL_ERROR disabled" >&2
fi

export BIGFLOW_PYTHON_HOME
export BIGFLOW_SERVER_PATH
export BIGFLOW_TEMP_PATH_HDFS
export BIGFLOW_REPREPARE_CACHE_ARCHIVE
export BIGFLOW_LOG_FILE
export BIGFLOW_LOG_FILE_BACKEND

export LC_ALL=C

# for JAVA_HOME
if [ -z $JAVA_HOME ]; then
        echo "Error: please set JAVA_HOME" >&2
        exit 1
fi
# for Hadoop classpath
if [ -z $HADOOP_HOME ]; then
    if [ ! -z $HX_HADOOP_HOME ]; then
        # the default hadoop_home deployed by matrix-bois.
        echo "HADOOP_HOME not set, but HX_HADOOP_HOME set, and use ${HX_HADOOP_HOME} as HADOOP_HOME to run bigflow" >&2
        export HADOOP_HOME=$HX_HADOOP_HOME
    else
        echo "Error: please set HADOOP_HOME" >&2
        exit 1
    fi
fi

#export JAVA_HOME=${HADOOP_HOME}/../java6

CLASSPATH=${HADOOP_HOME}/etc/hadoop
CLASSPATH=${CLASSPATH}:${JAVA_HOME}/lib/tools.jar
CLASSPATH=${CLASSPATH}:${HADOOP_HOME}

#for path in `ls ${HADOOP_HOME}/hadoop-2-*.jar`
#do
#   CLASSPATH=${CLASSPATH}:$path
#done
#for path in `find ${HADOOP_HOME}/lib/ -name "*.jar"`
#do
#   CLASSPATH=${CLASSPATH}:$path
#done

LIBRARY_PATH=$CUR/:${JAVA_HOME}/jre/lib/amd64:${JAVA_HOME}/jre/lib/amd64/native_threads:${JAVA_HOME}/jre/lib/amd64/server
export CLASSPATH=${CLASSPATH}

export HADOOP_LIB_DIR=$HADOOP_HOME/lib

# for HCE
#LIBRARY_PATH=${LIBRARY_PATH}:${HADOOP_HOME}/libhdfs:${HADOOP_HOME}/lib/native/Linux-amd64-64:
#LIBRARY_PATH=${LIBRARY_PATH}:${HADOOP_HOME}/libhdfs:${HADOOP_HOME}/libhce/lib

# for flume
#nm $HADOOP_HOME/libhce/lib/libhce.so | grep gflags > /dev/null
#if [ $? -eq 0 ]; then
#    LIBRARY_PATH=${LIBRARY_PATH}:${BIGFLOW_PYTHON_HOME}/flume/fake_thirdlib
#else
#    LIBRARY_PATH=${LIBRARY_PATH}:${BIGFLOW_PYTHON_HOME}/flume/googlelib
#fi

# set CPython
PYTHON_DEPEND_DIR=$BIGFLOW_PYTHON_HOME/bigflow/depend/protobuf-3.0.0-py2.7.egg
PROTO_JSON_DIR=$BIGFLOW_PYTHON_HOME/thirdparty/protobuf-json

export PYTHONHOME="$BIGFLOW_PYTHON_HOME/python_runtime"
export PYTHONPATH=$BIGFLOW_PYTHON_HOME:$PYTHON_DEPEND_DIR:.:$PROTO_JSON_DIR:$PYTHONPATH
export LIBRARY_PATH=${LIBRARY_PATH}:$PYTHONHOME/solib:$PYTHONHOME/lib

# LD_LIBRARY_PATH
export LD_LIBRARY_PATH=${LIBRARY_PATH}:${LD_LIBRARY_PATH}

# echo "PYTHONPATH: ${PYTHONPATH}" >&2
export PATH="$PYTHONHOME/bin:$PATH"

