#!/bin/bash
export LC_ALL=C
cd `dirname $0`/../../

if [ -f ${JAVA_HOME}/jre/lib/amd64/server/libjvm.so ];
then
    ARCH=amd64
elif [ -f ${JAVA_HOME}/jre/lib/i386/server/libjvm.so ];
then
    ARCH=i386
else
    echo "Fatal: Unkown arch or ${JAVA_HOME}/jre/lib/.../server/libjvm.so does not exist."
    return 1
fi

JAVA_LIB_HOME=${JAVA_HOME}/jre/lib/${ARCH}

export HADOOP_HOME=$HADOOP_HOME
export DEFAULT_HADOOP_CONF_PATH=${HADOOP_HOME}/etc/hadoop/core-site.xml
export HADOOP_LIB_DIR=${HADOOP_HOME}/lib

LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${JAVA_LIB_HOME}
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${JAVA_LIB_HOME}/native_threads
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${JAVA_LIB_HOME}/server
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/libhdfs
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/Linux-amd64-64
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/libhce/lib
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:`pwd`/thirdparty/googlelib

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}

export LIBRARY_PATH=${HADOOP_HOME}/libhce/lib:`pwd`/thirdparty/googlelib
export LIBHDFS_OPTS="-D{dfs.client.slow.write.limit}={1048576}"

cd bigflow_python

export HDFS_CONF_PATH=${HADOOP_HOME}/etc/hadoop/core-site.xml
# use HDFS conf to be default hadoop conf
export HADOOP_CONF_PATH=${HDFS_CONF_PATH}
export HDFS_ROOT_PATH='hdfs:///app/dc/bigflow/integrated_test_local/'

$HADOOP_HOME/bin/hadoop fs -conf $HDFS_CONF_PATH -rmr $HDFS_ROOT_PATH

cd python && sh run-tests
