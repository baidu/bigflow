#!/bin/bash
date 1>&2

$HADOOP_HOME/bin/hadoop version 1>&2

# set CLASSPATH and LD_LIBRARY_PATH
for jar in $(find $PWD/javalib -follow -name '*.jar'); do
    CLASSPATH=$CLASSPATH:$jar
done

if [ -d "$PWD/pythonlib" ]; then
    for egg in $(find $PWD/pythonlib -follow -name '*.egg'); do
        PYTHONPATH=$egg:$PYTHONPATH
    done
fi

date 1>&2

nm $HADOOP_HOME/libhce/lib/libhce.so | grep gflags > /dev/null
date 1>&2

if [ $? -eq 0 ]; then
    LD_LIBRARY_PATH=$PWD/fake_thirdlib:$LD_LIBRARY_PATH:$HADOOP_HOME/libhdfs/
else
    LD_LIBRARY_PATH=$PWD/thirdlib:$LD_LIBRARY_PATH
fi
LD_LIBRARY_PATH=$PWD/lib:$PWD:$LD_LIBRARY_PATH
PYTHONPATH=$PWD:$PWD/python:$PYTHONPATH

export CLASSPATH
export LD_LIBRARY_PATH
export PYTHONPATH

set -x

if [ -d "$PWD/prepare" ]; then
    for file in $PWD/prepare/*.sh; do
        source "$file"
    done
fi

worker_path=./flume/worker

date 1>&2

GLOG_logtostderr=1 ${worker_path} --flume_execute --flume_backend=local \
                & pid=$!

date 1>&2

wait $pid

STATUS=$?

if [ -d "$PWD/cleanup" ]; then
    for file in $PWD/cleanup/*.sh; do
        source "$file"
    done
fi

if [ "$STATUS" != "0" ];then
    exit $STATUS
else
    exit 0
fi
