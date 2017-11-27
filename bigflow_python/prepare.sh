set -e
export WORK_ROOT=$(cd `dirname "$0"`;pwd)
sh $WORK_ROOT/build_python.sh

LIBPYTHON_DIR=$WORK_ROOT/python/python_runtime/lib
if [ -r $LIBPYTHON_DIR ]
then
    if [ ! -L $WORK_ROOT/lib64_debug ]; then
        ln -s $LIBPYTHON_DIR $WORK_ROOT/lib64_debug
    fi
    if [ ! -L $WORK_ROOT/lib64_release ]; then
        ln -s $LIBPYTHON_DIR $WORK_ROOT/lib64_release
    fi
else
    echo "must link to libpython"
    exit 1
fi

wget https://pypi.python.org/packages/49/d5/9b1e52a2734195a3c8e7ce02ea03f26a7d6d7ade73f15d5fb818bd0f821d/protobuf-2.5.0-py2.7.egg -O python/bigflow/depend/protobuf-2.5.0-py2.7.egg

sh $WORK_ROOT/gen_proto.sh
