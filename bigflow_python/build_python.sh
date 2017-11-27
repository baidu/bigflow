WORK_ROOT=${WORK_ROOT:-`pwd`}
rm -rf $WORK_ROOT/python/python_runtime
binary_dir=${BINARY_DIR:-${WORK_ROOT}/../}
third_party_dir=${binary_dir}/thirdparty
cp -r ${third_party_dir}/Python-2.7.12/build $WORK_ROOT/python/python_runtime
mkdir $WORK_ROOT/python/python_runtime/solib
export LIBRARY_PATH=$WORK_ROOT/python/python_runtime/lib:$LIBRARY_PATH
export LD_LIBRARY_PATH=$WORK_ROOT/python/python_runtime/lib:$LD_LIBRARY_PATH
${WORK_ROOT}/python/python_runtime/bin/python $WORK_ROOT/get-pip.py
PYTHON_ROOT=$WORK_ROOT/python/python_runtime/
rm -rf $PYTHON_ROOT/lib.linux-x86_64-2.7 $PYTHON_ROOT/temp.linux-x86_64-2.7
PYTHON=$PYTHON_ROOT/bin/python
PIP=$PYTHON_ROOT/bin/pip
#PIP=pip

$PIP install protobuf==3.4.0 
$PIP install jinja2 numpy scipy readline
$PIP install ipython==5.2

#pushd ${WORK_ROOT}/../thirdparty/fast-python-pb
#PYTHONPATH=${WORK_ROOT%/}/python/bigflow/depend/protobuf-3.0.0-py2.7.egg $PYTHON setup.py install
#PYTHONPATH=${WORK_ROOT%/}/python/python_runtime $PYTHON setup.py install
#popd

#sed  -i '1s/.*/#!\/usr\/bin\/env python/g' ${WORK_ROOT}/python/python_runtime/bin/protoc-gen-fastpython
sed  -i -e '1s/.*/#!\/usr\/bin\/env python/g' ${WORK_ROOT}/python/python_runtime/bin/ipython
sed  -i -e '1s/.*/#!\/usr\/bin\/env python/g' ${WORK_ROOT}/python/python_runtime/bin/pip
