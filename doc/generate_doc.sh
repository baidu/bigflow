# build doc
export WORK_ROOT=$(cd `dirname "$0"`;pwd)
export BIGFLOW_PYTHON_HOME=$WORK_ROOT/../bigflow_python/python/
BIGFLOW=$BIGFLOW_PYTHON_HOME/bigflow/bin/bigflow

rm -rf protobuf-2.5.0
wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz -O protobuf-2.5.0.tar.gz && tar zxvf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0
./autogen.sh && CXXFLAGS=-fPIC ./configure && make -j 2 && make install
cd -
sh $WORK_ROOT/../bigflow_python/gen_proto.sh

VERSION="1.0.0.0"
DATE=`date '+%Y_%m_%d_%H_%M_%S'`
BIGFLOW_VERSION=${VERSION}_$DATE
echo "bigflow_version = \"${BIGFLOW_VERSION}\"" > ${WORK_ROOT}/../bigflow_python/python/bigflow/version.py

$BIGFLOW pip install sphinx
$BIGFLOW pip install sphinx-intl

$BIGFLOW make gettext
$BIGFLOW sphinx-intl update -l zh -l en



rm -rf html && mkdir html

$BIGFLOW make -e SPHINXOPTS="-D language='zh'" html
mv _build/html  html/zh

$BIGFLOW make -e SPHINXOPTS="-D language='en'" html
mv _build/html  html/en

touch html/.touch html/.nojekyll
