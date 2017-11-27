CUR=`dirname "$0"`
CUR=`cd "$CUR"; pwd`

mkdir -p thirdparty/lib
mkdir -p thirdparty/include

pushd ${CUR}
git submodule update --init --recursive
popd

pushd ${CUR}/toft
git checkout master
popd
