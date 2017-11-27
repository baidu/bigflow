目前在centos7.1测试通过

开始编译
""""""""""""""""""""
git clone https://github.com/baidu/bigflow.git

cd bigflow/build_support

sh build_deps.sh

source ./environment

cd ../build

cmake ..

make

make release
