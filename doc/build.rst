目前在centos7.1测试通过

编译
"""""""""""""""""""" 

::

    git clone https://github.com/baidu/bigflow.git
    cd bigflow/build_support
    sh build_deps.sh
    source ./environment
    mkdir -p ../build && cd ../build && cmake ..
    make
    make release
    
测试
"""""""""""""""""""""

::

    cd bigflow_python/python/
    sh run-tests
    
    
使用Docker构建
""""""""""""""""""""""

先下载bigflow代码库::

    git clone https://github.com/baidu/bigflow.git
    cd bigflow

运行 `docker run -i -t -v $PWD:/root/bigflow centos:7.1.1503 /bin/bash` 进入centos docker镜像

在镜像中执行：::
  
    yum install sudo -y
    cd /root/bigflow/build_support
    sh build_deps.sh
    source ./environment
    mkdir -p ../build && cd ../build && cmake ..
    make
    make release
    
