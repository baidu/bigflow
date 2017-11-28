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


运行::

    docker run -i -t -v $PWD:/root/bigflow centos:7.1.1503 /bin/bash
    
进入centos docker镜像


在镜像中执行：::
  
    yum install sudo -y
    cd /root/bigflow/build_support
    sh build_deps.sh
    source ./environment
    mkdir -p ../build && cd ../build && cmake ..
    make
    make release
    
注意，上述方法的执行环境是一次性的，用户可以使用docker命令保存编译完的docker镜像，或使用daemon的方式运行，具体方法这里不再详述。请参考docker文档。
