FROM centos:7.1.1503
MAINTAINER  <zjsxzong89@gmail.com>
RUN mkdir deps && \
    pushd deps
RUN mkdir profiles
RUN yum clean all && \
    yum install -q -y wget tar gcc gcc-c++ make git patch openssl-devel.x86_64 flex flex-devel.x86_64 byacc xz-devel libgcrypt-devel.x86_64  libidn-devel.x86_64 ncurses-devel.x86_64 autoconf automake libtool java-1.8.0-openjdk-devel.x86_64

RUN wget https://cmake.org/files/v3.10/cmake-3.10.0-Linux-x86_64.tar.gz && \
    tar zxf cmake-3.10.0-Linux-x86_64.tar.gz



RUN wget https://archive.apache.org/dist/maven/maven-3/3.5.0/binaries/apache-maven-3.5.0-bin.tar.gz && \
tar xzf apache-maven-3.5.0-bin.tar.gz

RUN wget https://ftp.gnu.org/pub/gnu/libiconv/libiconv-1.15.tar.gz
RUN tar xzf libiconv-1.15.tar.gz && \
    pushd libiconv-1.15 \
    ./configure \
    make \
    sudo make install \
    popd

RUN wget http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz

RUN tar xzf hadoop-2.6.5.tar.gz

RUN wget https://archive.apache.org/dist/spark/spark-2.1.0/spark-2.1.0-bin-hadoop2.7.tgz
RUN tar xzf spark-2.1.0-bin-hadoop2.7.tgz
CMD ["/bin/bash"]