set -e
rm -rf deps
mkdir deps
pushd deps
mkdir profiles

if [ -e /.dockerenv ];then
    sudo yum swap -y -q -- remove fakesystemd -- install systemd systemd-libs
fi

sudo yum install -q -y wget tar gcc gcc-c++ make git patch openssl-devel.x86_64 flex flex-devel.x86_64 byacc xz-devel libgcrypt-devel.x86_64  libidn-devel.x86_64 ncurses-devel.x86_64 autoconf automake libtool java-1.8.0-openjdk-devel.x86_64 

wget https://cmake.org/files/v3.10/cmake-3.10.0-Linux-x86_64.tar.gz
tar zxf cmake-3.10.0-Linux-x86_64.tar.gz

echo 'export PATH='$PWD'/cmake-3.10.0-Linux-x86_64/bin/:$PATH' >> profiles/cmake.profile
source ./profiles/cmake.profile

export JAVA_HOME=`ls -d /usr/lib/jvm/java-1.8.0-openjdk-* | head -n 1`

echo 'export JAVA_HOME='$JAVA_HOME >> profiles/java.profile
echo 'export JRE_HOME=$JAVA_HOME/jre' >> profiles/java.profile
echo 'export CLASS_PATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/deps.jar:$JRE_HOME/lib' >> profiles/java.profile
echo 'export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH' >> profiles/java.profile

source ./profiles/java.profile

wget https://archive.apache.org/dist/maven/maven-3/3.5.0/binaries/apache-maven-3.5.0-bin.tar.gz
tar xzf apache-maven-3.5.0-bin.tar.gz
echo 'export MAVEN_HOME='$PWD'/apache-maven-3.5.0' >> profiles/maven.profile
echo 'export PATH=${MAVEN_HOME}/bin:${PATH}' >> profiles/maven.profile
source ./profiles/maven.profile


wget https://ftp.gnu.org/pub/gnu/libiconv/libiconv-1.15.tar.gz
tar xzf libiconv-1.15.tar.gz
pushd libiconv-1.15
./configure
make
sudo make install
popd


wget http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
tar xzf hadoop-2.6.5.tar.gz
echo 'export HADOOP_HOME='$PWD'/hadoop-2.6.5' >> profiles/hadoop.profile
source ./profiles/hadoop.profile

wget https://archive.apache.org/dist/spark/spark-2.1.0/spark-2.1.0-bin-hadoop2.7.tgz
tar xzf spark-2.1.0-bin-hadoop2.7.tgz
echo 'export SPARK_HOME='$PWD'/spark-2.1.0-bin-hadoop2.7' >> profiles/spark.profile
source ./profiles/spark.profile

popd # leave deps

cat ./deps/profiles/cmake.profile ./deps/profiles/java.profile ./deps/profiles/maven.profile ./deps/profiles/hadoop.profile ./deps/profiles/spark.profile > ./environment

echo 'please source ./environment' >&2
