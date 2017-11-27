set -e

cd /deps

echo 'export PATH='$PWD'/cmake-3.10.0-Linux-x86_64/bin/:$PATH' >> profiles/cmake.profile
source ./profiles/cmake.profile

export JAVA_HOME=`ls -d /usr/lib/jvm/java-1.8.0-openjdk-* | head -n 1`

echo 'export JAVA_HOME='$JAVA_HOME >> profiles/java.profile
echo 'export JRE_HOME=$JAVA_HOME/jre' >> profiles/java.profile
echo 'export CLASS_PATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/deps.jar:$JRE_HOME/lib' >> profiles/java.profile
echo 'export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH' >> profiles/java.profile

source ./profiles/java.profile

echo 'export MAVEN_HOME='$PWD'/apache-maven-3.5.0' >> profiles/maven.profile
echo 'export PATH=${MAVEN_HOME}/bin:${PATH}' >> profiles/maven.profile
source ./profiles/maven.profile

echo 'export HADOOP_HOME='$PWD'/hadoop-2.6.5' >> profiles/hadoop.profile
source ./profiles/hadoop.profile


echo 'export SPARK_HOME='$PWD'/spark-2.1.0-bin-hadoop2.7' >> profiles/spark.profile
source ./profiles/spark.profile

cat /deps/profiles/cmake.profile /deps/profiles/java.profile /deps/profiles/maven.profile /deps/profiles/hadoop.profile /deps/profiles/spark.profile > /environment
