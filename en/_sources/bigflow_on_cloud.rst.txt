Cloud Document
===============

公有云资源申请
---------------

  参考 `百度云环境准备 <https://cloud.baidu.com/doc/BMR/QuickGuide.html#.E7.99.BE.E5.BA.A6.E4.BA.91.E7.8E.AF.E5.A2.83.E5.87.86.E5.A4.87>`_

集群环境
-----------

* 集群创建

  参考 `集群创建 <https://cloud.baidu.com/doc/BMR/QuickGuide.html#.E9.9B.86.E7.BE.A4.E5.87.86.E5.A4.87>`_ 中集群准备的第三步和第四步

  注意，请选择spark 2.0 集群

* 数据准备

  1. 数据上传至对象存储BOS（具体操作详见 `对象存储BOS入门指南 <https://cloud.baidu.com/doc/BOS/GettingStarted-new.html#.E5.BC.80.E9.80.9ABOS.E6.9C.8D.E5.8A.A1>`_ ）

  2. 从BOS 上传至集群HDFS，参考指令：

     hadoop distcp bos_path hdfs_path (eg: hadoop distcp bos://spark-input-data/join_lists_input/  hdfs:///user/input)

设置环境变量
---------------

公有云有现有的hadoop spark 环境，不需要下载hadoop client、spark client

.. code-block:: cpp

  export HADOOP_HOME=/opt/bmr/hadoop/

  export SPARK_HOME=/opt/bmr/spark2

  export JAVA_HOME=/opt/jdk1.8.0_144/jre


我们申请了一些 `公有云 <https://cloud.baidu.com/?from=console>`_ 的资源，如果想试用Bigflow，可以 `联系 <bigflow-opensource@baidu.com>`_ 我们。
