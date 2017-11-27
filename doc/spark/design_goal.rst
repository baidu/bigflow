############
设计目标
############

功能指标
==============

Baidu Bigflow(以下简称Bigflow)能够将Baidu Spark(以下简称Spark)作为底层支持的引擎之一，
更加具体地：

* 当前Bigflow的多语言版本API均可以使用：Bigflow Python和C++ API
* 除指定Pipeline（引擎的抽象，例如Pipeline.create("Hadoop")修改为Pipeline.create("Spark")）
  的代码外，现有的使用Bigflow写出的计算任务，均可以不加修改地运行在Spark平台上
* 支持的范围尚不包括未正式发布的流式计算接口（这部分对应于Spark Streaming）

性能指标
==============

Bigflow on Spark的作业运行性能应当尽可能高效

* Bigflow Python API性能应当明显地优于PySpark
* Bigflow C++ API写出的逻辑应当与Spark Scala API性能相仿
