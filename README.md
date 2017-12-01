# Bigflow

### Bigflow 是什么?

**Baidu Bigflow** (以下简称 Bigflow)是百度的一套计算框架， 它致力于提供一套简单易用的接口来描述用户的计算任务，并使同一套代码可以运行在不同的执行引擎之上。

它的设计中有许多思想借鉴自 [Google FlumeJava](http://pages.cs.wisc.edu/~akella/CS838/F12/838-CloudPapers/FlumeJava.pdf)以及 [Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/)，另有部分接口设计借鉴自 [Apache Spark](http://spark.apache.org/)。

用户基本可以不去关心 Bigflow 的计算真正运行在哪里，可以像写一个单机的程序一样写出自己的逻辑， Bigflow 会将这些计算分发到相应的执行引擎之上执行。

Bigflow 的目标是: 使分布式程序写起来更简单，测起来更方便，跑起来更高效，维护起来更容易，迁移起来成本更小。

目前 Bigflow 在百度公司内部对接了公司内部的批量计算引擎 DCE（与社区 Tez 比较类似），迭代引擎 Spark，以及公司内部的流式计算引擎 Gemini。

在开源版本中，目前仅开放了Bigflow on Spark。

### 为什么要使用 Bigflow?

* **高性能**
  Bigflow 的接口设计使得 Bigflow 可以感知更多的用户需求的细节属性，并且 Bigflow 会根据计算的属性进行作业的优化；另其执行层使用 C++ 实现，用户的一些代码逻辑会被翻译为 C++ 执行，有较大的性能提升。
 在公司内部的实际业务测试来看，其性能远高于用户手写的作业。根据一些从现有业务改写过来的作业平均来看，其性能都比原用户代码提升了 100%+。开源版本的 benchmark 正在准备中。

* **简单易用**
  Bigflow 的接口表面看起来很像 Spark，但实际实用之后会发现 Bigflow 使用一些独特的设计使得 Bigflow 的代码更像是单机程序，例如，屏蔽了 partitioner 的概念，支持嵌套的分布式数据集等，使得其接口更加易于理解，并且拥有更强的代码可复用性。
 特别的，在许多需要优化的场景中，因为 Bigflow 可以进行自动的性能以及内存占用优化，所以用户可以避免许多因 OOM 或性能不足而必须进行的优化工作，降低用户的使用成本。

* **在这里，Python 是一等公民**
  我们目前原生支持的语言是 Python。
 使用 PySpark 时，有不少用户都困扰于 PySpark 的低效，或困扰于其不支持某些 CPython 库，或困扰于一些仅功能仅仅在 Scala 和 Java 中可用，在 PySpark 中暂时处于不可用状态。
 而在 Bigflow 中，Python 是一等公民（毕竟当前我们仅仅支持 Python），以上问题在 Bigflow 中都不是问题，性能、功能、易用性都对 Python 用户比较友好。

### 在线试用

[在线试用网页](http://180.76.236.159:8732/?token=9a1bd5c7aeb2b217bef4e85c007f275e82744ba33f42eaf9) 包含了一些简单的例子介绍Bigflow的概念和API用法，同时也可以在线编写Python代码尝试Bigflow的功能，可智能提示。

注：该页面仅提供使用功能，并没有做安全防护，相关机器每隔一段时间会被清空一次，请不要做代码存储等操作。

### Bigflow详细文档

[Bigflow 主页](http://bigflow.cloud)

[快速入门](http://bigflow.cloud/zh/quickstart.html)

[编程指南](http://bigflow.cloud/zh/guide.html)

[API 参考](http://bigflow.cloud/zh/rst/modules.html)

[编译构建](http://bigflow.cloud/zh/build.html)

[如何贡献](http://bigflow.cloud/zh/contributing.html)

[设计文档](http://bigflow.cloud/zh/bigflow_design.html)
