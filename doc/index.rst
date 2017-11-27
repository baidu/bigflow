.. bigflow_python documentation master file, created by
   sphinx-quickstart on Sun Mar 29 20:59:50 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

欢迎来到Bigflow Python项目
===========================

Bigflow是什么?
===========================

**Baidu Bigflow** (以下简称Bigflow)是百度的一套计算框架， 它致力于提供一套简单易用的接口来描述用户的计算任务，并使同一套代码可以运行在不同的执行引擎之上。

它的设计中有许多思想借鉴自 `google flume java <http://pages.cs.wisc.edu/~akella/CS838/F12/838-CloudPapers/FlumeJava.pdf>`_ 以及 `google cloud bigflow <https://github.com/GoogleCloudPlatform/BigflowJavaSDK/>`_ ，另有部分接口设计借鉴自 `apache spark <http://spark.apache.org/>`_ 。

用户基本可以不去关心Bigflow的计算真正运行在哪里，可以像写一个单机的程序一样写出自己的逻辑， Bigflow会将这些计算分发到相应的执行引擎之上执行。

Bigflow的目标是: 使分布式程序写起来更简单，测起来更方便，跑起来更高效，维护起来更容易，迁移起来成本更小。

目前Bigflow在百度公司内部对接了公司内部的批量计算引擎DCE（与社区Tez比较类似），迭代引擎Spark，以及公司内部的流式计算引擎Gemini。

在开源版本中，目前仅开放了Bigflow on Spark。

为什么要使用Bigflow?
===========================

虽然Bigflow目前仅仅开源了Bigflow on Spark，但仍有许多理由让你选择Bigflow on Spark:

* **高性能**

  Bigflow的接口设计使得Bigflow可以感知更多的用户需求的细节属性，并且Bigflow会根据计算的属性进行作业的优化；另其执行层使用C++实现，用户的一些代码逻辑会被翻译为C++执行，有较大的性能提升。

  在公司内部的实际业务测试来看，其性能远高于用户手写的作业。根据一些从现有业务改写过来的作业平均来看，其性能都比原用户代码提升了100%+。

  开源版本的benchmark正在准备中。

* **简单易用**

  Bigflow的接口表面看起来很像Spark，但实际实用之后会发现Bigflow使用一些独特的设计使得Bigflow的代码更像是单机程序，例如，屏蔽了partitioner的概念，支持嵌套的分布式数据集等，使得其接口更加易于理解，并且拥有更强的代码可复用性。

  特别的，在许多需要优化的场景中，因为Bigflow的可以进行自动的性能以及内存占用优化，所以用户可以避免许多因OOM或性能不足而必须进行的优化工作，降低用户的使用成本。
  
* **在这里，Python是一等公民** 

  我们目前原生支持的语言是Python。

  使用PySpark时，有不少用户都困扰于PySpark的低效，或困扰于其不支持某些CPython库，或困扰于一些功能仅在Scala和Java中可用，在PySpark中暂时处于不可用状态。
 
  而在Bigflow中，Python是一等公民（毕竟当前我们仅仅支持Python），以上问题在Bigflow中都不是问题，性能、功能、易用性都对Python用户比较友好。
 
 
在线试用
========
`在线试用网页 <http://180.76.236.159:8732/?token=e347f1e762cdcfb63bd3682781260f2fdbc4862592fd38f8>`_ 包含了一些简单的例子介绍Bigflow的概念和API用法，同时也可以在线编写Python代码尝试Bigflow的功能，可智能提示。

注：该页面仅提供使用功能，并没有做安全防护，相关机器每隔一段时间会被清空一次，请不要做代码存储等操作。


文档索引
========

使用者文档
-----------------

* :doc:`快速入门 <quickstart>`
* :doc:`编程指南 <guide>`
* :doc:`API参考 <rst/modules>`
* :doc:`相关ppt <ppt>`

开发者文档
-------------------
* :doc:`编译构建 <build>` 
* :doc:`如何贡献 <contributing>`
* :doc:`设计文档 <bigflow_design>`

Github
===========

`Bigflow <https://github.com/baidu/bigflow>`_
