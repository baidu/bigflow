Bigflow 设计文档
================

注意：下述文档为设计文档原文，后续有不少实现发生过改变，故可能有细节上与文档并不一致。
相关文档仅供学习Bigflow原理之用。

Bigflow架构与百度分布式技术栈：

   .. image:: static/cstack.png
       :width: 450px


其中，Bigflow核心的优化与执行层称为Bigflow Core层（又称Flume），它定义了一套与引擎无关的计算描述语言，用户可以使用这种语言描述自己的计算需求，而无需关注底层的计算引擎细节。


* :doc:`Bigflow Flume 设计文档 <flume/index>`
* :doc:`Bigflow On Spark 设计文档 <spark/index>`

