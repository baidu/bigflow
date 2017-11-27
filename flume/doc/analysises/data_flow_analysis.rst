=============================
DataFlowAnalysis
=============================

1. 功能介绍
-----------------
DataFlowAnalysis的主要作用是分析plan中每个节点的数据流信息, 主要的信息包括数据流上游节点,
数据流下游节点, 以及相应的连接边。在头文件中定义了一个Info的struct结构,
DataFlowAnalysis使用Info结构来存储每个节点数据流信息。

Info结构如下

* nodes：当前节点所有的孩子节点, 包含自身
* users：当前节点所有直接下游节点
* needs：当前节点所有直接上游节点
* upstreams：当前节点所有上游节点
* downstreams：当前节点所有下游节点
* inputs：当前节点所有直接输入的边
* outputs：当前节点所有直接输出的边

2. 依赖说明
-----------
**ANALYSIS**

无


`返回 <../plan_pass.html#analysis>`_
