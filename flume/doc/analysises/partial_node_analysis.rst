=============================
PartialNodeAnalysis
=============================

1. 功能介绍
-----------------
PartialNodeAnalysis的作用是分析哪些LOCAL_SHUFFLE节点是partial的。
整个过程包含三个部分, 首先分析可以partial的PROCESS节点, 其次根据上下游数据流关系, 分析哪些SHUFFLE
节点是可以partial, 最后根据之前的分析, 为那些可以partial的LOCAL_SHUFFLE节点做上标记。
//TODO(Pan Yuchang)

2. 依赖说明
-----------
**ANALYSIS**

* `DataFlowAnalysis <data_flow_analysis.html>`_


`返回 <../plan_pass.html#analysis>`_
