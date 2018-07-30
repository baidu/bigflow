=============================
PromotableNodeAnalysis
=============================

1. 功能介绍
-----------------
PromotableNodeAnalysis的作用是为符合条件的节点做上Promotable标记, 为节点移动提供信息。整个分析包含
三个部分, 分别为叶子节点分析, SHUFFLE节点分析以及DUMMY节点分析。

2. 依赖说明
-----------
**ANALYSIS**

* `DataFlowAnalysis <data_flow_analysis.html>`_
* `ScopeBasicInfoAnalysis <scope_basic_info_analysis.html>`_
* `PartialNodeAnalysis <partial_node_analysis.html>`_


`返回 <../plan_pass.html#analysis>`_
