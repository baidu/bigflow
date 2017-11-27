=============================
BuildMapInputPass
=============================

1. 功能介绍
-----------------
在plan中, 有一些节点没有输入, 这些节点为LOAD节点, BuildMapInputPass的作用就是创建Map Input节点,
同时创建相应的Executor。

2. 依赖说明
------------
**PASS**

* `RemoveEmptyUnitPass <remove_empty_unit_pass.html>`_

**ANALYSIS**

* `DataFlowAnalysis <../analysises/data_flow_analysis.html>`_
* `ScopeBasicInfoAnalysis <../analysises/scope_basic_info_analysis.html>`_
* `TaskIndexAnalysis <../analysises/task_index_analysis.html>`_
* `ReduceNumberAnalysis <../analysises/reduce_number_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/build_map_input_pass_0.png
       :width: 600px

上图中, 由于输入为LOAD节点, LOAD节点所属的SCOPE, 需要变成MAP_INPUT类型

**plan图（运行后）**

    .. image:: ../static/passes/build_map_input_pass_1.png
       :width: 600px

运行BuildMapInput之后, LOAD节点的父节点变成MAP_INPUT类型。


`返回 <../plan_pass.html#pass>`_
