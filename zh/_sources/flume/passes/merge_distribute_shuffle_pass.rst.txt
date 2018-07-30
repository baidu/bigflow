==========================
MergeDistributeShufflePass
==========================

1. 功能介绍
-----------------
在plan中, 连续的两个TASK中后者具有SHUFFLE节点, 并且第二个TASK中的SHUFFLE节点类型为SEQUENCE,
并且没有指定partitioner, 那么就可以考虑将给SHUFFLE节点完整的移动到第一个TASK中去。
MergeDistributeShufflePass的作用就是找到这样的TASK, 并且完成移动。

2. 依赖说明
------------
**PASS**

* `MergeSingleNodePass <merge_single_node_pass.html>`_

**ANALYSIS**

* `DataFlowAnalysis <../analysises/data_flow_analysis.html>`_
* `ScopeBasicInfoAnalysis <../analysises/scope_basic_info_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/merge_distribute_shuffle_pass_0.png
       :width: 600px

上图中, 节点task2具有一个EXTERNAL节点, 其中包含两个SHUFFLE节点, 并且节点类型为SEQUENCE, 并且没有
指定partitioner, 那么就可以将整个EXTERNAL节点上移到task1中去, 并且删除task2。

**plan图（运行后）**

    .. image:: ../static/passes/merge_distribute_shuffle_pass_1.png
       :width: 600px

对plan应用了MergeDistributeShufflePass之后, 原先在task2中的EXTERNAL节点被整体移动到task1中去, 并且
task2被删除。


`返回 <../plan_pass.html#pass>`_
